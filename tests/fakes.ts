import type { DecodedBlock, DecodedTx, ExpiredEvent, MaturingRecord, Tip, TxEvent, WeirEvent } from '../src/lib/types'
import type { SendResult } from '../src/delivery/webhook'
import { MAX_EVALUATION_AGE_MS, makeEventId, type OutboxRecord } from '../src/store/redis'

/**
 * In-memory fakes for engine tests — no redis/bitcoind needed.
 *
 * FakeStore mirrors the `src/store/redis.ts` Store surface and MUST match its
 * semantics exactly (removeMaturing deletes index AND record; the ring is a ZSET
 * member→score map, so two hashes can coexist at one height; every transition that
 * produces an event enqueues it in the same step). All internal state is public so
 * tests can seed and assert directly. Values are cloned on the way in/out to mimic
 * redis serialization (no shared object aliasing).
 *
 * The outbox is modelled as the real schema: `outbox` (id → hash), `outboxQueue` (the
 * ZSET: id → nextAttemptAt score), `outboxCreated` (id → createdAt score), `outboxDeadSet`
 * (id → deadAt score), plus `tombstones` (txid → doneAt score). Ids come from the REAL
 * `makeEventId`, and every ZSET read sorts by score then member exactly like redis — so
 * the monotonic-id tie order is what `outboxEvents()` (what engine tests assert on) shows.
 */

/** ZSET iteration order: score ascending, ties by member lexicographically (redis semantics). */
function zsetSorted(m: ReadonlyMap<string, number>): Array<[string, number]> {
  return [...m.entries()].sort(([ia, a], [ib, b]) => a - b || (ia < ib ? -1 : ia > ib ? 1 : 0))
}

export class FakeStore {
  // watches
  watches = new Set<string>()
  /** address -> expiresAt unix ms */
  expiries = new Map<string, number>()

  // evaluated / pending
  pending = new Set<string>()
  evaluated = new Set<string>()

  // reorg-displaced txids awaiting re-resolution
  limbo = new Set<string>()

  // mempool / block scratch sets
  mempoolCurrent = new Set<string>()
  mempoolPostBlock = new Set<string>()
  blockTxids = new Set<string>()

  // tip / ring
  tip: Tip | null = null
  /** the `blocks` ZSET: member (blockHash) -> score (height); two hashes CAN share a height */
  ring = new Map<string, number>()

  // maturing: zset index (mined txs only) + per-txid record hashes
  /** txid -> inclusion height; only mined txs are indexed here */
  maturingIndex = new Map<string, number>()
  /** txid -> record hash; exists from seen-time onward (recordSeen) */
  /** may hold a PARTIAL record (only `fired`) after markFired on a missing txid — HSET semantics */
  records = new Map<string, Partial<MaturingRecord> & { txid: string }>()

  // outbox: per-id hash + the ZSETs (see class doc)
  outbox = new Map<string, OutboxRecord>()
  /** the `outbox` ZSET: id → nextAttemptAt unix ms */
  outboxQueue = new Map<string, number>()
  /** the `outbox:created` ZSET: id → createdAt unix ms (mirrors `outbox` membership) */
  outboxCreated = new Map<string, number>()
  /** the `outbox:dead` ZSET: id → deadAt unix ms */
  outboxDeadSet = new Map<string, number>()

  /** the `tombstones` ZSET: txid → doneAt unix ms (tracking ended) */
  tombstones = new Map<string, number>()

  // meta (settable by tests)
  memory: { usedBytes: number; maxBytes: number | null } = { usedBytes: 0, maxBytes: null }
  policy: string | null = 'noeviction'

  // --- watches ---

  async isWatched(addr: string): Promise<boolean> {
    return this.watches.has(addr)
  }

  /** returns ALL matches, preserving input order */
  async watchedSubset(addrs: string[]): Promise<string[]> {
    return addrs.filter((a) => this.watches.has(a))
  }

  async addWatch(addr: string, expiresAtMs?: number): Promise<void> {
    this.watches.add(addr)
    if (expiresAtMs !== undefined) this.expiries.set(addr, expiresAtMs)
    else this.expiries.delete(addr)
  }

  async removeWatch(addr: string): Promise<boolean> {
    const had = this.watches.delete(addr)
    this.expiries.delete(addr)
    return had
  }

  async watchCount(): Promise<number> {
    return this.watches.size
  }

  /** SSCAN COUNT — deliberately small so tests exercise multi-page iteration */
  scanPageSize = 2

  /**
   * SSCAN: the cursor is a numeric string (here: the offset into the set), "0" once the
   * iteration is complete; a non-numeric cursor is rejected by redis ("ERR invalid cursor"),
   * which node-redis surfaces as a thrown Error.
   */
  async scanWatches(cursor: string): Promise<{ cursor: string; addresses: string[] }> {
    if (!/^\d+$/.test(cursor)) throw new Error('ERR invalid cursor')
    const all = [...this.watches]
    const start = Number(cursor)
    const next = start + this.scanPageSize
    return { cursor: next >= all.length ? '0' : String(next), addresses: all.slice(start, next) }
  }

  /** ZRANGEBYSCORE order: by expiry ascending, ties by address lexicographically */
  async dueExpiries(nowMs: number): Promise<Array<{ address: string; expiresAtMs: number }>> {
    return [...this.expiries.entries()]
      .filter(([, at]) => at <= nowMs)
      .sort(([aa, a], [ab, b]) => a - b || (aa < ab ? -1 : aa > ab ? 1 : 0))
      .map(([address, expiresAtMs]) => ({ address, expiresAtMs }))
  }

  async clearExpiry(addr: string): Promise<void> {
    this.expiries.delete(addr)
  }

  async getExpiry(addr: string): Promise<number | null> {
    return this.expiries.get(addr) ?? null
  }

  // --- evaluated / pending ---

  async isEvaluated(txid: string): Promise<boolean> {
    return this.evaluated.has(txid)
  }

  async markEvaluated(txid: string): Promise<void> {
    this.evaluated.add(txid)
  }

  async pendingTxids(): Promise<string[]> {
    return [...this.pending]
  }

  // --- block / mempool bookkeeping ---

  async setBlockTxids(txids: string[]): Promise<void> {
    this.blockTxids = new Set(txids)
  }

  /** pending ∩ blockTxids */
  async pendingInBlock(): Promise<string[]> {
    return [...this.pending].filter((t) => this.blockTxids.has(t))
  }

  async replaceCurrentMempool(txids: string[]): Promise<void> {
    this.mempoolCurrent = new Set(txids)
  }

  /** current − evaluated */
  async newMempoolTxids(): Promise<string[]> {
    return [...this.mempoolCurrent].filter((t) => !this.evaluated.has(t))
  }

  /** DEL current */
  async clearCurrentMempool(): Promise<void> {
    this.mempoolCurrent = new Set()
  }

  async replacePostBlockMempool(txids: string[]): Promise<void> {
    this.mempoolPostBlock = new Set(txids)
  }

  /** pending − postBlock − blockTxids */
  async droppedPending(): Promise<string[]> {
    return [...this.pending].filter(
      (t) => !this.mempoolPostBlock.has(t) && !this.blockTxids.has(t),
    )
  }

  /** evaluated = evaluated ∩ postBlock */
  async pruneEvaluated(): Promise<void> {
    this.evaluated = new Set([...this.evaluated].filter((t) => this.mempoolPostBlock.has(t)))
  }

  // --- tip / ring ---

  async getTip(): Promise<Tip | null> {
    return this.tip === null ? null : { ...this.tip }
  }

  async setTip(tip: Tip): Promise<void> {
    this.tip = { ...tip }
  }

  /** ZSET order: by score ascending, ties by member lexicographically (redis semantics). */
  private ringSorted(): Array<{ height: number; hash: string }> {
    return [...this.ring.entries()]
      .map(([hash, height]) => ({ height, hash }))
      .sort((a, b) => a.height - b.height || (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0))
  }

  /** ZADD member=hash score=height — does NOT replace another hash at the same height */
  async ringPut(height: number, hash: string): Promise<void> {
    this.ring.set(hash, height)
  }

  /** ZRANGEBYSCORE h h → first member (lexicographic tie order), null when none */
  async ringHashAt(height: number): Promise<string | null> {
    return this.ringSorted().find((e) => e.height === height)?.hash ?? null
  }

  /** ZRANGE 0 -1 WITHSCORES: every entry, ascending by score (height 0 included) */
  async ringAll(): Promise<Array<{ height: number; hash: string }>> {
    return this.ringSorted()
  }

  /** ZREMRANGEBYRANK 0 -(keep+1): keep only the `keep` highest-ranked entries */
  async ringPrune(keep: number): Promise<void> {
    const sorted = this.ringSorted()
    for (const e of sorted.slice(0, Math.max(0, sorted.length - keep))) this.ring.delete(e.hash)
  }

  /** ZREMRANGEBYSCORE (height +inf — drop every member scored strictly above `height` */
  async ringRemoveAbove(height: number): Promise<void> {
    for (const [hash, h] of [...this.ring.entries()]) {
      if (h > height) this.ring.delete(hash)
    }
  }

  // --- limbo ---

  async addLimbo(txids: string[]): Promise<void> {
    for (const t of txids) this.limbo.add(t)
  }

  async limboTxids(): Promise<string[]> {
    return [...this.limbo]
  }

  async removeLimbo(txid: string): Promise<void> {
    this.limbo.delete(txid)
  }

  // --- record ops (hash-only; do NOT touch the maturing zset index) ---

  /** like Store.readRecord: null when absent, THROWS on a partial (corrupt) hash */
  async readRecord(txid: string): Promise<MaturingRecord | null> {
    const rec = this.records.get(txid)
    if (rec === undefined) return null
    if (rec.height === undefined || rec.blockHash === undefined || rec.matched === undefined || rec.fired === undefined || rec.hex === undefined) {
      throw new Error(`[fakes] corrupt maturing record for ${txid}: missing fields`)
    }
    return structuredClone(rec as MaturingRecord)
  }

  // --- transitions that ENQUEUE (state mutation + outbox, one step) ---

  /** the real Store's private `enqueue`: HSET outbox:{id} + ZADD outbox + ZADD outbox:created. Public here so drainer tests can seed. */
  enqueue(event: WeirEvent, nowMs: number): string {
    const id = makeEventId(nowMs)
    this.outbox.set(id, { event: structuredClone(event), attempts: 0, createdAt: nowMs, lastError: null })
    this.outboxQueue.set(id, nowMs)
    this.outboxCreated.set(id, nowMs)
    return id
  }

  /**
   * One guarded atomic step (Lua in the real Store): refused (false) when the evaluation is
   * older than MAX_EVALUATION_AGE_MS; skipped (false) when the txid is already evaluated,
   * its record already has height > 0 (mined meanwhile), or it is tombstoned (tracking
   * ended); otherwise record at height 0 + SADD pending + SADD evaluated + enqueue `seen`
   * when given.
   */
  async recordSeen(rec: MaturingRecord, event: TxEvent | null, startedAtMs: number): Promise<boolean> {
    if (Date.now() - startedAtMs > MAX_EVALUATION_AGE_MS) return false
    if (this.evaluated.has(rec.txid)) return false
    const existing = this.records.get(rec.txid)
    if (existing !== undefined && (existing.height ?? 0) > 0) return false
    if (this.tombstones.has(rec.txid)) return false
    this.records.set(rec.txid, structuredClone(rec))
    this.pending.add(rec.txid)
    this.evaluated.add(rec.txid)
    if (event !== null) this.enqueue(event, Date.now())
    return true
  }

  /** HSET fired (on a missing txid this CREATES a partial hash, exactly like redis) + enqueue `confirmed` */
  async markFired(txid: string, fired: number[], event: TxEvent): Promise<void> {
    const rec = this.records.get(txid) ?? { txid }
    rec.fired = [...fired]
    this.records.set(txid, rec)
    this.enqueue(event, Date.now())
  }

  /** SREM pending, SREM evaluated, DEL record + enqueue `dropped` */
  async dropPending(txid: string, event: TxEvent): Promise<void> {
    this.pending.delete(txid)
    this.evaluated.delete(txid)
    this.records.delete(txid)
    this.enqueue(event, Date.now())
  }

  /** SREM addresses, ZREM expiries + enqueue `expired` */
  async expireWatch(addr: string, event: ExpiredEvent): Promise<void> {
    this.watches.delete(addr)
    this.expiries.delete(addr)
    this.enqueue(event, Date.now())
  }

  /** SREM pending, SREM limbo, DEL record — no event */
  async endTracking(txid: string): Promise<void> {
    this.pending.delete(txid)
    this.limbo.delete(txid)
    this.records.delete(txid)
  }

  // --- maturing ---

  /** one MULTI: record + index + SREM pending + SREM limbo + SADD evaluated */
  async promoteToMaturing(rec: MaturingRecord): Promise<void> {
    this.records.set(rec.txid, structuredClone(rec))
    this.maturingIndex.set(rec.txid, rec.height)
    this.pending.delete(rec.txid)
    this.limbo.delete(rec.txid)
    this.evaluated.add(rec.txid)
  }

  /** one MULTI: record → height 0 / blockHash '' / fired [] + SADD pending + SADD evaluated + SREM limbo + enqueue `demoted` */
  async demoteToPending(rec: MaturingRecord, event: TxEvent): Promise<void> {
    this.records.set(rec.txid, structuredClone({ ...rec, height: 0, blockHash: '', fired: [] }))
    this.pending.add(rec.txid)
    this.evaluated.add(rec.txid)
    this.limbo.delete(rec.txid)
    this.enqueue(event, Date.now())
  }

  /** one MULTI: SREM pending, ZREM maturing, DEL record, SREM limbo, ZADD tombstones + enqueue `conflicted` */
  async conflict(txid: string, event: TxEvent): Promise<void> {
    const nowMs = Date.now()
    this.pending.delete(txid)
    this.maturingIndex.delete(txid)
    this.records.delete(txid)
    this.limbo.delete(txid)
    this.tombstones.set(txid, nowMs)
    this.enqueue(event, nowMs)
  }

  /** one MULTI: DEL record, ZREM maturing, ZADD tombstones — tracking ENDED */
  async finishMaturing(txid: string, nowMs: number): Promise<void> {
    this.records.delete(txid)
    this.maturingIndex.delete(txid)
    this.tombstones.set(txid, nowMs)
  }

  /** ZSCORE tombstones */
  async isTombstoned(txid: string): Promise<boolean> {
    return this.tombstones.has(txid)
  }

  /** ZREMRANGEBYSCORE tombstones -inf beforeMs */
  async pruneTombstones(beforeMs: number): Promise<void> {
    for (const [txid, at] of [...this.tombstones.entries()]) if (at <= beforeMs) this.tombstones.delete(txid)
  }

  /** ZSET order: by height ascending, ties by txid lexicographically (redis semantics) */
  async maturingEntries(): Promise<Array<{ txid: string; height: number }>> {
    return [...this.maturingIndex.entries()]
      .sort(([ta, a], [tb, b]) => a - b || (ta < tb ? -1 : ta > tb ? 1 : 0))
      .map(([txid, height]) => ({ txid, height }))
  }

  /** ZREM from the index only — the records stay (limbo transition); no-op on empty */
  async unindexMaturing(txids: string[]): Promise<void> {
    for (const t of txids) this.maturingIndex.delete(t)
  }

  /** DEL record + ZREM index — same as the real Store */
  async removeMaturing(txid: string): Promise<void> {
    this.maturingIndex.delete(txid)
    this.records.delete(txid)
  }

  /** wipe all tx tracking, keep watches/tip/ring AND the outbox; returns lost txids */
  async clearTracking(): Promise<string[]> {
    const lost = new Set<string>([...this.limbo, ...this.pending, ...this.maturingIndex.keys()])
    this.maturingIndex.clear()
    this.records.clear()
    this.pending.clear()
    this.limbo.clear()
    this.evaluated.clear()
    this.mempoolCurrent.clear()
    this.mempoolPostBlock.clear()
    this.blockTxids.clear()
    return [...lost]
  }

  // --- outbox (the drainer's surface) ---

  /** ZRANGEBYSCORE outbox -inf nowMs LIMIT 0 limit — ids ascending by score, ties by id */
  async outboxDue(nowMs: number, limit: number): Promise<string[]> {
    return zsetSorted(this.outboxQueue)
      .filter(([, score]) => score <= nowMs)
      .slice(0, limit)
      .map(([id]) => id)
  }

  /** like Store.outboxRead: null when absent */
  async outboxRead(id: string): Promise<OutboxRecord | null> {
    const rec = this.outbox.get(id)
    return rec === undefined ? null : structuredClone(rec)
  }

  /** DEL hash + ZREM queue + ZREM created */
  async outboxAck(id: string): Promise<void> {
    this.outbox.delete(id)
    this.outboxQueue.delete(id)
    this.outboxCreated.delete(id)
  }

  /** Lua: no-op (false) when the hash is gone; else HSET attempts/lastError + ZADD queue nextAtMs */
  async outboxRetry(id: string, nextAtMs: number, attempts: number, lastError: string): Promise<boolean> {
    const rec = this.outbox.get(id)
    if (rec === undefined) return false
    rec.attempts = attempts
    rec.lastError = lastError
    this.outboxQueue.set(id, nextAtMs)
    return true
  }

  /** Lua: no-op (false) when the hash is gone; else HSET, ZREM queue + created, ZADD dead nowMs, then cap (oldest overflow + hashes dropped) */
  async outboxDead(id: string, nowMs: number, attempts: number, lastError: string, deadMax: number): Promise<boolean> {
    const rec = this.outbox.get(id)
    if (rec === undefined) return false
    rec.attempts = attempts
    rec.lastError = lastError
    this.outboxQueue.delete(id)
    this.outboxCreated.delete(id)
    this.outboxDeadSet.set(id, nowMs)
    const sorted = zsetSorted(this.outboxDeadSet)
    for (const [dead] of sorted.slice(0, Math.max(0, sorted.length - deadMax))) {
      this.outbox.delete(dead)
      this.outboxDeadSet.delete(dead)
    }
    return true
  }

  /** depth/dead = ZCARD; oldestCreatedAt = the lowest score in `outbox:created` (exact) */
  async outboxStats(): Promise<{ depth: number; oldestCreatedAt: number | null; dead: number }> {
    const oldest = zsetSorted(this.outboxCreated)[0]
    return { depth: this.outboxQueue.size, oldestCreatedAt: oldest === undefined ? null : oldest[1], dead: this.outboxDeadSet.size }
  }

  /** Test helper: every QUEUED event (not dead) in score-then-insertion order — what engine tests assert on. */
  outboxEvents(): WeirEvent[] {
    return zsetSorted(this.outboxQueue).map(([id]) => {
      const rec = this.outbox.get(id)
      if (rec === undefined) throw new Error(`[fakes] queued outbox id ${id} has no event hash`)
      return structuredClone(rec.event)
    })
  }

  // --- meta ---

  async memoryInfo(): Promise<{ usedBytes: number; maxBytes: number | null }> {
    return { ...this.memory }
  }

  async maxmemoryPolicy(): Promise<string | null> {
    return this.policy
  }
}

/**
 * Captures every send() call in `attempts` and the successful ones in `delivered`.
 * Simulate webhook failure with `deliverResult = false` (all events) or `failWhen`
 * (per event). send never throws, matching the real WebhookSink contract.
 */
export class FakeSink {
  attempts: WeirEvent[] = []
  delivered: WeirEvent[] = []
  deliverResult = true
  failWhen: (ev: WeirEvent) => boolean = () => false

  async send(event: WeirEvent): Promise<SendResult> {
    const ev = structuredClone(event)
    this.attempts.push(ev)
    if (!this.deliverResult || this.failWhen(ev)) return { ok: false, error: 'HTTP 503' }
    this.delivered.push(ev)
    return { ok: true }
  }
}

/** Address every test watches; `mkTx` pays it at vout 0. */
export const ADDR = 'bcrt1qwatchedwatchedwatched'

/** A decoded tx paying `address` (null = unrecognized script) at vout 0 plus an unwatched change output. */
export function mkTx(txid: string, address: string | null = ADDR, valueSats = 5000): DecodedTx {
  return {
    txid,
    hex: `hex-${txid}`,
    outputs: [
      { vout: 0, valueSats, address, scriptType: address ? 'p2wpkh' : null },
      { vout: 1, valueSats: 111, address: 'bcrt1qchange', scriptType: 'p2wpkh' },
    ],
  }
}

export interface FakeBlock {
  hash: string
  prevHash: string
  height: number
  time: number
  txs: DecodedTx[]
}

/**
 * Settable fake of bitcoind's chain view for block-pipeline tests: blocks by hash, the
 * CURRENT main chain by height, the live mempool, and a matching Rpc subset. Raw block
 * bytes are just the hash (see `raw`/`decode`), so no real serialization is needed.
 */
export class FakeChain {
  blocks = new Map<string, FakeBlock>()
  /** height → hash of the CURRENT main chain (what getblockhash answers) */
  mainChain = new Map<number, string>()
  mempool: string[] = []
  mempoolEntries = new Map<string, object>()
  rawTxs = new Map<string, { blockhash?: string; hex: string }>()
  getBlockHashCalls: number[] = []
  pruned = false
  pruneheight: number | undefined = undefined

  addBlock(b: FakeBlock, opts: { main?: boolean } = {}): FakeBlock {
    this.blocks.set(b.hash, b)
    if (opts.main !== false) this.mainChain.set(b.height, b.hash)
    return b
  }

  raw(hash: string): Buffer {
    return Buffer.from(hash, 'utf8')
  }

  decode = (rawBuf: Buffer): DecodedBlock => {
    const b = this.blocks.get(rawBuf.toString('utf8'))
    if (!b) throw new Error(`decode: unknown block ${rawBuf.toString('utf8')}`)
    return { hash: b.hash, prevHash: b.prevHash, time: b.time, txs: b.txs }
  }

  rpc() {
    return {
      getBlockHeader: async (hash: string) => {
        const b = this.blocks.get(hash)
        if (!b) throw new Error(`getblockheader: unknown block ${hash}`)
        return { height: b.height, previousblockhash: b.prevHash || undefined, time: b.time }
      },
      getBlockHash: async (height: number) => {
        this.getBlockHashCalls.push(height)
        const hash = this.mainChain.get(height)
        if (!hash) throw new Error(`getblockhash: no main-chain block at height ${height}`)
        return hash
      },
      getBlockRaw: async (hash: string) => this.raw(hash),
      getRawMempool: async () => [...this.mempool],
      getMempoolEntry: async (txid: string) => this.mempoolEntries.get(txid) ?? null,
      getRawTransactionVerbose: async (txid: string) => this.rawTxs.get(txid) ?? null,
      getBlockchainInfo: async () => ({
        chain: 'regtest',
        blocks: this.mainChain.size,
        pruned: this.pruned,
        pruneheight: this.pruneheight,
      }),
    }
  }
}
