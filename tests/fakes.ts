import type { DecodedBlock, DecodedTx, ExpiredEvent, MaturingRecord, Outpoint, Tip, TxEvent, WeirEvent } from '../src/lib/types'
import type { SendResult } from '../src/delivery/webhook'
import { outpointField } from '../src/store/keys'
import { type BlockWrites, type Claimant, type Drop, type EvaluationWrites, makeEventId, type OutboxRecord } from '../src/store/redis'
import { metrics } from '../src/lib/metrics'

/**
 * In-memory fakes for engine tests — no redis/bitcoind needed.
 *
 * FakeStore mirrors the `src/store/redis.ts` Store surface and MUST match its semantics
 * exactly (the ring is a ZSET member→score map, so two hashes can coexist at one height;
 * every transition that produces an event enqueues it in the same step; a MULTI is applied
 * in the same command order). All internal state is public so tests can seed and assert
 * directly. Values are cloned on the way in/out to mimic redis serialization.
 *
 * The outbox is modelled as the real schema: `outbox` (id → hash), `outboxQueue` (the
 * ZSET: id → nextAttemptAt score), `outboxCreated` (id → createdAt score), `outboxDeadSet`
 * (id → deadAt score). Ids come from the REAL `makeEventId`, and every ZSET read sorts by
 * score then member exactly like redis — so the monotonic-id tie order is what
 * `outboxEvents()` (what engine tests assert on) shows.
 *
 * `outpoints` models the `outpoint:{txid}:{vout}` claimant SETs (keyed by `{txid}:{vout}`):
 * CLAIM = SADD by the seen/promotion fragments, RELEASE = SREM of only the releaser's own
 * txid (from the claimant the caller passes) by every fragment that deletes a record; an
 * empty set vanishes.
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

  // tracking sets
  pending = new Set<string>()
  evaluated = new Set<string>()
  limbo = new Set<string>()

  /** the `outpoint:{txid}:{vout}` SETs: field → claimant txids; a key with no members does not exist */
  outpoints = new Map<string, Set<string>>()

  // tip / ring
  tip: Tip | null = null
  /** the `blocks` ZSET: member (blockHash) -> score (height); two hashes CAN share a height */
  ring = new Map<string, number>()

  // maturing: zset index (mined txs only) + per-txid record hashes
  /** txid -> inclusion height; only mined txs are indexed here */
  maturingIndex = new Map<string, number>()
  /** txid -> record hash; exists from seen-time onward. May hold a PARTIAL record (only `fired`) after an HSET on a missing txid — HSET semantics */
  records = new Map<string, Partial<MaturingRecord> & { txid: string }>()

  // outbox: per-id hash + the ZSETs (see class doc)
  outbox = new Map<string, OutboxRecord>()
  /** the `outbox` ZSET: id → nextAttemptAt unix ms */
  outboxQueue = new Map<string, number>()
  /** the `outbox:created` ZSET: id → createdAt unix ms (mirrors `outbox` membership) */
  outboxCreated = new Map<string, number>()
  /** the `outbox:dead` ZSET: id → deadAt unix ms */
  outboxDeadSet = new Map<string, number>()

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

  async getExpiry(addr: string): Promise<number | null> {
    return this.expiries.get(addr) ?? null
  }

  /** SREM addresses, ZREM expiries + enqueue `expired` */
  async expireWatch(addr: string, event: ExpiredEvent): Promise<void> {
    this.watches.delete(addr)
    this.expiries.delete(addr)
    this.enqueue(event, Date.now())
  }

  // --- tracking reads ---

  async isEvaluated(txid: string): Promise<boolean> {
    return this.evaluated.has(txid)
  }

  async evaluatedTxids(): Promise<string[]> {
    return [...this.evaluated]
  }

  async pendingTxids(): Promise<string[]> {
    return [...this.pending]
  }

  async limboTxids(): Promise<string[]> {
    return [...this.limbo]
  }

  /** ZSET order: by height ascending, ties by txid lexicographically (redis semantics) */
  async maturingEntries(): Promise<Array<{ txid: string; height: number }>> {
    return [...this.maturingIndex.entries()]
      .sort(([ta, a], [tb, b]) => a - b || (ta < tb ? -1 : ta > tb ? 1 : 0))
      .map(([txid, height]) => ({ txid, height }))
  }

  /** like Store.readRecord: null when absent, THROWS on a partial (corrupt) hash; a missing `inputs` (pre-outpoint record) reads as [] */
  async readRecord(txid: string): Promise<MaturingRecord | null> {
    const rec = this.records.get(txid)
    if (rec === undefined) return null
    if (rec.height === undefined || rec.blockHash === undefined || rec.matched === undefined || rec.fired === undefined || rec.hex === undefined) {
      throw new Error(`[fakes] corrupt maturing record for ${txid}: missing fields`)
    }
    return structuredClone({ ...(rec as MaturingRecord), inputs: rec.inputs ?? [] })
  }

  /** SMEMBERS per prevout (pipelined, chunked in the real Store) — prevouts with claimants only, keyed by `{txid}:{vout}` */
  async outpointOwners(outpoints: Outpoint[]): Promise<Map<string, string[]>> {
    const owners = new Map<string, string[]>()
    for (const o of outpoints) {
      const field = outpointField(o)
      const set = this.outpoints.get(field)
      if (set !== undefined && set.size > 0) owners.set(field, [...set])
    }
    return owners
  }

  // --- tip / ring ---

  async getTip(): Promise<Tip | null> {
    return this.tip === null ? null : { ...this.tip }
  }

  /** HSET tip + ZADD ring (ZADD member=hash score=height — does NOT replace another hash at the same height) */
  async setTip(tip: Tip): Promise<void> {
    this.tipOps(tip)
  }

  /** ZSET order: by score ascending, ties by member lexicographically (redis semantics). */
  private ringSorted(): Array<{ height: number; hash: string }> {
    return [...this.ring.entries()]
      .map(([hash, height]) => ({ height, hash }))
      .sort((a, b) => a.height - b.height || (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0))
  }

  /** ZRANGEBYSCORE h h → first member (lexicographic tie order), null when none */
  async ringHashAt(height: number): Promise<string | null> {
    return this.ringSorted().find((e) => e.height === height)?.hash ?? null
  }

  /** ZRANGE 0 -1 WITHSCORES: every entry, ascending by score (height 0 included) */
  async ringAll(): Promise<Array<{ height: number; hash: string }>> {
    return this.ringSorted()
  }

  // --- MULTI fragments (the real Store's private helpers, same order of effects) ---

  /**
   * The real Store's private `enqueue`: HSET outbox:{id} + ZADD outbox + ZADD outbox:created.
   * Public here so drainer tests can seed. Here the enqueue IS the successful write, so this
   * is where `weir_events_enqueued_total` is bumped (the real Store bumps it after each
   * transition's MULTI exec'd — same moment, same count).
   */
  enqueue(event: WeirEvent, nowMs: number): string {
    const id = makeEventId(nowMs)
    this.outbox.set(id, { event: structuredClone(event), attempts: 0, createdAt: nowMs, lastError: null })
    this.outboxQueue.set(id, nowMs)
    this.outboxCreated.set(id, nowMs)
    metrics.counters.inc('weir_events_enqueued_total', { event: event.event })
    return id
  }

  private tipOps(tip: Tip): void {
    this.tip = { ...tip }
    this.ring.set(tip.hash, tip.height)
  }

  /** SADD txid into each prevout's claimant set */
  private claimOutpoints(txid: string, inputs: Outpoint[]): void {
    for (const o of inputs) {
      const field = outpointField(o)
      const set = this.outpoints.get(field) ?? new Set<string>()
      set.add(txid)
      this.outpoints.set(field, set)
    }
  }

  /** release the claimant's own claims (SREM only its txid; the key vanishes when empty) + DEL record */
  private forgetOps(rec: Claimant): void {
    for (const o of rec.inputs) {
      const field = outpointField(o)
      const set = this.outpoints.get(field)
      if (set === undefined) continue
      set.delete(rec.txid)
      if (set.size === 0) this.outpoints.delete(field)
    }
    this.records.delete(rec.txid)
  }

  /** pending → gone: SREM pending, SREM evaluated, forget, + `dropped` */
  private dropOps(d: Drop, nowMs: number): void {
    this.pending.delete(d.rec.txid)
    this.evaluated.delete(d.rec.txid)
    this.forgetOps(d.rec)
    this.enqueue(d.event, nowMs)
  }

  /** limbo → gone: SREM pending, ZREM maturing, SREM limbo, forget, + `conflicted` */
  private conflictOps(rec: Claimant, event: TxEvent, nowMs: number): void {
    this.pending.delete(rec.txid)
    this.maturingIndex.delete(rec.txid)
    this.limbo.delete(rec.txid)
    this.forgetOps(rec)
    this.enqueue(event, nowMs)
  }

  /** HSET record, ZADD maturing, SREM pending, SREM limbo, SADD its claims */
  private promoteOps(rec: MaturingRecord): void {
    this.records.set(rec.txid, structuredClone(rec))
    this.maturingIndex.set(rec.txid, rec.height)
    this.pending.delete(rec.txid)
    this.limbo.delete(rec.txid)
    this.claimOutpoints(rec.txid, rec.inputs)
  }

  // --- transitions (each ONE MULTI in the real Store) ---

  /** SADD evaluated; each replaced claimant dropped; when seen: HSET record (height 0) + SADD pending + SADD claims + enqueue `seen` when given */
  async applyEvaluation(w: EvaluationWrites): Promise<void> {
    const nowMs = Date.now()
    this.evaluated.add(w.txid)
    for (const d of w.dropped) this.dropOps(d, nowMs)
    if (w.seen !== null) {
      this.records.set(w.seen.rec.txid, structuredClone(w.seen.rec))
      this.pending.add(w.seen.rec.txid)
      this.claimOutpoints(w.seen.rec.txid, w.seen.rec.inputs)
      if (w.seen.event !== null) this.enqueue(w.seen.event, nowMs)
    }
  }

  /** promotions → fired → finished → dropped → conflicted → ended → unindexed → tip + ring put + ring prune */
  async applyBlock(w: BlockWrites): Promise<void> {
    const nowMs = Date.now()
    for (const rec of w.promoted) this.promoteOps(rec)
    for (const f of w.fired) {
      const rec = this.records.get(f.txid) ?? { txid: f.txid } // HSET on a missing key creates a partial hash
      rec.fired = [...f.fired]
      this.records.set(f.txid, rec)
      this.enqueue(f.event, nowMs)
    }
    for (const rec of w.finished) {
      this.maturingIndex.delete(rec.txid)
      this.forgetOps(rec)
    }
    for (const d of w.dropped) this.dropOps(d, nowMs)
    for (const c of w.conflicted) this.conflictOps(c.rec, c.event, nowMs)
    for (const rec of w.ended) {
      this.pending.delete(rec.txid)
      this.limbo.delete(rec.txid)
      this.forgetOps(rec)
    }
    for (const txid of w.unindexed) this.maturingIndex.delete(txid)
    this.tipOps(w.tip)
    const sorted = this.ringSorted()
    for (const e of sorted.slice(0, Math.max(0, sorted.length - w.ringKeep))) this.ring.delete(e.hash)
  }

  /** SADD limbo + ZREM maturing (records kept), ZREMRANGEBYSCORE ring (ancestor +inf, tip = ancestor */
  async rewind(ancestor: Tip, displaced: string[]): Promise<void> {
    for (const t of displaced) {
      this.limbo.add(t)
      this.maturingIndex.delete(t)
    }
    for (const [hash, h] of [...this.ring.entries()]) if (h > ancestor.height) this.ring.delete(hash)
    this.tipOps(ancestor)
  }

  /** the tip-block eviction: `dropOps` */
  async dropPending(d: Drop): Promise<void> {
    this.dropOps(d, Date.now())
  }

  /** record → height 0 / blockHash '' / fired [] + SADD pending + SADD evaluated + SREM limbo + enqueue `demoted` (claims kept) */
  async demoteToPending(rec: MaturingRecord, event: TxEvent): Promise<void> {
    this.records.set(rec.txid, structuredClone({ ...rec, height: 0, blockHash: '', fired: [] }))
    this.pending.add(rec.txid)
    this.evaluated.add(rec.txid)
    this.limbo.delete(rec.txid)
    this.enqueue(event, Date.now())
  }

  /** the by-elimination conflict: `conflictOps` */
  async conflict(rec: Claimant, event: TxEvent): Promise<void> {
    this.conflictOps(rec, event, Date.now())
  }

  async removeLimbo(txid: string): Promise<void> {
    this.limbo.delete(txid)
  }

  /** SREM evaluated txids… */
  async forgetEvaluated(txids: string[]): Promise<void> {
    for (const t of txids) this.evaluated.delete(t)
  }

  /** DEL every record + claimant SET + the tracking sets, tip/ring jump; keeps watches/expiries/outbox; returns the lost txids */
  async resetTracking(tip: Tip): Promise<string[]> {
    const lost = [...new Set<string>([...this.limbo, ...this.pending, ...this.maturingIndex.keys()])]
    this.outpoints.clear()
    this.records.clear()
    this.maturingIndex.clear()
    this.pending.clear()
    this.limbo.clear()
    this.evaluated.clear()
    this.tipOps(tip)
    return lost
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

  /** HSET attempts/lastError + ZADD queue nextAtMs (HSET on a missing hash creates a partial one, like redis) */
  async outboxRetry(id: string, nextAtMs: number, attempts: number, lastError: string): Promise<void> {
    const rec = this.outbox.get(id) ?? ({ attempts: 0, createdAt: 0, lastError: null } as unknown as OutboxRecord)
    rec.attempts = attempts
    rec.lastError = lastError
    this.outbox.set(id, rec)
    this.outboxQueue.set(id, nextAtMs)
  }

  /** HSET, ZREM queue + created, ZADD dead nowMs, then the cap (oldest overflow + hashes dropped) */
  async outboxDead(id: string, nowMs: number, attempts: number, lastError: string, deadMax: number): Promise<void> {
    const overflow = this.outboxDeadSet.size + 1 - deadMax
    const oldest = overflow > 0 ? zsetSorted(this.outboxDeadSet).slice(0, overflow).map(([dead]) => dead) : []
    const rec = this.outbox.get(id) ?? ({ attempts: 0, createdAt: 0, lastError: null } as unknown as OutboxRecord)
    rec.attempts = attempts
    rec.lastError = lastError
    this.outbox.set(id, rec)
    this.outboxQueue.delete(id)
    this.outboxCreated.delete(id)
    this.outboxDeadSet.set(id, nowMs)
    for (const dead of oldest) {
      this.outbox.delete(dead)
      this.outboxDeadSet.delete(dead)
    }
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

/**
 * A decoded tx paying `address` (null = unrecognized script) at vout 0 plus an unwatched
 * change output. `inputs` default to [] (spends nothing weir could track); RBF/double-spend
 * tests pass explicit prevouts so two txs can share one.
 */
export function mkTx(txid: string, address: string | null = ADDR, valueSats = 5000, inputs: Outpoint[] = []): DecodedTx {
  return {
    txid,
    hex: `hex-${txid}`,
    inputs: inputs.map((o) => ({ ...o })),
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
 * CURRENT main chain by height (what getblockhash and getbestblockhash answer), the live
 * mempool, and a matching Rpc subset. Raw block bytes are just the hash (see `raw`/`decode`),
 * so no real serialization is needed. Adding a block at a height already on the main chain
 * REPLACES it there — that is how a test reorgs the node.
 */
export class FakeChain {
  blocks = new Map<string, FakeBlock>()
  /** height → hash of the CURRENT main chain (what getblockhash answers) */
  mainChain = new Map<number, string>()
  /** what getbestblockhash answers; null = the highest main-chain block */
  best: string | null = null
  /** the live mempool: getrawmempool, and getmempoolentry for anything listed here (or in `mempoolEntries`) */
  mempool: string[] = []
  mempoolEntries = new Map<string, object>()
  rawTxs = new Map<string, { blockhash?: string; hex: string }>()
  getBlockHashCalls: number[] = []
  pruned = false
  pruneheight: number | undefined = undefined
  /** what getblockcount answers; null = the highest main-chain height (0 when the chain is empty) */
  blockCount: number | null = null
  /** set to make getblockcount reject (bitcoind unreachable) */
  blockCountError: Error | null = null
  getBlockCountCalls = 0

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

  private topHeight(): number {
    return this.mainChain.size === 0 ? 0 : Math.max(...this.mainChain.keys())
  }

  rpc() {
    return {
      getBlockCount: async () => {
        this.getBlockCountCalls++
        if (this.blockCountError !== null) throw this.blockCountError
        if (this.blockCount !== null) return this.blockCount
        return this.topHeight()
      },
      getBestBlockHash: async () => this.best ?? this.mainChain.get(this.topHeight()) ?? '',
      getBlockHeader: async (hash: string) => {
        const b = this.blocks.get(hash)
        if (!b) throw new Error(`getblockheader: unknown block ${hash}`)
        // Core: confirmations = depth on the active chain, -1 when the block is not on it
        const confirmations = this.mainChain.get(b.height) === hash ? this.topHeight() - b.height + 1 : -1
        return { height: b.height, previousblockhash: b.prevHash || undefined, time: b.time, confirmations }
      },
      getBlockHash: async (height: number) => {
        this.getBlockHashCalls.push(height)
        const hash = this.mainChain.get(height)
        if (!hash) throw new Error(`getblockhash: no main-chain block at height ${height}`)
        return hash
      },
      getBlockRaw: async (hash: string) => this.raw(hash),
      getRawMempool: async () => [...this.mempool],
      getMempoolEntry: async (txid: string) => this.mempoolEntries.get(txid) ?? (this.mempool.includes(txid) ? {} : null),
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
