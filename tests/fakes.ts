import { MaturingRecord, Tip, WeirEvent } from '../src/lib/types'

/**
 * In-memory fakes for engine tests — no redis/bitcoind needed.
 *
 * FakeStore mirrors the full `src/store/redis.ts` Store surface from DESIGN.md,
 * including the hash-only record ops (putRecord/readRecord/deleteRecord) used to
 * persist pending-tx records at seen-time. All internal state is public so tests
 * can seed and assert directly. Values are cloned on the way in/out to mimic
 * redis serialization (no shared object aliasing between test and store).
 */
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
  mempoolPrevious = new Set<string>()
  mempoolCurrent = new Set<string>()
  mempoolPostBlock = new Set<string>()
  blockTxids = new Set<string>()

  // tip / ring
  tip: Tip | null = null
  /** height -> blockHash */
  ring = new Map<number, string>()

  // maturing: zset index (mined txs only) + per-txid record hashes
  /** txid -> inclusion height; only mined txs are indexed here */
  maturingIndex = new Map<string, number>()
  /** txid -> record hash; exists from seen-time onward (putRecord) */
  records = new Map<string, MaturingRecord>()

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

  /** single-page SSCAN: returns everything with terminal cursor "0" */
  async scanWatches(_cursor: string): Promise<{ cursor: string; addresses: string[] }> {
    return { cursor: '0', addresses: [...this.watches] }
  }

  async dueExpiries(nowMs: number): Promise<Array<{ address: string; expiresAtMs: number }>> {
    return [...this.expiries.entries()]
      .filter(([, at]) => at <= nowMs)
      .map(([address, expiresAtMs]) => ({ address, expiresAtMs }))
  }

  async clearExpiry(addr: string): Promise<void> {
    this.expiries.delete(addr)
  }

  // --- evaluated / pending ---

  async isEvaluated(txid: string): Promise<boolean> {
    return this.evaluated.has(txid)
  }

  async markEvaluated(txid: string): Promise<void> {
    this.evaluated.add(txid)
  }

  async unmarkEvaluated(txid: string): Promise<void> {
    this.evaluated.delete(txid)
  }

  async addPending(txid: string): Promise<void> {
    this.pending.add(txid)
  }

  async removePending(txid: string): Promise<void> {
    this.pending.delete(txid)
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

  /** current − previous − evaluated */
  async newMempoolTxids(): Promise<string[]> {
    return [...this.mempoolCurrent].filter(
      (t) => !this.mempoolPrevious.has(t) && !this.evaluated.has(t),
    )
  }

  /** current → previous */
  async rotateMempool(): Promise<void> {
    this.mempoolPrevious = this.mempoolCurrent
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

  async ringPut(height: number, hash: string): Promise<void> {
    this.ring.set(height, hash)
  }

  async ringHashAt(height: number): Promise<string | null> {
    return this.ring.get(height) ?? null
  }

  /** entries strictly above `height`, ascending */
  async ringAbove(height: number): Promise<Array<{ height: number; hash: string }>> {
    return [...this.ring.entries()]
      .filter(([h]) => h > height)
      .sort(([a], [b]) => a - b)
      .map(([h, hash]) => ({ height: h, hash }))
  }

  /** keep only the `keep` highest entries */
  async ringPrune(keep: number): Promise<void> {
    const heights = [...this.ring.keys()].sort((a, b) => b - a)
    for (const h of heights.slice(keep)) this.ring.delete(h)
  }

  /** drop entries strictly above `height` (reorg rewind) */
  async ringRemoveAbove(height: number): Promise<void> {
    for (const h of [...this.ring.keys()]) {
      if (h > height) this.ring.delete(h)
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

  // --- maturing ---

  async addMaturing(rec: MaturingRecord): Promise<void> {
    this.maturingIndex.set(rec.txid, rec.height)
    this.records.set(rec.txid, structuredClone(rec))
  }

  async getMaturing(txid: string): Promise<MaturingRecord | null> {
    const rec = this.records.get(txid)
    return rec === undefined ? null : structuredClone(rec)
  }

  /** ascending by inclusion height */
  async maturingEntries(): Promise<Array<{ txid: string; height: number }>> {
    return [...this.maturingIndex.entries()]
      .sort(([, a], [, b]) => a - b)
      .map(([txid, height]) => ({ txid, height }))
  }

  async setMaturingFired(txid: string, fired: number[]): Promise<void> {
    const rec = this.records.get(txid)
    if (rec === undefined) throw new Error(`[fakes] setMaturingFired: no record for ${txid}`)
    rec.fired = [...fired]
  }

  /** update zset score + record fields, reset fired to [] */
  async moveMaturing(txid: string, newHeight: number, newBlockHash: string): Promise<void> {
    const rec = this.records.get(txid)
    if (rec === undefined) throw new Error(`[fakes] moveMaturing: no record for ${txid}`)
    this.maturingIndex.set(txid, newHeight)
    rec.height = newHeight
    rec.blockHash = newBlockHash
    rec.fired = []
  }

  async removeMaturing(txid: string): Promise<void> {
    this.maturingIndex.delete(txid)
    this.records.delete(txid)
  }

  /** ZREM from the index only — the record stays (limbo transition) */
  async unindexMaturing(txid: string): Promise<void> {
    this.maturingIndex.delete(txid)
  }

  /** wipe all tx tracking, keep watches/tip/ring; returns lost txids */
  async clearTracking(): Promise<string[]> {
    const lost = new Set<string>([...this.limbo, ...this.pending, ...this.maturingIndex.keys()])
    this.maturingIndex.clear()
    this.records.clear()
    this.pending.clear()
    this.limbo.clear()
    this.evaluated.clear()
    this.mempoolPrevious.clear()
    this.mempoolCurrent.clear()
    this.mempoolPostBlock.clear()
    this.blockTxids.clear()
    return [...lost]
  }

  // --- record ops (hash-only; do NOT touch the maturing zset index) ---

  /** persist a pending-tx record at seen-time (height 0, blockHash '') */
  async putRecord(rec: MaturingRecord): Promise<void> {
    this.records.set(rec.txid, structuredClone(rec))
  }

  async readRecord(txid: string): Promise<MaturingRecord | null> {
    const rec = this.records.get(txid)
    return rec === undefined ? null : structuredClone(rec)
  }

  async deleteRecord(txid: string): Promise<void> {
    this.records.delete(txid)
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
 * Captures every deliver() attempt (success or failure) in `delivered`.
 * Set `deliverResult = false` to simulate webhook failure — deliver never throws,
 * matching the real WebhookSink contract.
 */
export class FakeSink {
  delivered: WeirEvent[] = []
  deliverResult = true

  async deliver(event: WeirEvent): Promise<boolean> {
    this.delivered.push(structuredClone(event))
    return this.deliverResult
  }
}

/**
 * Settable fake of src/bitcoin/rpc.ts Rpc. Seed `mempool`, `txs`, `headers`,
 * `blockHashByHeight`, `rawBlocks`, `bestBlockHash`, `blockCount` per test.
 * Missing headers/hashes/blocks throw, like a real RPC error would.
 * getMempoolEntry derives membership from `mempool`.
 */
export class FakeRpc {
  mempool: string[] = []
  /** txid -> verbose result; absent txid → getRawTransactionVerbose returns null */
  txs = new Map<string, { blockhash?: string; hex: string }>()
  headers = new Map<string, { height: number; previousblockhash?: string; time: number }>()
  blockHashByHeight = new Map<number, string>()
  rawBlocks = new Map<string, Buffer>()
  bestBlockHash = ''
  blockCount = 0
  blockchainInfo: { chain: string; blocks: number; pruned: boolean; pruneheight?: number } = {
    chain: 'regtest',
    blocks: 0,
    pruned: false,
  }
  zmqNotifications: Array<{ type: string; address: string }> = [
    { type: 'pubrawtx', address: 'tcp://127.0.0.1:28332' },
    { type: 'pubrawblock', address: 'tcp://127.0.0.1:28332' },
  ]

  async getBlockCount(): Promise<number> {
    return this.blockCount
  }

  async getBestBlockHash(): Promise<string> {
    return this.bestBlockHash
  }

  async getBlockHash(height: number): Promise<string> {
    const hash = this.blockHashByHeight.get(height)
    if (hash === undefined) throw new Error(`[fakes] getBlockHash: no block at height ${height}`)
    return hash
  }

  async getBlockHeader(
    hash: string,
  ): Promise<{ height: number; previousblockhash?: string; time: number }> {
    const header = this.headers.get(hash)
    if (header === undefined) throw new Error(`[fakes] getBlockHeader: unknown hash ${hash}`)
    return { ...header }
  }

  async getBlockRaw(hash: string): Promise<Buffer> {
    const raw = this.rawBlocks.get(hash)
    if (raw === undefined) throw new Error(`[fakes] getBlockRaw: unknown hash ${hash}`)
    return raw
  }

  async getRawMempool(): Promise<string[]> {
    return [...this.mempool]
  }

  async getRawTransactionVerbose(
    txid: string,
  ): Promise<{ blockhash?: string; hex: string } | null> {
    const tx = this.txs.get(txid)
    return tx === undefined ? null : { ...tx }
  }

  async getMempoolEntry(txid: string): Promise<object | null> {
    return this.mempool.includes(txid) ? {} : null
  }

  async getBlockchainInfo(): Promise<{
    chain: string
    blocks: number
    pruned: boolean
    pruneheight?: number
  }> {
    return { ...this.blockchainInfo }
  }

  async getZmqNotifications(): Promise<Array<{ type: string; address: string }>> {
    return this.zmqNotifications.map((n) => ({ ...n }))
  }
}
