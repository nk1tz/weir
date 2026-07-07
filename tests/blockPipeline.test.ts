import { describe, expect, it } from 'vitest'
import { makeBlockHandler, makeBlockProcessor, BlockPipelineDeps } from '../src/engine/blockPipeline'
import { DecodedBlock, DecodedTx, MatchedOutput, MaturingRecord, Tip, TxEvent, WeirEvent } from '../src/lib/types'

// ─── minimal local fakes (deliberately NOT imported from tests/fakes.ts) ─────────────────

const silentLog = { info() {}, warn() {}, error() {} }

const cloneRec = (rec: MaturingRecord): MaturingRecord => ({
  ...rec,
  matched: rec.matched.map((m) => ({ ...m })),
  fired: [...rec.fired],
})

class FakeStore {
  watches = new Set<string>()
  expiries = new Map<string, number>() // address → expiresAtMs
  pending = new Set<string>()
  evaluated = new Set<string>()
  limbo = new Set<string>() // reorg-displaced txids awaiting re-resolution
  maturingZ = new Map<string, number>() // txid → inclusion height (the ZSET)
  records = new Map<string, MaturingRecord>() // maturing:{txid} hash
  tip: Tip | null = null
  ring = new Map<number, string>() // height → hash
  ringRemoveAboveCalls: number[] = []
  blockTxids = new Set<string>()
  postBlock = new Set<string>()

  // watches
  async watchedSubset(addrs: string[]): Promise<string[]> {
    return addrs.filter((a) => this.watches.has(a))
  }
  async dueExpiries(nowMs: number): Promise<Array<{ address: string; expiresAtMs: number }>> {
    return [...this.expiries.entries()]
      .filter(([, at]) => at <= nowMs)
      .map(([address, expiresAtMs]) => ({ address, expiresAtMs }))
  }
  async removeWatch(addr: string): Promise<boolean> {
    this.expiries.delete(addr)
    return this.watches.delete(addr)
  }
  async clearExpiry(addr: string): Promise<void> {
    this.expiries.delete(addr)
  }

  // evaluated / pending
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

  // block/mempool bookkeeping
  async setBlockTxids(txids: string[]): Promise<void> {
    this.blockTxids = new Set(txids)
  }
  async pendingInBlock(): Promise<string[]> {
    return [...this.pending].filter((t) => this.blockTxids.has(t))
  }
  async replacePostBlockMempool(txids: string[]): Promise<void> {
    this.postBlock = new Set(txids)
  }
  async droppedPending(): Promise<string[]> {
    return [...this.pending].filter((t) => !this.postBlock.has(t) && !this.blockTxids.has(t))
  }
  async pruneEvaluated(): Promise<void> {
    this.evaluated = new Set([...this.evaluated].filter((t) => this.postBlock.has(t)))
  }

  // tip / ring
  async getTip(): Promise<Tip | null> {
    return this.tip
  }
  async setTip(tip: Tip): Promise<void> {
    this.tip = tip
  }
  async ringPut(height: number, hash: string): Promise<void> {
    this.ring.set(height, hash)
  }
  async ringHashAt(height: number): Promise<string | null> {
    return this.ring.get(height) ?? null
  }
  async ringAbove(height: number): Promise<Array<{ height: number; hash: string }>> {
    return [...this.ring.entries()]
      .filter(([h]) => h > height)
      .sort((a, b) => a[0] - b[0])
      .map(([h, hash]) => ({ height: h, hash }))
  }
  async ringPrune(keep: number): Promise<void> {
    const sorted = [...this.ring.keys()].sort((a, b) => b - a)
    for (const h of sorted.slice(keep)) this.ring.delete(h)
  }
  async ringRemoveAbove(height: number): Promise<void> {
    this.ringRemoveAboveCalls.push(height)
    for (const h of [...this.ring.keys()]) {
      if (h > height) this.ring.delete(h)
    }
  }

  // limbo
  async addLimbo(txids: string[]): Promise<void> {
    for (const t of txids) this.limbo.add(t)
  }
  async limboTxids(): Promise<string[]> {
    return [...this.limbo]
  }
  async removeLimbo(txid: string): Promise<void> {
    this.limbo.delete(txid)
  }

  // maturing (zset + record hash)
  async addMaturing(rec: MaturingRecord): Promise<void> {
    this.maturingZ.set(rec.txid, rec.height)
    this.records.set(rec.txid, cloneRec(rec))
  }
  async getMaturing(txid: string): Promise<MaturingRecord | null> {
    const rec = this.records.get(txid)
    return rec ? cloneRec(rec) : null
  }
  async maturingEntries(): Promise<Array<{ txid: string; height: number }>> {
    return [...this.maturingZ.entries()]
      .sort((a, b) => a[1] - b[1])
      .map(([txid, height]) => ({ txid, height }))
  }
  async setMaturingFired(txid: string, fired: number[]): Promise<void> {
    const rec = this.records.get(txid)
    if (!rec) throw new Error(`setMaturingFired: no record for ${txid}`)
    rec.fired = [...fired]
  }
  async moveMaturing(txid: string, newHeight: number, newBlockHash: string): Promise<void> {
    const rec = this.records.get(txid)
    if (!rec) throw new Error(`moveMaturing: no record for ${txid}`)
    this.maturingZ.set(txid, newHeight)
    rec.height = newHeight
    rec.blockHash = newBlockHash
    rec.fired = []
  }
  // Strictest interpretation: removeMaturing touches only the ZSET; the pipeline must
  // manage the record hash explicitly via putRecord/deleteRecord.
  async removeMaturing(txid: string): Promise<void> {
    this.maturingZ.delete(txid)
  }
  async unindexMaturing(txid: string): Promise<void> {
    this.maturingZ.delete(txid)
  }
  async clearTracking(): Promise<string[]> {
    const lost = new Set<string>([...this.limbo, ...this.pending, ...this.maturingZ.keys()])
    this.maturingZ.clear()
    this.records.clear()
    this.pending.clear()
    this.limbo.clear()
    this.evaluated.clear()
    this.blockTxids.clear()
    this.postBlock.clear()
    return [...lost]
  }

  // hash-only record ops (records exist from seen-time onward)
  async putRecord(rec: MaturingRecord): Promise<void> {
    this.records.set(rec.txid, cloneRec(rec))
  }
  async readRecord(txid: string): Promise<MaturingRecord | null> {
    const rec = this.records.get(txid)
    return rec ? cloneRec(rec) : null
  }
  async deleteRecord(txid: string): Promise<void> {
    this.records.delete(txid)
  }
}

interface FakeBlock {
  hash: string
  prevHash: string
  height: number
  time: number
  txs: DecodedTx[]
}

class FakeChain {
  blocks = new Map<string, FakeBlock>()
  mainChain = new Map<number, string>() // height → hash of the CURRENT main chain
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

class FakeSink {
  attempts: WeirEvent[] = []
  delivered: WeirEvent[] = []
  failWhen: (ev: WeirEvent) => boolean = () => false
  deliver = async (ev: WeirEvent): Promise<boolean> => {
    this.attempts.push(ev)
    if (this.failWhen(ev)) return false
    this.delivered.push(ev)
    return true
  }
}

// ─── helpers ─────────────────────────────────────────────────────────────────────────────

const ADDR = 'bcrt1qwatchedwatchedwatched'
const MATCHED: MatchedOutput[] = [{ address: ADDR, vout: 0, valueSats: 5000 }]

function mkTx(txid: string, address: string | null = ADDR, valueSats = 5000): DecodedTx {
  return {
    txid,
    hex: `hex-${txid}`,
    outputs: [{ vout: 0, valueSats, address, scriptType: address ? 'p2wpkh' : null }],
  }
}

function setup(cfgOverrides: Partial<BlockPipelineDeps['cfg']> = {}) {
  const store = new FakeStore()
  const chain = new FakeChain()
  const sink = new FakeSink()
  const cfg = {
    network: 'regtest' as const,
    confirmMilestones: [1, 3],
    maxMilestone: 3,
    ringSize: 12,
    ...cfgOverrides,
  }
  const deps: BlockPipelineDeps = {
    cfg,
    store,
    rpc: chain.rpc(),
    sink,
    decodeBlock: chain.decode,
    log: silentLog,
  }
  const process = makeBlockProcessor(deps)
  return { store, chain, sink, cfg, deps, process }
}

/** Register `hash` as a block, set it as weir's tip and put it in the ring. */
function seedTip(store: FakeStore, chain: FakeChain, height: number, hash: string, prevHash = ''): void {
  chain.addBlock({ hash, prevHash, height, time: 1_700_000_000 + height, txs: [] })
  store.tip = { hash, height }
  store.ring.set(height, hash)
}

/** Leave a tx exactly as the tx pipeline would after a delivered `seen`. */
function seedPending(store: FakeStore, tx: DecodedTx): void {
  store.pending.add(tx.txid)
  store.evaluated.add(tx.txid)
  store.records.set(tx.txid, {
    txid: tx.txid,
    height: 0,
    blockHash: '',
    matched: MATCHED.map((m) => ({ ...m })),
    fired: [],
    hex: tx.hex,
  })
}

const confirmedEvents = (sink: FakeSink): TxEvent[] =>
  sink.delivered.filter((e): e is TxEvent => e.event === 'confirmed')

// ─── tests ───────────────────────────────────────────────────────────────────────────────

describe('blockPipeline', () => {
  it('connected block promotes pending → maturing and fires confirmed:1 with the correct idempotency key', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1, mkTx('unrelated', null)] })

    await process(chain.raw('b101'))

    expect(store.pending.size).toBe(0)
    expect(store.maturingZ.get('tx1')).toBe(101)
    const rec = store.records.get('tx1')!
    expect(rec.height).toBe(101)
    expect(rec.blockHash).toBe('b101')
    expect(rec.fired).toEqual([1])

    const confirmed = confirmedEvents(sink)
    expect(confirmed).toHaveLength(1)
    const ev = confirmed[0]!
    expect(ev.txid).toBe('tx1')
    expect(ev.confs).toBe(1)
    expect(ev.blockHeight).toBe(101)
    expect(ev.blockHash).toBe('b101')
    expect(ev.hex).toBe('hex-tx1')
    expect(ev.matched).toEqual(MATCHED)
    expect(ev.timestamp).toBe(1_700_000_101 * 1000)
    expect(ev.idempotencyKey).toBe('regtest:tx1:confirmed:1:b101')

    expect(store.tip).toEqual({ hash: 'b101', height: 101 })
    expect(store.ring.get(101)).toBe('b101')
  })

  it('a matched tx never seen in the mempool still confirms (and is markEvaluated)', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx9 = mkTx('tx9') // never pending, never evaluated
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx9] })

    await process(chain.raw('b101'))

    expect(store.maturingZ.get('tx9')).toBe(101)
    const confirmed = confirmedEvents(sink)
    expect(confirmed).toHaveLength(1)
    expect(confirmed[0]!.idempotencyKey).toBe('regtest:tx9:confirmed:1:b101')
    // markEvaluated happened during step 2 (pruneEvaluated later removes it: not in mempool)
    expect(store.evaluated.has('tx9')).toBe(false)
    expect(store.postBlock.has('tx9')).toBe(false)
  })

  it('milestone 3 fires two blocks after the inclusion block, then tracking ends', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
    chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })

    await process(chain.raw('b101'))
    await process(chain.raw('b102'))
    expect(confirmedEvents(sink)).toHaveLength(1) // confs=2 fires nothing new

    await process(chain.raw('b103'))
    const confirmed = confirmedEvents(sink)
    expect(confirmed).toHaveLength(2)
    expect(confirmed[1]!.confs).toBe(3)
    expect(confirmed[1]!.idempotencyKey).toBe('regtest:tx1:confirmed:3:b101')
    expect(confirmed[1]!.timestamp).toBe(1_700_000_103 * 1000)
    // final removal: confs ≥ maxMilestone and all milestones fired
    expect(store.maturingZ.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)
  })

  it('a jump across two milestones fires both in one sweep', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 102, 'b102')
    // tx maturing since height 100 with nothing fired yet
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.maturingZ.set('tx1', 100)
    store.records.set('tx1', { txid: 'tx1', height: 100, blockHash: 'b100', matched: MATCHED, fired: [], hex: 'hex-tx1' })
    chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })

    await process(chain.raw('b103')) // confs = 103 - 100 + 1 = 4

    const confirmed = confirmedEvents(sink)
    expect(confirmed.map((e) => e.idempotencyKey)).toEqual([
      'regtest:tx1:confirmed:1:b100',
      'regtest:tx1:confirmed:3:b100',
    ])
    expect(store.maturingZ.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)
  })

  it('gap catch-up processes intermediate blocks in order', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
    chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })

    await process(chain.raw('b103')) // only the newest block arrives

    expect(chain.getBlockHashCalls).toEqual([101, 102])
    expect(store.ring.get(101)).toBe('b101')
    expect(store.ring.get(102)).toBe('b102')
    expect(store.ring.get(103)).toBe('b103')
    expect(store.tip).toEqual({ hash: 'b103', height: 103 })
    // milestones fired in block order during catch-up: 1 at b101's sweep, 3 at b103's
    const confirmed = confirmedEvents(sink)
    expect(confirmed.map((e) => e.idempotencyKey)).toEqual([
      'regtest:tx1:confirmed:1:b101',
      'regtest:tx1:confirmed:3:b101',
    ])
    expect(confirmed[0]!.timestamp).toBe(1_700_000_101 * 1000)
    expect(confirmed[1]!.timestamp).toBe(1_700_000_103 * 1000)
    expect(store.maturingZ.has('tx1')).toBe(false)
  })

  it('reorg with re-inclusion resets fired and re-fires with the new blockHash idempotency key', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    const tx1 = mkTx('tx1')
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] }, { main: false })
    // weir's view: tip = b101a, tx1 maturing there with milestone 1 already fired
    store.tip = { hash: 'b101a', height: 101 }
    store.ring.set(100, 'b100')
    store.ring.set(101, 'b101a')
    store.maturingZ.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-tx1' })
    // the new chain re-includes tx1 in b101b
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [tx1] })
    chain.addBlock({ hash: 'b102b', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [] })
    chain.rawTxs.set('tx1', { blockhash: 'b101b', hex: 'hex-tx1' })

    await process(chain.raw('b102b'))

    const confirmed = confirmedEvents(sink)
    expect(confirmed).toHaveLength(1)
    expect(confirmed[0]!.idempotencyKey).toBe('regtest:tx1:confirmed:1:b101b')
    expect(confirmed[0]!.blockHash).toBe('b101b')
    const rec = store.records.get('tx1')!
    expect(rec.blockHash).toBe('b101b')
    expect(rec.height).toBe(101)
    expect(rec.fired).toEqual([1])
    expect(store.maturingZ.get('tx1')).toBe(101)
    expect(store.tip).toEqual({ hash: 'b102b', height: 102 })
    expect(sink.delivered.filter((e) => e.event === 'demoted' || e.event === 'conflicted')).toHaveLength(0)
  })

  it('reorg demotion emits demoted with the old block fields and returns the tx to pending', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.tip = { hash: 'b101a', height: 101 }
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [mkTx('tx1')] }, { main: false })
    store.ring.set(100, 'b100')
    store.ring.set(101, 'b101a')
    store.maturingZ.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-tx1' })
    // replacement block does NOT contain tx1; tx1 went back to the node's mempool
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    chain.mempoolEntries.set('tx1', { time: 1 })
    chain.mempool = ['tx1']

    await process(chain.raw('b101b'))

    const demoted = sink.delivered.filter((e): e is TxEvent => e.event === 'demoted')
    expect(demoted).toHaveLength(1)
    expect(demoted[0]!.txid).toBe('tx1')
    expect(demoted[0]!.confs).toBe(0)
    expect(demoted[0]!.blockHeight).toBe(101)
    expect(demoted[0]!.blockHash).toBe('b101a')
    expect(demoted[0]!.hex).toBe('hex-tx1')
    expect(demoted[0]!.idempotencyKey).toBe('regtest:tx1:demoted:b101a')

    expect(store.pending.has('tx1')).toBe(true)
    expect(store.maturingZ.has('tx1')).toBe(false)
    const rec = store.records.get('tx1')!
    expect(rec.height).toBe(0)
    expect(rec.blockHash).toBe('')
    expect(rec.fired).toEqual([])
    expect(store.tip).toEqual({ hash: 'b101b', height: 101 })
  })

  it('conflicted (gone from mempool and chain) emits with last confirmed depth and cleans up fully', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [mkTx('tx1')] }, { main: false })
    store.tip = { hash: 'b101a', height: 101 }
    store.ring.set(100, 'b100')
    store.ring.set(101, 'b101a')
    store.maturingZ.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-tx1' })
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    // no mempool entry, no verbose result → conflicted

    await process(chain.raw('b101b'))

    const conflicted = sink.delivered.filter((e): e is TxEvent => e.event === 'conflicted')
    expect(conflicted).toHaveLength(1)
    expect(conflicted[0]!.confs).toBe(1) // last confirmed depth
    expect(conflicted[0]!.blockHeight).toBe(101)
    expect(conflicted[0]!.blockHash).toBe('b101a')
    expect(conflicted[0]!.idempotencyKey).toBe('regtest:tx1:conflicted')

    expect(store.maturingZ.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)
    expect(store.pending.has('tx1')).toBe(false)
  })

  it('dropped emits, removes pending, un-evaluates (seen can re-fire) and deletes the record', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    // block without tx1; mempool without tx1 → dropped
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    chain.mempool = []

    await process(chain.raw('b101'))

    const dropped = sink.delivered.filter((e): e is TxEvent => e.event === 'dropped')
    expect(dropped).toHaveLength(1)
    expect(dropped[0]!.txid).toBe('tx1')
    expect(dropped[0]!.confs).toBe(0)
    expect(dropped[0]!.blockHeight).toBeNull()
    expect(dropped[0]!.blockHash).toBeNull()
    expect(dropped[0]!.matched).toEqual(MATCHED) // from the seen-time record
    expect(dropped[0]!.hex).toBe('hex-tx1')
    expect(dropped[0]!.idempotencyKey).toBe('regtest:tx1:dropped:101')

    expect(store.pending.has('tx1')).toBe(false)
    expect(store.evaluated.has('tx1')).toBe(false) // rebroadcast → seen can fire again
    expect(store.records.has('tx1')).toBe(false)
  })

  it('TTL sweep emits expired and removes the watch', async () => {
    const { store, chain, sink, process } = setup()
    const addr = 'bcrt1qttlwatch'
    const expiresAtMs = Date.now() - 60_000
    store.watches.add(addr)
    store.expiries.set(addr, expiresAtMs)
    seedTip(store, chain, 100, 'b100')
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })

    await process(chain.raw('b101'))

    const expired = sink.delivered.filter((e) => e.event === 'expired')
    expect(expired).toHaveLength(1)
    expect(expired[0]!).toMatchObject({
      event: 'expired',
      address: addr,
      network: 'regtest',
      idempotencyKey: `regtest:${addr}:expired:${expiresAtMs}`,
    })
    expect(store.watches.has(addr)).toBe(false)
    expect(store.expiries.has(addr)).toBe(false)
  })

  it('failed confirmed delivery is not added to fired and retries on the next block', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })

    sink.failWhen = (ev) => ev.event === 'confirmed'
    await process(chain.raw('b101'))

    expect(confirmedEvents(sink)).toHaveLength(0)
    expect(store.records.get('tx1')!.fired).toEqual([]) // failure NOT recorded as fired
    expect(store.maturingZ.get('tx1')).toBe(101) // still tracked

    sink.failWhen = () => false
    await process(chain.raw('b102'))

    const confirmed = confirmedEvents(sink)
    expect(confirmed).toHaveLength(1)
    expect(confirmed[0]!.confs).toBe(1)
    expect(confirmed[0]!.idempotencyKey).toBe('regtest:tx1:confirmed:1:b101') // same key as the failed attempt
    expect(store.records.get('tx1')!.fired).toEqual([1])
    const confirmedAttempts = sink.attempts.filter((e) => e.event === 'confirmed')
    expect(confirmedAttempts).toHaveLength(2)
  })

  it('a pending tx mined in a LATER missed block is NOT falsely dropped during catch-up', async () => {
    // Regression: the dropped check compares pending against the LIVE mempool, which is
    // meaningless for historical blocks — it must only run for the tip block.
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    // weir was down for b101 and b102; tx1 was mined in b102 (the LATER missed block)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [tx1] })
    chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
    chain.mempool = [] // tx1 is mined, so it is NOT in the live mempool

    await process(chain.raw('b103'))

    // the old behavior emitted dropped (at b101) then confirmed (at b102) — seen by
    // consumers as a payment that failed and then somehow confirmed anyway
    expect(sink.delivered.filter((e) => e.event === 'dropped')).toHaveLength(0)
    const confirmed = confirmedEvents(sink)
    expect(confirmed.map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b102'])
    expect(store.maturingZ.get('tx1')).toBe(102)
    expect(store.pending.has('tx1')).toBe(false)
  })

  it('reorg rewinds the ring (one hash per height) and the tip to the fork point', async () => {
    // Regression: stale disconnected hashes left in the ring poisoned the NEXT fork search.
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    const tx1 = mkTx('tx1')
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] }, { main: false })
    store.tip = { hash: 'b101a', height: 101 }
    store.ring.set(100, 'b100')
    store.ring.set(101, 'b101a')
    store.maturingZ.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-tx1' })
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [tx1] })
    chain.addBlock({ hash: 'b102b', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [] })

    await process(chain.raw('b102b'))

    // the ring was truncated at the ancestor before the replacement chain was recorded
    expect(store.ringRemoveAboveCalls).toEqual([100])
    expect(store.ring.get(101)).toBe('b101b')
    expect(store.ring.get(102)).toBe('b102b')
    expect(store.limbo.size).toBe(0) // re-included → left limbo during promotion
    expect(sink.delivered.filter((e) => e.event === 'conflicted' || e.event === 'demoted')).toHaveLength(0)
  })

  it('downtime beyond the prune window resets tracking loudly instead of crash-looping', async () => {
    const { store, chain, sink, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    // in-flight state that will be unrecoverable
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    store.maturingZ.set('tx2', 100)
    store.records.set('tx2', { txid: 'tx2', height: 100, blockHash: 'b100', matched: MATCHED, fired: [1], hex: 'hex-tx2' })
    // node pruned everything below 103; weir needs 101 → catch-up impossible
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
    chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
    chain.addBlock({ hash: 'b104', prevHash: 'b103', height: 104, time: 1_700_000_104, txs: [] })
    chain.pruned = true
    chain.pruneheight = 103

    await process(chain.raw('b104')) // must NOT throw

    // tracking wiped, no phantom events, watches intact, tip jumped forward
    expect(sink.delivered).toHaveLength(0)
    expect(store.pending.size).toBe(0)
    expect(store.maturingZ.size).toBe(0)
    expect(store.records.size).toBe(0)
    expect(store.watches.has(ADDR)).toBe(true)
    expect(store.tip).toEqual({ hash: 'b104', height: 104 })
    // no attempt to fetch pruned blocks
    expect(chain.getBlockHashCalls).toEqual([])
  })

  it('makeBlockHandler serializes concurrent blocks in order', async () => {
    const { store, chain, deps } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
    const handler = makeBlockHandler(deps)

    await Promise.all([handler(chain.raw('b101')), handler(chain.raw('b102'))])

    expect(store.tip).toEqual({ hash: 'b102', height: 102 })
    expect(store.ring.get(101)).toBe('b101')
    expect(store.ring.get(102)).toBe('b102')
    expect(chain.getBlockHashCalls).toEqual([]) // no gap walk needed — processed in order
  })
})
