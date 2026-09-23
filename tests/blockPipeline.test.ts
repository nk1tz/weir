import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { makeBlockProcessor, type BlockPipelineDeps } from '../src/engine/blockPipeline'
import { makeTxEvaluator } from '../src/engine/txPipeline'
import { findForkPoint } from '../src/engine/reorg'
import { metrics } from '../src/lib/metrics'
import type { DecodedTx, MatchedOutput, TxEvent } from '../src/lib/types'
import { ADDR, FakeChain, FakeStore, mkTx } from './fakes'

// ─── helpers ─────────────────────────────────────────────────────────────────────────────

const MATCHED: MatchedOutput[] = [{ address: ADDR, vout: 0, valueSats: 5000 }]

function setup(cfgOverrides: Partial<BlockPipelineDeps['cfg']> = {}) {
  const store = new FakeStore()
  const chain = new FakeChain()
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
    decodeBlock: chain.decode,
  }
  const process = makeBlockProcessor(deps)
  return { store, chain, cfg, deps, process }
}

/** Register `hash` as a block, set it as weir's tip and put it in the ring. */
function seedTip(store: FakeStore, chain: FakeChain, height: number, hash: string, prevHash = ''): void {
  chain.addBlock({ hash, prevHash, height, time: 1_700_000_000 + height, txs: [] })
  store.tip = { hash, height }
  store.ring.set(hash, height)
}

/** Leave a tx exactly as the tx pipeline would after its evaluation. */
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

/** Every ENQUEUED confirmed event (tests/fakes.ts outbox), in enqueue order. */
const confirmedEvents = (store: FakeStore): TxEvent[] =>
  store.outboxEvents().filter((e): e is TxEvent => e.event === 'confirmed')

// ─── tests ───────────────────────────────────────────────────────────────────────────────

describe('blockPipeline', () => {
  beforeAll(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  afterAll(() => {
    vi.restoreAllMocks()
  })

  it('connected block promotes pending → maturing and fires confirmed:1 with the correct idempotency key', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1, mkTx('unrelated', null)] })

    await process(chain.raw('b101'))

    expect(store.pending.size).toBe(0)
    expect(store.maturingIndex.get('tx1')).toBe(101)
    const rec = store.records.get('tx1')!
    expect(rec.height).toBe(101)
    expect(rec.blockHash).toBe('b101')
    expect(rec.fired).toEqual([1])

    const confirmed = confirmedEvents(store)
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
    expect(await store.ringHashAt(101)).toBe('b101')
    expect(store.evaluated.has('tx1')).toBe(false) // tip prune: it left the mempool
  })

  it('a matched tx never seen in the mempool still confirms', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx9 = mkTx('tx9') // never pending, never evaluated
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx9] })

    await process(chain.raw('b101'))

    expect(store.maturingIndex.get('tx9')).toBe(101)
    expect(store.records.get('tx9')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1], hex: 'hex-tx9' })
    const confirmed = confirmedEvents(store)
    expect(confirmed).toHaveLength(1)
    expect(confirmed[0]!.idempotencyKey).toBe('regtest:tx9:confirmed:1:b101')
  })

  it('promotion ignores the evaluated flag: evaluated with NO record still confirms', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    store.evaluated.add('tx1') // no record, no pending, not maturing
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })

    await process(chain.raw('b101'))

    expect(store.maturingIndex.get('tx1')).toBe(101)
    expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })
    expect(confirmedEvents(store).map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101'])
  })

  it('a tx evaluated as no-match in the mempool, whose address is watched afterwards, confirms when mined', async () => {
    const { store, chain, process, cfg } = setup()
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    const evaluate = makeTxEvaluator({ store, cfg: { network: cfg.network, seenEnabled: true } })
    await evaluate(tx1) // nothing watched yet → evaluated, no match
    expect(store.evaluated.has('tx1')).toBe(true)
    expect(store.outboxEvents()).toHaveLength(0)

    store.watches.add(ADDR) // the consumer starts watching after the tx was already in the mempool
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })

    await process(chain.raw('b101'))

    expect(store.maturingIndex.get('tx1')).toBe(101)
    expect(confirmedEvents(store).map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101'])
  })

  it('a block is ONE MULTI: a crash before exec leaves nothing behind (no promotion, no event, tip unchanged) and the replay processes it once', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })

    const applyBlock = store.applyBlock.bind(store)
    store.applyBlock = async () => {
      throw new Error('redis went away')
    }
    await expect(process(chain.raw('b101'))).rejects.toThrow('redis went away')
    expect(store.tip).toEqual({ hash: 'b100', height: 100 })
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.maturingIndex.has('tx1')).toBe(false)
    expect(store.records.get('tx1')).toMatchObject({ height: 0, fired: [] })
    expect(store.outboxEvents()).toHaveLength(0)

    // After the restart boot reconciliation replays the block: tip is still b100, so it is not a duplicate.
    store.applyBlock = applyBlock
    await process(chain.raw('b101'))

    expect(confirmedEvents(store).map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101']) // once
    expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })
    expect(store.maturingIndex.get('tx1')).toBe(101)
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.tip).toEqual({ hash: 'b101', height: 101 })
    expect(await store.ringHashAt(101)).toBe('b101')

    // and the same block a third time (tip already moved) is a duplicate: skipped
    await process(chain.raw('b101'))
    expect(confirmedEvents(store)).toHaveLength(1)
  })

  it('milestone 3 fires two blocks after the inclusion block, then tracking ends (record + index gone, claims released)', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1', ADDR, 5000, [{ txid: 'prev', vout: 0 }])
    seedPending(store, tx1)
    store.records.get('tx1')!.inputs = tx1.inputs
    store.outpoints.set('prev:0', new Set(['tx1']))
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
    chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })

    await process(chain.raw('b101'))
    await process(chain.raw('b102'))
    expect(confirmedEvents(store)).toHaveLength(1) // confs=2 fires nothing new

    await process(chain.raw('b103'))
    const confirmed = confirmedEvents(store)
    expect(confirmed).toHaveLength(2)
    expect(confirmed[1]!.confs).toBe(3)
    expect(confirmed[1]!.idempotencyKey).toBe('regtest:tx1:confirmed:3:b101')
    expect(confirmed[1]!.timestamp).toBe(1_700_000_103 * 1000)
    expect(store.maturingIndex.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)
    expect(store.outpoints.size).toBe(0)
  })

  it('a jump across two milestones fires both in one sweep', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 102, 'b102')
    // tx maturing since height 100 with nothing fired yet
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.maturingIndex.set('tx1', 100)
    store.records.set('tx1', { txid: 'tx1', height: 100, blockHash: 'b100', matched: MATCHED, fired: [], hex: 'hex-tx1' })
    chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })

    await process(chain.raw('b103')) // confs = 103 - 100 + 1 = 4

    const confirmed = confirmedEvents(store)
    expect(confirmed.map((e) => e.idempotencyKey)).toEqual([
      'regtest:tx1:confirmed:1:b100',
      'regtest:tx1:confirmed:3:b100',
    ])
    expect(store.maturingIndex.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)
  })

  it('gap catch-up processes intermediate blocks in order', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
    chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })

    await process(chain.raw('b103')) // only the newest block arrives

    expect(chain.getBlockHashCalls).toEqual([101, 102])
    expect(await store.ringHashAt(101)).toBe('b101')
    expect(await store.ringHashAt(102)).toBe('b102')
    expect(await store.ringHashAt(103)).toBe('b103')
    expect(store.tip).toEqual({ hash: 'b103', height: 103 })
    // milestones enqueued in block order during catch-up: 1 at b101's sweep, 3 at b103's
    const confirmed = confirmedEvents(store)
    expect(confirmed.map((e) => e.idempotencyKey)).toEqual([
      'regtest:tx1:confirmed:1:b101',
      'regtest:tx1:confirmed:3:b101',
    ])
    expect(confirmed[0]!.timestamp).toBe(1_700_000_101 * 1000)
    expect(confirmed[1]!.timestamp).toBe(1_700_000_103 * 1000)
    expect(store.maturingIndex.has('tx1')).toBe(false)
  })

  it('metrics: weir_blocks_processed_total counts every processed block (catch-up included, duplicates excluded); weir_reorgs_total counts reorgs', async () => {
    metrics.reset()
    const { store, chain, process } = setup()
    seedTip(store, chain, 100, 'b100')
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })

    await process(chain.raw('b102')) // gap: b101 walked, then b102
    await process(chain.raw('b102')) // duplicate (already tip) — skipped, not counted
    expect(metrics.counters.get('weir_blocks_processed_total')).toBe(2)
    expect(metrics.counters.get('weir_reorgs_total')).toBe(0)

    // reorg: b102 replaced by b102b on b101
    chain.addBlock({ hash: 'b102b', prevHash: 'b101', height: 102, time: 1_700_000_112, txs: [] })
    await process(chain.raw('b102b'))
    expect(metrics.counters.get('weir_reorgs_total')).toBe(1)
    expect(metrics.counters.get('weir_blocks_processed_total')).toBe(3)
  })

  it('reorg with re-inclusion resets fired and re-fires with the new blockHash idempotency key', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    const tx1 = mkTx('tx1')
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] }, { main: false })
    // weir's view: tip = b101a, tx1 maturing there with milestone 1 already fired
    store.tip = { hash: 'b101a', height: 101 }
    store.ring.set('b100', 100)
    store.ring.set('b101a', 101)
    store.maturingIndex.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-tx1' })
    // the new chain re-includes tx1 in b101b
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [tx1] })
    chain.addBlock({ hash: 'b102b', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [] })

    await process(chain.raw('b102b'))

    const confirmed = confirmedEvents(store)
    expect(confirmed).toHaveLength(1)
    expect(confirmed[0]!.idempotencyKey).toBe('regtest:tx1:confirmed:1:b101b')
    expect(confirmed[0]!.blockHash).toBe('b101b')
    const rec = store.records.get('tx1')!
    expect(rec.blockHash).toBe('b101b')
    expect(rec.height).toBe(101)
    expect(rec.fired).toEqual([1])
    expect(store.maturingIndex.get('tx1')).toBe(101)
    expect(store.tip).toEqual({ hash: 'b102b', height: 102 })
    expect(store.outboxEvents().filter((e) => e.event === 'demoted' || e.event === 'conflicted')).toHaveLength(0)
  })

  it('reorg demotion emits demoted with the old block fields and returns the tx to pending', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.tip = { hash: 'b101a', height: 101 }
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [mkTx('tx1')] }, { main: false })
    store.ring.set('b100', 100)
    store.ring.set('b101a', 101)
    store.maturingIndex.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-tx1' })
    // replacement block does NOT contain tx1; tx1 went back to the node's mempool
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    chain.mempoolEntries.set('tx1', { time: 1 })
    chain.mempool = ['tx1']

    await process(chain.raw('b101b'))

    const demoted = store.outboxEvents().filter((e): e is TxEvent => e.event === 'demoted')
    expect(demoted).toHaveLength(1)
    expect(demoted[0]!.txid).toBe('tx1')
    expect(demoted[0]!.confs).toBe(0)
    expect(demoted[0]!.blockHeight).toBe(101)
    expect(demoted[0]!.blockHash).toBe('b101a')
    expect(demoted[0]!.hex).toBe('hex-tx1')
    expect(demoted[0]!.idempotencyKey).toBe('regtest:tx1:demoted:b101a')

    expect(store.pending.has('tx1')).toBe(true)
    expect(store.evaluated.has('tx1')).toBe(true) // back in the mempool: the reparse must not re-fire seen
    expect(store.maturingIndex.has('tx1')).toBe(false)
    const rec = store.records.get('tx1')!
    expect(rec.height).toBe(0)
    expect(rec.blockHash).toBe('')
    expect(rec.fired).toEqual([])
    expect(store.tip).toEqual({ hash: 'b101b', height: 101 })
  })

  it('conflicted (gone from mempool and chain) emits with last confirmed depth and cleans up fully', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [mkTx('tx1')] }, { main: false })
    store.tip = { hash: 'b101a', height: 101 }
    store.ring.set('b100', 100)
    store.ring.set('b101a', 101)
    store.maturingIndex.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-tx1' })
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    // no mempool entry, no verbose result → conflicted

    await process(chain.raw('b101b'))

    const conflicted = store.outboxEvents().filter((e): e is TxEvent => e.event === 'conflicted')
    expect(conflicted).toHaveLength(1)
    expect(conflicted[0]!.confs).toBe(1) // last confirmed depth
    expect(conflicted[0]!.blockHeight).toBe(101)
    expect(conflicted[0]!.blockHash).toBe('b101a')
    expect(conflicted[0]!.idempotencyKey).toBe('regtest:tx1:conflicted')

    expect(store.maturingIndex.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.limbo.size).toBe(0)
  })

  it('dropped (reason evicted) emits, removes pending, un-evaluates (seen can re-fire) and deletes the record', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    // block without tx1; mempool without tx1 → dropped
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    chain.mempool = []

    await process(chain.raw('b101'))

    const dropped = store.outboxEvents().filter((e): e is TxEvent => e.event === 'dropped')
    expect(dropped).toHaveLength(1)
    expect(dropped[0]!.txid).toBe('tx1')
    expect(dropped[0]!.confs).toBe(0)
    expect(dropped[0]!.blockHeight).toBeNull()
    expect(dropped[0]!.blockHash).toBeNull()
    expect(dropped[0]!.matched).toEqual(MATCHED) // from the seen-time record
    expect(dropped[0]!.hex).toBe('hex-tx1')
    expect(dropped[0]!.idempotencyKey).toBe('regtest:tx1:dropped:101')
    expect(dropped[0]!.reason).toBe('evicted') // no replacement was seen: the residual verdict
    expect(dropped[0]!.replacedBy).toBeUndefined()

    expect(store.pending.has('tx1')).toBe(false)
    expect(store.evaluated.has('tx1')).toBe(false) // rebroadcast → seen can fire again
    expect(store.records.has('tx1')).toBe(false)
  })

  it('TTL sweep emits expired and removes the watch', async () => {
    const { store, chain, process } = setup()
    const addr = 'bcrt1qttlwatch'
    const expiresAtMs = Date.now() - 60_000
    store.watches.add(addr)
    store.expiries.set(addr, expiresAtMs)
    seedTip(store, chain, 100, 'b100')
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })

    await process(chain.raw('b101'))

    const expired = store.outboxEvents().filter((e) => e.event === 'expired')
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

  it('a pending tx mined in a LATER missed block is NOT falsely dropped during catch-up', async () => {
    // The eviction check compares pending against the LIVE mempool, which is meaningless for
    // historical blocks — it must only run for the tip block.
    const { store, chain, process } = setup()
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

    expect(store.outboxEvents().filter((e) => e.event === 'dropped')).toHaveLength(0)
    const confirmed = confirmedEvents(store)
    expect(confirmed.map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b102'])
    expect(store.maturingIndex.get('tx1')).toBe(102)
    expect(store.pending.has('tx1')).toBe(false)
  })

  it('tip-only work runs only when the block is the node\'s CURRENT tip: a block processed while the node is already further ahead defers eviction, TTL and limbo resolution to the next block', async () => {
    // A queued burst (or a reorg in progress): weir processes b101 while the node's best is
    // already b102. The live mempool then describes b102's world, not b101's — tx1 was mined
    // in b102, so b101's eviction check would falsely report it dropped.
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    const addr = 'bcrt1qttlwatch'
    store.watches.add(addr)
    store.expiries.set(addr, Date.now() - 60_000)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [tx1] })
    chain.mempool = [] // the node is at b102: tx1 left the mempool by being mined there

    await process(chain.raw('b101')) // best is b102 → the block's MULTI lands, the tip work does not

    expect(store.tip).toEqual({ hash: 'b101', height: 101 })
    expect(store.outboxEvents()).toHaveLength(0) // no false dropped, and no expired yet
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.watches.has(addr)).toBe(true)

    await process(chain.raw('b102')) // now the tip: promotion in the MULTI, then the deferred tip work

    expect(store.outboxEvents().map((e) => e.event)).toEqual(['confirmed', 'expired'])
    expect(store.maturingIndex.get('tx1')).toBe(102)
    expect(store.watches.has(addr)).toBe(false)
  })

  it('a second reorg inside the ring finds the fork point and leaves a still-canonical tx alone', async () => {
    // Regression: stale disconnected hashes left in the ring poisoned the NEXT fork search.
    // The stale hash (b101z) deliberately sorts AFTER the canonical one (b101b): with the
    // rewind missing, redis tie order makes findForkPoint pick the stale hash and the
    // second reorg emits a false `conflicted` for a still-canonical tx.
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    const tx1 = mkTx('tx1')
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    chain.addBlock({ hash: 'b101z', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] }, { main: false })
    store.tip = { hash: 'b101z', height: 101 }
    store.ring.set('b100', 100)
    store.ring.set('b101z', 101)
    store.maturingIndex.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101z', matched: MATCHED, fired: [1], hex: 'hex-tx1' })

    // reorg 1
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [tx1] })
    chain.addBlock({ hash: 'b102b', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [] })
    await process(chain.raw('b102b'))

    expect(await store.ringAll()).toEqual([
      { height: 100, hash: 'b100' },
      { height: 101, hash: 'b101b' },
      { height: 102, hash: 'b102b' },
    ])
    expect(store.limbo.size).toBe(0) // re-included → left limbo during promotion

    // reorg 2: fork at b101b, b102b disconnected
    chain.addBlock({ hash: 'b102c', prevHash: 'b101b', height: 102, time: 1_700_000_122, txs: [] })
    chain.addBlock({ hash: 'b103c', prevHash: 'b102c', height: 103, time: 1_700_000_123, txs: [] })
    await process(chain.raw('b103c'))

    expect(store.tip).toEqual({ hash: 'b103c', height: 103 })
    expect(await store.ringAll()).toEqual([
      { height: 100, hash: 'b100' },
      { height: 101, hash: 'b101b' },
      { height: 102, hash: 'b102c' },
      { height: 103, hash: 'b103c' },
    ])
    expect(store.limbo.size).toBe(0)
    expect(store.outboxEvents().filter((e) => e.event === 'conflicted' || e.event === 'demoted')).toHaveLength(0)
    expect(confirmedEvents(store).map((e) => e.idempotencyKey)).toEqual([
      'regtest:tx1:confirmed:1:b101b',
      'regtest:tx1:confirmed:3:b101b',
    ])
    expect(store.maturingIndex.has('tx1')).toBe(false) // tracking ended at 3 confs
  })

  it('findForkPoint sees a ring entry at height 0 (fresh regtest genesis) — no "ring is empty" path', async () => {
    const { store, chain, deps } = setup()
    chain.addBlock({ hash: 'genesis', prevHash: '', height: 0, time: 1_700_000_000, txs: [] })
    store.ring.set('genesis', 0)
    vi.mocked(console.error).mockClear()

    const result = await findForkPoint(deps, 'genesis', 1)

    expect(result).toEqual({ ancestorHeight: 0, disconnected: [] })
    expect(console.error).not.toHaveBeenCalled()
  })

  it('a reorg at height 1 on a fresh regtest chain is detected (ring floor at height 0)', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'genesis', prevHash: '', height: 0, time: 1_700_000_000, txs: [] })
    chain.addBlock({ hash: 'b1a', prevHash: 'genesis', height: 1, time: 1_700_000_001, txs: [mkTx('tx1')] }, { main: false })
    store.tip = { hash: 'b1a', height: 1 }
    store.ring.set('genesis', 0)
    store.ring.set('b1a', 1)
    store.maturingIndex.set('tx1', 1)
    store.records.set('tx1', { txid: 'tx1', height: 1, blockHash: 'b1a', matched: MATCHED, fired: [1], hex: 'hex-tx1' })
    chain.addBlock({ hash: 'b1b', prevHash: 'genesis', height: 1, time: 1_700_000_011, txs: [] })
    chain.mempoolEntries.set('tx1', { time: 1 })
    chain.mempool = ['tx1']

    await process(chain.raw('b1b'))

    const demoted = store.outboxEvents().filter((e): e is TxEvent => e.event === 'demoted')
    expect(demoted.map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:demoted:b1a'])
    expect(store.maturingIndex.has('tx1')).toBe(false)
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.tip).toEqual({ hash: 'b1b', height: 1 })
    expect(await store.ringHashAt(1)).toBe('b1b') // b1a rewound out of the ring
    expect(chain.getBlockHashCalls).toEqual([]) // no gap walk
  })

  it('downtime beyond the prune window resets tracking loudly instead of crash-looping', async () => {
    const { store, chain, process } = setup()
    store.watches.add(ADDR)
    seedTip(store, chain, 100, 'b100')
    // in-flight state that will be unrecoverable
    const tx1 = mkTx('tx1')
    seedPending(store, tx1)
    store.maturingIndex.set('tx2', 100)
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
    expect(store.outboxEvents()).toHaveLength(0)
    expect(store.pending.size).toBe(0)
    expect(store.maturingIndex.size).toBe(0)
    expect(store.records.size).toBe(0)
    expect(store.watches.has(ADDR)).toBe(true)
    expect(store.tip).toEqual({ hash: 'b104', height: 104 })
    // no attempt to fetch pruned blocks
    expect(chain.getBlockHashCalls).toEqual([])
  })
})
