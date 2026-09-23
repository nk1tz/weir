import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { makeBlockProcessor } from '../src/engine/blockPipeline'
import { makeMempoolReparser } from '../src/engine/mempool'
import { makeEngineQueue } from '../src/engine/queue'
import { makeTxEvaluator } from '../src/engine/txPipeline'
import { startOutboxDrainer } from '../src/delivery/outbox'
import type { DecodedTx, TxEvent, WeirEvent } from '../src/lib/types'
import { ADDR, FakeChain, FakeSink, FakeStore, mkTx } from './fakes'

/**
 * Whole-lifecycle tests: the tx pipeline, the mempool reparser and the block pipeline wired
 * together over ONE shared FakeStore/FakeChain, so state handed from one module to the
 * next (evaluated, pending, records, limbo, the outbox) is exercised the way the daemon
 * uses it. Engine modules hold no sink: they enqueue; the drainer + FakeSink deliver.
 */
function wire(cfgOverrides: Partial<{ confirmMilestones: number[]; maxMilestone: number }> = {}) {
  const store = new FakeStore()
  const chain = new FakeChain()
  const cfg = { network: 'regtest' as const, seenEnabled: true, confirmMilestones: [1, 3], maxMilestone: 3, ringSize: 12, ...cfgOverrides }
  const byHex = new Map<string, DecodedTx>()
  /** make the node know a tx (getrawtransaction answers with its hex) */
  const register = (tx: DecodedTx): DecodedTx => {
    byHex.set(tx.hex, tx)
    chain.rawTxs.set(tx.txid, { hex: tx.hex })
    return tx
  }
  const rpc = chain.rpc()
  const evaluateRaw = makeTxEvaluator({ store, rpc, cfg })
  /** a rawtx packet: the tx is in the node's mempool when weir evaluates it */
  const evaluate = async (tx: DecodedTx): Promise<void> => {
    if (!chain.mempool.includes(tx.txid)) chain.mempool.push(tx.txid)
    await evaluateRaw(tx)
  }
  const reparse = makeMempoolReparser({
    rpc,
    store,
    cfg,
    decodeRawTx: (raw) => {
      const tx = byHex.get(String(raw))
      if (!tx) throw new Error(`no fake tx for hex ${String(raw)}`)
      return tx
    },
    evaluate: evaluateRaw,
  })
  const process = makeBlockProcessor({ cfg, store, rpc, decodeBlock: chain.decode })
  return { store, chain, rpc, register, evaluate, evaluateRaw, reparse, process }
}

const names = (events: WeirEvent[]): string[] => events.map((e) => e.event)
const enqueued = (store: FakeStore): string[] => names(store.outboxEvents())
const seenEnqueued = (store: FakeStore): TxEvent[] => store.outboxEvents().filter((e): e is TxEvent => e.event === 'seen')

describe('lifecycle', () => {
  beforeEach(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('seen → confirmed → reorg demotion → reparse enqueues exactly seen, confirmed, demoted — NO second seen', async () => {
    // After mining, the tip prune forgets the txid from `evaluated` (it left the mempool).
    // Demotion puts it back in the mempool + pending AND re-marks it evaluated, so the next
    // reparse does not re-fire `seen` (same idempotency key) or overwrite its record.
    const { store, chain, register, reparse, process } = wire()
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.tip = { hash: 'b100', height: 100 }
    store.ring.set('b100', 100)
    const tx1 = register(mkTx('tx1'))

    // 1. tx1 enters the mempool; a reparse enqueues `seen`
    chain.mempool = ['tx1']
    await reparse()
    expect(enqueued(store)).toEqual(['seen'])
    expect(store.pending.has('tx1')).toBe(true)

    // 2. mined in b101a and gone from the mempool → confirmed:1; the tip prune drops it from `evaluated`
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
    chain.mempool = []
    await process(chain.raw('b101a'))
    expect(enqueued(store)).toEqual(['seen', 'confirmed'])
    expect(store.evaluated.has('tx1')).toBe(false)

    // 3. b101a is reorged out by b101b (tx1 not re-included); the node returns tx1 to its mempool → demoted
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    chain.setMempoolEntry('tx1')
    chain.mempool = ['tx1']
    await process(chain.raw('b101b'))
    expect(enqueued(store)).toEqual(['seen', 'confirmed', 'demoted'])
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.limbo.size).toBe(0)
    const recordAfterDemotion = structuredClone(store.records.get('tx1'))
    expect(recordAfterDemotion).toMatchObject({ height: 0, blockHash: '', fired: [] })

    // 4. the next reparse (gap-triggered, or at boot) sees tx1 in the mempool again — already tracked
    await reparse()
    expect(enqueued(store)).toEqual(['seen', 'confirmed', 'demoted'])
    expect(seenEnqueued(store)).toHaveLength(1)
    expect(store.records.get('tx1')).toEqual(recordAfterDemotion)
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.evaluated.has('tx1')).toBe(true)

    // 5. re-mined in b102 → a fresh confirmed:1 under the new block hash, still no seen
    chain.addBlock({ hash: 'b102', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [tx1] })
    chain.mempool = []
    await process(chain.raw('b102'))
    expect(enqueued(store)).toEqual(['seen', 'confirmed', 'demoted', 'confirmed'])
    const confirmed = store.outboxEvents().filter((e): e is TxEvent => e.event === 'confirmed')
    expect(confirmed.map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101a', 'regtest:tx1:confirmed:1:b102'])
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.maturingIndex.get('tx1')).toBe(102)
  })

  it('end to end through the drainer: every enqueued event reaches the sink in order, survives a down endpoint, and the outbox empties', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_700_000_000_000)
    const { store, chain, register, reparse, process } = wire()
    const sink = new FakeSink()
    const drainer = startOutboxDrainer({ cfg: { outboxMaxAgeSec: 259_200, outboxDeadMax: 1000 }, store, sink })
    try {
      store.watches.add(ADDR)
      chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
      store.tip = { hash: 'b100', height: 100 }
      store.ring.set('b100', 100)
      const tx1 = register(mkTx('tx1'))

      // seen, delivered
      chain.mempool = ['tx1']
      await reparse()
      await expect(drainer.drainOnce()).resolves.toBe(1)
      expect(names(sink.delivered)).toEqual(['seen'])
      expect(store.outboxQueue.size).toBe(0)

      // confirmed:1 — but the endpoint is down: the event stays queued, the block pipeline never noticed
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
      chain.mempool = []
      sink.deliverResult = false
      await process(chain.raw('b101a'))
      expect(store.records.get('tx1')!.fired).toEqual([1]) // recorded at ENQUEUE time
      await expect(drainer.drainOnce()).resolves.toBe(0)
      expect(names(sink.delivered)).toEqual(['seen'])
      expect(store.outboxEvents().map((e) => e.event)).toEqual(['confirmed'])
      expect([...store.outbox.values()][0]).toMatchObject({ attempts: 1, lastError: 'HTTP 503' })

      // reorg while the endpoint is still down: demoted joins the queue; the pass retries the
      // confirmed once more (attempt 2) and pushes it BEHIND demoted (next-attempt order)
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
      chain.setMempoolEntry('tx1')
      chain.mempool = ['tx1']
      vi.setSystemTime(Date.now() + 2000)
      await process(chain.raw('b101b'))
      await drainer.drainOnce()
      expect(names(sink.delivered)).toEqual(['seen'])
      expect(enqueued(store).sort()).toEqual(['confirmed', 'demoted'])
      expect([...store.outbox.values()].find((r) => r.event?.event === 'confirmed')).toMatchObject({ attempts: 2 })

      // the endpoint recovers: once both are due they go out in score order — the retried
      // confirmed AFTER the younger demoted. That is the documented best-effort ordering:
      // nothing is lost, and consumers rely on absolute state + idempotencyKey, not arrival order.
      sink.deliverResult = true
      vi.setSystemTime(Date.now() + 10_000)
      await expect(drainer.drainOnce()).resolves.toBe(2)
      expect(names(sink.delivered)).toEqual(['seen', 'demoted', 'confirmed'])
      expect(store.outboxQueue.size).toBe(0)
      expect(store.outbox.size).toBe(0)

      // re-mined → confirmed:1 under the new hash; the interval loop delivers it by itself
      chain.addBlock({ hash: 'b102', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [tx1] })
      chain.mempool = []
      await process(chain.raw('b102'))
      await vi.advanceTimersByTimeAsync(1000)
      const delivered = sink.delivered.filter((e): e is TxEvent => e.event !== 'expired' && e.event !== 'heartbeat')
      expect(delivered.map((e) => e.idempotencyKey)).toEqual([
        'regtest:tx1:seen',
        'regtest:tx1:demoted:b101a',
        'regtest:tx1:confirmed:1:b101a',
        'regtest:tx1:confirmed:1:b102',
      ])
      expect(store.outboxQueue.size).toBe(0)
    } finally {
      await drainer.stop()
    }
  })

  describe('one writer: a rawtx and the block that mines it, queued together, run in arrival order', () => {
    // bitcoind publishes a tx's rawtx before the rawblock that includes it, and re-publishes
    // rawtx for every tx of a connected or disconnected block. On the engine queue those are
    // whole items, one after the other — there is no interleaving to guard against.
    function race() {
      const w = wire()
      w.store.watches.add(ADDR)
      w.chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
      w.store.tip = { hash: 'b100', height: 100 }
      w.store.ring.set('b100', 100)
      const tx1 = w.register(mkTx('tx1'))
      w.chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
      return { ...w, tx1, engine: makeEngineQueue(vi.fn()) }
    }

    it('rawtx first, then the block: one seen + one confirmed, record at the block height', async () => {
      const { store, chain, evaluateRaw, process, tx1, engine } = race()
      chain.mempool = ['tx1'] // the packet arrives while tx1 is in the mempool; the block then mines it

      await Promise.all([
        engine.run(() => evaluateRaw(tx1)),
        engine.run(async () => {
          chain.mempool = []
          await process(chain.raw('b101'))
        }),
      ])

      expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })
      expect(store.maturingIndex.get('tx1')).toBe(101)
      expect(store.pending.has('tx1')).toBe(false)
      expect(store.outboxEvents().map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:seen', 'regtest:tx1:confirmed:1:b101'])
    })

    it('the block first, then a late rawtx for the same tx (a block re-publish): confirmed only — the tracked record is never put back to height 0', async () => {
      const { store, chain, rpc, evaluateRaw, process, tx1, engine } = race()
      chain.mempool = [] // tx1 is MINED: genuinely absent from the node's mempool
      const probe = vi.spyOn(rpc, 'getMempoolEntry')

      await Promise.all([engine.run(() => process(chain.raw('b101'))), engine.run(() => evaluateRaw(tx1))])

      expect(probe).not.toHaveBeenCalled() // already tracked: the sighting is dismissed before any probe
      expect(store.evaluated.has('tx1')).toBe(false) // nothing written by the late packet
      expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })
      expect(store.maturingIndex.get('tx1')).toBe(101)
      expect(store.pending.has('tx1')).toBe(false)
      expect(store.outboxEvents().map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101'])

      // and the next blocks confirm normally (tracking was never corrupted)
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
      chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
      await engine.run(() => process(chain.raw('b102')))
      await engine.run(() => process(chain.raw('b103')))
      expect(enqueued(store)).toEqual(['confirmed', 'confirmed'])
      expect(store.maturingIndex.has('tx1')).toBe(false) // tracking ended at 3 confs
      expect(store.records.has('tx1')).toBe(false)
    })
  })

  it('REGRESSION (resolveLimbo discards the validated snapshot): a limbo tx that leaves the mempool after the tip snapshot is still resolved FROM the snapshot — demoted, never a live re-probe', async () => {
    // The tip path snapshots getrawmempool, confirms best == B, then resolves limbo. A live
    // getmempoolentry at that point could see a block that landed after the confirmation
    // (the tx re-mined there) and say `conflicted` instead of `demoted`.
    const { store, chain, rpc, register, evaluate, process } = wire()
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.tip = { hash: 'b100', height: 100 }
    store.ring.set('b100', 100)
    const tx1 = register(mkTx('tx1'))
    await evaluate(tx1)
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
    chain.mempool = []
    await process(chain.raw('b101a'))
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    chain.mempool = ['tx1']
    // the snapshot says tx1 is in the mempool; right after it is taken the node moves on (tx1 mined elsewhere)
    const getRawMempool = rpc.getRawMempool
    rpc.getRawMempool = async () => {
      const snapshot = await getRawMempool()
      chain.mempool = []
      chain.mempoolEntries.clear()
      return snapshot
    }
    const probe = vi.spyOn(rpc, 'getMempoolEntry')

    await process(chain.raw('b101b'))

    expect(enqueued(store)).toEqual(['seen', 'confirmed', 'demoted'])
    expect(probe).not.toHaveBeenCalled() // the outcome came from the snapshot, not a later probe
    expect(store.pending.has('tx1')).toBe(true)
  })

  it('a rebroadcast after an eviction re-fires seen (drops are not terminal)', async () => {
    const { store, chain, register, evaluate, process } = wire()
    store.watches.add(ADDR)
    const tx1 = register(mkTx('tx1'))
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.tip = { hash: 'b100', height: 100 }
    store.ring.set('b100', 100)
    await evaluate(tx1)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    chain.mempool = [] // gone without being mined
    await process(chain.raw('b101'))
    expect(enqueued(store)).toEqual(['seen', 'dropped'])
    expect((store.outboxEvents()[1] as TxEvent).reason).toBe('evicted')
    expect(store.evaluated.has('tx1')).toBe(false)

    await evaluate(tx1) // the rebroadcast

    expect(enqueued(store)).toEqual(['seen', 'dropped', 'seen'])
    expect(store.pending.has('tx1')).toBe(true)
  })

  it('two milestones enqueued in the same millisecond deliver 1 then 3', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_700_000_000_000) // Date.now pinned: both confirmed events get the same score
    const { store, chain, process } = wire()
    const sink = new FakeSink()
    const drainer = startOutboxDrainer({ cfg: { outboxMaxAgeSec: 259_200, outboxDeadMax: 1000 }, store, sink })
    try {
      store.watches.add(ADDR)
      chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
      store.tip = { hash: 'b102', height: 102 }
      store.ring.set('b102', 102)
      store.maturingIndex.set('tx1', 100)
      store.records.set('tx1', { txid: 'tx1', height: 100, blockHash: 'b100', matched: [{ address: ADDR, vout: 0, valueSats: 5000 }], fired: [], hex: 'hex-tx1' })
      chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })

      await process(chain.raw('b103')) // confs = 4 → milestones 1 and 3 in one sweep, same ms
      const scores = [...store.outboxQueue.values()]
      expect(scores).toEqual([1_700_000_000_000, 1_700_000_000_000])

      await drainer.drainOnce()

      expect(sink.delivered.map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b100', 'regtest:tx1:confirmed:3:b100'])
    } finally {
      await drainer.stop()
    }
  })
})
