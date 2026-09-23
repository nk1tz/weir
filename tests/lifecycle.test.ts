import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { makeBlockProcessor, TOMBSTONE_TTL_MS } from '../src/engine/blockPipeline'
import { makeMempoolReparser } from '../src/engine/mempool'
import { makeTxEvaluator } from '../src/engine/txPipeline'
import { startOutboxDrainer } from '../src/delivery/outbox'
import type { DecodedTx, TxEvent, WeirEvent } from '../src/lib/types'
import { MAX_EVALUATION_AGE_MS } from '../src/store/redis'
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
  const evaluate = makeTxEvaluator({ store, cfg })
  const reparse = makeMempoolReparser({
    rpc,
    store,
    cfg,
    decodeRawTx: (raw) => {
      const tx = byHex.get(String(raw))
      if (!tx) throw new Error(`no fake tx for hex ${String(raw)}`)
      return tx
    },
    evaluate,
  })
  const process = makeBlockProcessor({ cfg, store, rpc, decodeBlock: chain.decode })
  return { store, chain, register, evaluate, reparse, process }
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
    // Regression: after mining, the tip prune forgets the txid from `evaluated` (it left the
    // mempool). Demotion put it back in the mempool + pending WITHOUT re-marking it evaluated,
    // so the next reparse re-evaluated it and emitted a duplicate `seen` (same idempotency
    // key) while overwriting its record. Demotion must be one atomic step that also SADDs
    // `evaluated`.
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
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] }, { main: false })
    chain.mempool = []
    await process(chain.raw('b101a'))
    expect(enqueued(store)).toEqual(['seen', 'confirmed'])
    expect(store.evaluated.has('tx1')).toBe(false)

    // 3. b101a is reorged out by b101b (tx1 not re-included); the node returns tx1 to its mempool → demoted
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    chain.mempoolEntries.set('tx1', { time: 1 })
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
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] }, { main: false })
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
      chain.mempoolEntries.set('tx1', { time: 1 })
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

  describe('RACE: a block promotes a tx while the tx pipeline is evaluating the same tx', () => {
    // Before the outbox, the evaluator awaited the webhook INSIDE the evaluation; a block
    // arriving meanwhile promoted the tx, and the evaluator's late write put the record back
    // to height 0 (tracking corrupted: confirmations lost). With no network await left, only
    // the ordering of two store steps remains — and recordSeen refuses to overwrite a mined
    // record, so BOTH orderings end with the record at the block height.

    function race(cfgOverrides: Partial<{ confirmMilestones: number[]; maxMilestone: number }> = {}) {
      const w = wire(cfgOverrides)
      w.store.watches.add(ADDR)
      w.chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
      w.store.tip = { hash: 'b100', height: 100 }
      w.store.ring.set('b100', 100)
      const tx1 = w.register(mkTx('tx1'))
      w.chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
      w.chain.mempool = []
      return { ...w, tx1 }
    }

    it('evaluation parked mid-flight, block completes first: record stays at the block height, ONE confirmed, no stale seen', async () => {
      const { store, chain, evaluate, process, tx1 } = race()
      // Park the evaluator inside its match step (after it read `isEvaluated` = false).
      let release!: () => void
      const gate = new Promise<void>((r) => {
        release = r
      })
      let parkedResolve!: () => void
      const parked = new Promise<void>((r) => {
        parkedResolve = r
      })
      const watchedSubset = store.watchedSubset.bind(store)
      let gateNext = true
      store.watchedSubset = async (addrs) => {
        if (gateNext) {
          gateNext = false
          parkedResolve()
          await gate
        }
        return watchedSubset(addrs)
      }
      const evaluation = evaluate(tx1, Date.now())
      await parked

      await process(chain.raw('b101')) // promotes tx1 → maturing@101, confirmed:1 enqueued, tip prune
      expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })

      release()
      await evaluation // its recordSeen lands AFTER the promotion

      expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] }) // never height 0
      expect(store.maturingIndex.get('tx1')).toBe(101)
      expect(store.pending.has('tx1')).toBe(false)
      expect(store.outboxEvents().map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101'])
    })

    it('evaluation lands first, block promotes right after: one seen + one confirmed, record at the block height', async () => {
      const { store, chain, evaluate, process, tx1 } = race()
      // The block is mid-flight (block txids + pending snapshot already read) when the
      // evaluator runs to completion; the promotion then follows.
      const promote = store.promoteToMaturing.bind(store)
      let evaluation: Promise<void> | null = null
      store.promoteToMaturing = async (rec) => {
        evaluation = evaluate(tx1, Date.now())
        await evaluation
        expect(store.records.get('tx1')).toMatchObject({ height: 0, blockHash: '' }) // seen landed first
        expect(store.pending.has('tx1')).toBe(true)
        return promote(rec)
      }

      await process(chain.raw('b101'))

      expect(evaluation).not.toBeNull()
      expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })
      expect(store.maturingIndex.get('tx1')).toBe(101)
      expect(store.pending.has('tx1')).toBe(false)
      expect(store.outboxEvents().map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:seen', 'regtest:tx1:confirmed:1:b101'])
    })

    it('in both orderings the next blocks confirm normally (tracking was never corrupted)', async () => {
      const { store, chain, evaluate, process, tx1 } = race()
      const promote = store.promoteToMaturing.bind(store)
      store.promoteToMaturing = async (rec) => {
        await evaluate(tx1, Date.now())
        return promote(rec)
      }
      await process(chain.raw('b101'))
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
      chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
      await process(chain.raw('b102'))
      await process(chain.raw('b103'))

      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'confirmed'])
      expect(store.maturingIndex.has('tx1')).toBe(false) // tracking ended at 3 confs
      expect(store.records.has('tx1')).toBe(false)
    })

    it('STALE EVALUATION after the final-milestone cleanup (milestones 0,1): confirmed:1 only — no seen, no false dropped', async () => {
      // The reviewer's interleaving: a mempool RPC read tx1 before the block and answers after
      // it. The block promotes, enqueues confirmed:1, hits the max milestone (1) → the record is
      // DELETED and the tip prune forgets the txid. Without a tombstone the late evaluator would
      // pass both other guards, recreate a height-0 pending record, enqueue `seen`, and the NEXT
      // block would emit `dropped` for a payment that confirmed.
      const { store, chain, evaluate, process, tx1 } = race({ confirmMilestones: [1], maxMilestone: 1 })
      let release!: () => void
      const gate = new Promise<void>((r) => {
        release = r
      })
      let parkedResolve!: () => void
      const parked = new Promise<void>((r) => {
        parkedResolve = r
      })
      const watchedSubset = store.watchedSubset.bind(store)
      let gateNext = true
      store.watchedSubset = async (addrs) => {
        if (gateNext) {
          gateNext = false
          parkedResolve()
          await gate
        }
        return watchedSubset(addrs)
      }
      const evaluation = evaluate(tx1, Date.now())
      await parked

      await process(chain.raw('b101')) // promote → confirmed:1 → final cleanup (record gone, tombstoned) → prune
      expect(store.records.has('tx1')).toBe(false)
      expect(store.evaluated.has('tx1')).toBe(false)
      expect(store.tombstones.has('tx1')).toBe(true)

      release()
      await evaluation // the stale write lands now

      expect(store.records.has('tx1')).toBe(false) // NOT resurrected
      expect(store.pending.has('tx1')).toBe(false)
      expect(enqueued(store)).toEqual(['confirmed'])

      // the next tip block: nothing pending, so nothing can be reported dropped
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
      await process(chain.raw('b102'))
      expect(enqueued(store)).toEqual(['confirmed'])
      expect(store.outboxEvents().map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101'])
    })
  })

  describe('tombstones', () => {
    it('a conflicted tx cannot re-enter tracking through a stale seen', async () => {
      const { store, chain, register, evaluate, process } = wire()
      store.watches.add(ADDR)
      const tx1 = register(mkTx('tx1'))
      chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] }, { main: false })
      store.tip = { hash: 'b101a', height: 101 }
      store.ring.set('b100', 100)
      store.ring.set('b101a', 101)
      store.maturingIndex.set('tx1', 101)
      store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101a', matched: [{ address: ADDR, vout: 0, valueSats: 5000 }], fired: [1], hex: tx1.hex })
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
      // not in the new chain, not in the mempool → conflicted (terminal)
      await process(chain.raw('b101b'))
      expect(enqueued(store)).toEqual(['conflicted'])
      expect(store.tombstones.has('tx1')).toBe(true)

      await evaluate(tx1, Date.now()) // a late/duplicate rawtx for the losing tx

      expect(enqueued(store)).toEqual(['conflicted'])
      expect(store.pending.has('tx1')).toBe(false)
      expect(store.records.has('tx1')).toBe(false)
    })

    it('tombstones are pruned after TOMBSTONE_TTL_MS at the next tip block; a genuinely new sighting is tracked again', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, chain, register, evaluate, process } = wire({ confirmMilestones: [1], maxMilestone: 1 })
      store.watches.add(ADDR)
      const tx1 = register(mkTx('tx1'))
      chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
      store.tip = { hash: 'b100', height: 100 }
      store.ring.set('b100', 100)
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
      await process(chain.raw('b101'))
      expect(store.tombstones.get('tx1')).toBe(1_700_000_000_000)

      // within the TTL: still tombstoned after another tip block; a stale seen is refused
      vi.setSystemTime(1_700_000_000_000 + TOMBSTONE_TTL_MS - 1)
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
      await process(chain.raw('b102'))
      expect(store.tombstones.has('tx1')).toBe(true)
      await evaluate(tx1, Date.now())
      expect(enqueued(store)).toEqual(['confirmed'])

      // past the TTL: pruned by the tip block
      vi.setSystemTime(1_700_000_000_000 + TOMBSTONE_TTL_MS)
      chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
      await process(chain.raw('b103'))
      expect(store.tombstones.size).toBe(0)
      await evaluate(tx1, Date.now())
      expect(enqueued(store)).toEqual(['confirmed', 'seen'])
    })

    it('EVALUATION FENCE: a mempool RPC parked past the tombstone TTL cannot land after the prune — confirmed:1 only', async () => {
      // The reviewer's interleaving: the reparser issues getrawtransaction before the block;
      // the response parks; the block mines the tx, fires confirmed:1 and finishes tracking
      // (tombstoned); an hour passes and the tip prune drops the tombstone; the response
      // arrives. The tombstone can no longer help — the fence (startedAtMs) refuses it.
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, chain, register, evaluate, process } = wire({ confirmMilestones: [1], maxMilestone: 1 })
      store.watches.add(ADDR)
      const tx1 = register(mkTx('tx1'))
      chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
      store.tip = { hash: 'b100', height: 100 }
      store.ring.set('b100', 100)
      chain.mempool = ['tx1']
      let release!: () => void
      const gate = new Promise<void>((r) => {
        release = r
      })
      // a reparser whose getrawtransaction parks until released (same store + evaluator)
      const rpc = chain.rpc()
      const reparseParked = makeMempoolReparser({
        rpc: {
          getRawMempool: rpc.getRawMempool,
          getRawTransactionVerbose: async (txid) => {
            await gate
            return rpc.getRawTransactionVerbose(txid)
          },
        },
        store,
        cfg: { network: 'regtest' },
        decodeRawTx: () => tx1,
        evaluate,
      })
      const reparsing = reparseParked() // startedAtMs = now; parked inside getrawtransaction

      // the block: promote → confirmed:1 → final cleanup → tombstone
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] })
      chain.mempool = []
      await process(chain.raw('b101'))
      expect(enqueued(store)).toEqual(['confirmed'])
      expect(store.tombstones.has('tx1')).toBe(true)

      // an hour later the tip prune drops the tombstone
      vi.setSystemTime(1_700_000_000_000 + TOMBSTONE_TTL_MS)
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
      await process(chain.raw('b102'))
      expect(store.tombstones.size).toBe(0)

      // the parked response finally arrives — fenced, not recorded, txid left un-evaluated
      release()
      await reparsing
      expect(store.records.has('tx1')).toBe(false)
      expect(store.pending.has('tx1')).toBe(false)
      expect(store.evaluated.has('tx1')).toBe(false)
      expect(enqueued(store)).toEqual(['confirmed'])

      // and the next tip block has nothing pending to report dropped
      chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
      await process(chain.raw('b103'))
      expect(store.outboxEvents().map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101'])
      expect(MAX_EVALUATION_AGE_MS).toBeLessThan(TOMBSTONE_TTL_MS)
    })

    it('dropped does NOT tombstone: a rebroadcast (an evaluation started AFTER the drop) re-fires seen; one started before it is refused', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, chain, register, evaluate, process } = wire()
      store.watches.add(ADDR)
      const tx1 = register(mkTx('tx1'))
      chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
      store.tip = { hash: 'b100', height: 100 }
      store.ring.set('b100', 100)
      await evaluate(tx1, Date.now())
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
      chain.mempool = [] // gone without being mined
      const beforeDrop = Date.now()
      vi.setSystemTime(beforeDrop + 1000)
      await process(chain.raw('b101'))
      expect(enqueued(store)).toEqual(['seen', 'dropped'])
      expect((store.outboxEvents()[1] as TxEvent).reason).toBe('evicted')
      expect(store.tombstones.size).toBe(0)
      expect(store.retired.get('tx1')).toBe(beforeDrop + 1000) // the retirement watermark, not a tombstone

      await evaluate(tx1, beforeDrop) // a duplicate evaluation that was in flight when it dropped: refused
      expect(enqueued(store)).toEqual(['seen', 'dropped'])
      expect(store.pending.has('tx1')).toBe(false)
      expect(store.evaluated.has('tx1')).toBe(false)

      vi.setSystemTime(beforeDrop + 2000)
      await evaluate(tx1, Date.now()) // a real rebroadcast

      expect(enqueued(store)).toEqual(['seen', 'dropped', 'seen'])
      expect(store.pending.has('tx1')).toBe(true)
    })
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
