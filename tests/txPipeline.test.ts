import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { makeRawTxHandler, makeTxEvaluator } from '../src/engine/txPipeline'
import { makeBlockProcessor } from '../src/engine/blockPipeline'
import { makeMempoolReparser } from '../src/engine/mempool'
import type { DecodedTx, TxEvent } from '../src/lib/types'
import type { MempoolEntry } from '../src/bitcoin/rpc'
import { ADDR, DEFAULT_MEMPOOL_ENTRY, FakeChain, FakeStore, mkTx } from './fakes'

function setup(overrides: { seenEnabled?: boolean; entry?: MempoolEntry } = {}) {
  const store = new FakeStore()
  const cfg = { network: 'regtest' as const, seenEnabled: overrides.seenEnabled ?? true }
  /** the node: every tx these tests evaluate is in its mempool (a packet is a mempool sighting) */
  const rpc = { getMempoolEntry: async () => overrides.entry ?? DEFAULT_MEMPOOL_ENTRY }
  const evaluate = makeTxEvaluator({ store, rpc, cfg })
  return { store, cfg, rpc, evaluate }
}

/** Every ENQUEUED seen event (tests/fakes.ts outbox), in enqueue order. */
const seenEvents = (store: FakeStore): TxEvent[] => store.outboxEvents().filter((e): e is TxEvent => e.event === 'seen')

describe('txPipeline', () => {
  beforeAll(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
  })
  afterAll(() => {
    vi.restoreAllMocks()
  })

  it('seen is ENQUEUED with ALL matched outputs and the right idempotency key; record + pending + evaluated persist in the same step', async () => {
    const { store, evaluate } = setup()
    store.watches.add(ADDR)
    // tx pays the watched address twice — both outputs must appear in matched
    const tx: DecodedTx = {
      txid: 'tx1',
      hex: 'hex-tx1',
      inputs: [],
      outputs: [
        { vout: 0, valueSats: 5000, address: ADDR, scriptType: 'p2wpkh' },
        { vout: 2, valueSats: 7000, address: ADDR, scriptType: 'p2wpkh' },
        { vout: 1, valueSats: 111, address: 'bcrt1qother', scriptType: 'p2wpkh' },
      ],
    }

    await evaluate(tx)

    expect(store.outboxEvents()).toHaveLength(1)
    const ev = store.outboxEvents()[0] as TxEvent
    expect(ev.event).toBe('seen')
    expect(ev.confs).toBe(0)
    expect(ev.idempotencyKey).toBe('regtest:tx1:seen')
    expect(ev.blockHeight).toBeNull()
    expect(ev.hex).toBe('hex-tx1')
    expect(ev.feeRateSatVb).toBe(10) // DEFAULT_MEMPOOL_ENTRY: 1410 sat / 141 vB
    expect(ev.matched).toEqual([
      { address: ADDR, vout: 0, valueSats: 5000 },
      { address: ADDR, vout: 2, valueSats: 7000 },
    ])

    expect(store.pending.has('tx1')).toBe(true)
    expect(store.evaluated.has('tx1')).toBe(true)
    // seen-time record: height 0, blockHash '', so dropped/mined transitions have hex+matched
    expect(store.records.get('tx1')).toMatchObject({ height: 0, blockHash: '', fired: [], hex: 'hex-tx1' })
  })

  it('seen carries feeRateSatVb: the ancestor package rate from the probe entry, sat/vB rounded to one decimal; the idempotency key is unchanged', async () => {
    // 0.00002345 BTC over 226 vB = 10.376… sat/vB → 10.4
    const { store, evaluate } = setup({ entry: { ancestorsize: 226, fees: { ancestor: 0.00002345 } } })
    store.watches.add(ADDR)

    await evaluate(mkTx('tx1'))

    const ev = store.outboxEvents()[0] as TxEvent
    expect(ev.event).toBe('seen')
    expect(ev.feeRateSatVb).toBe(10.4)
    expect(ev.idempotencyKey).toBe('regtest:tx1:seen')
  })

  it('feeRateSatVb is OMITTED (not 0) when the node reports no usable fees or size', async () => {
    for (const entry of [
      {} as MempoolEntry,
      { ancestorsize: 226 } as MempoolEntry,
      { ancestorsize: 0, fees: { ancestor: 0.00002345 } },
      { ancestorsize: 226, fees: { ancestor: Number.NaN } },
    ]) {
      const { store, evaluate } = setup({ entry })
      store.watches.add(ADDR)

      await evaluate(mkTx('tx1'))

      const ev = store.outboxEvents()[0] as TxEvent
      expect(ev.event).toBe('seen')
      expect('feeRateSatVb' in ev).toBe(false)
      expect(ev.idempotencyKey).toBe('regtest:tx1:seen')
    }
  })

  it('dropped, confirmed, demoted and conflicted payloads never carry feeRateSatVb', async () => {
    // the tx pipeline and the block pipeline over one FakeStore/FakeChain (as tests/lifecycle.test.ts)
    const store = new FakeStore()
    const chain = new FakeChain()
    const cfg = { network: 'regtest' as const, seenEnabled: true, confirmMilestones: [1, 3], maxMilestone: 3, ringSize: 12 }
    const evaluate = makeTxEvaluator({ store, rpc: chain.rpc(), cfg })
    const process = makeBlockProcessor({ cfg, store, rpc: chain.rpc(), decodeBlock: chain.decode })
    const byEvent = (name: TxEvent['event']) => store.outboxEvents().filter((e): e is TxEvent => e.event === name)
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.tip = { hash: 'b100', height: 100 }
    store.ring.set('b100', 100)

    // mempool path: A seen; B (same input) replaces it → dropped A + seen B, both with a fee-bearing probe entry
    const fat = { ancestorsize: 226, fees: { ancestor: 0.00002345 } }
    chain.mempool = ['A']
    chain.setMempoolEntry('A', fat)
    await evaluate(mkTx('A', ADDR, 5000, [{ txid: 'prev', vout: 0 }]))
    const b = mkTx('B', ADDR, 5000, [{ txid: 'prev', vout: 0 }])
    chain.mempool = ['B']
    chain.setMempoolEntry('B', fat)
    await evaluate(b)
    expect(byEvent('seen').map((e) => e.feeRateSatVb)).toEqual([10.4, 10.4])
    expect(byEvent('dropped').map((e) => e.txid)).toEqual(['A'])

    // block path: b101a mines B → confirmed:1; b101b reorgs it out with B back in the mempool → demoted;
    // b102 re-mines B → confirmed:1; b102x reorgs it out with B gone from the mempool → conflicted
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [b] })
    chain.mempool = []
    chain.mempoolEntries.clear()
    await process(chain.raw('b101a'))
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    chain.mempool = ['B']
    await process(chain.raw('b101b'))
    chain.addBlock({ hash: 'b102', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [b] })
    chain.mempool = []
    await process(chain.raw('b102'))
    chain.addBlock({ hash: 'b102x', prevHash: 'b101b', height: 102, time: 1_700_000_122, txs: [] })
    await process(chain.raw('b102x'))

    expect(byEvent('confirmed').map((e) => e.txid)).toEqual(['B', 'B'])
    expect(byEvent('demoted').map((e) => e.txid)).toEqual(['B'])
    expect(byEvent('conflicted').map((e) => e.txid)).toEqual(['B'])
    for (const name of ['dropped', 'confirmed', 'demoted', 'conflicted'] as const) {
      for (const e of byEvent(name)) expect('feeRateSatVb' in e).toBe(false)
    }
  })

  it('a tx that is already tracked (mined record, no longer in `evaluated`) is a repeated sighting: nothing written, no seen', async () => {
    // bitcoind re-publishes rawtx for every tx of a connected or disconnected block, after
    // the tip prune forgot the txid from `evaluated`. `seen` is the unseen → pending
    // transition only; a maturing/limbo record must never be put back to height 0.
    const { store, evaluate } = setup()
    store.watches.add(ADDR)
    const tx = mkTx('tx1')
    store.maturingIndex.set('tx1', 101)
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101', matched: [{ address: ADDR, vout: 0, valueSats: 5000 }], fired: [1], hex: tx.hex, inputs: [] })

    await evaluate(tx)

    expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.evaluated.has('tx1')).toBe(false)
    expect(store.outboxEvents()).toHaveLength(0)
  })

  it('seen disabled (no 0 milestone): tracks silently — evaluated + pending + record, nothing enqueued', async () => {
    const { store, evaluate } = setup({ seenEnabled: false })
    store.watches.add(ADDR)

    await evaluate(mkTx('tx1'))

    expect(store.outboxEvents()).toHaveLength(0)
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.evaluated.has('tx1')).toBe(true)
    expect(store.records.has('tx1')).toBe(true)
  })

  it('already-evaluated txs are skipped entirely: a second sighting (ZMQ then reparse) enqueues nothing more', async () => {
    const { store, evaluate } = setup()
    store.watches.add(ADDR)

    await evaluate(mkTx('tx1'))
    await evaluate(mkTx('tx1'))

    expect(seenEvents(store).map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:seen'])
    expect(store.pending.has('tx1')).toBe(true)
  })

  it('no matched outputs → marked evaluated only', async () => {
    const { store, evaluate } = setup()
    // nothing watched

    await evaluate(mkTx('tx1'))

    expect(store.outboxEvents()).toHaveLength(0)
    expect(store.evaluated.has('tx1')).toBe(true)
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)
  })

  it('makeRawTxHandler decodes then evaluates', async () => {
    const { store, cfg } = setup()
    store.watches.add(ADDR)
    const tx = mkTx('tx1')
    const handler = makeRawTxHandler({
      store,
      rpc: { getMempoolEntry: async () => ({}) },
      cfg,
      decodeRawTx: (raw) => {
        expect(Buffer.isBuffer(raw)).toBe(true)
        return tx
      },
    })

    await handler(Buffer.from('00', 'hex'))

    expect(seenEvents(store).map((e) => e.txid)).toEqual(['tx1'])
  })
})

describe('mempool reparser', () => {
  beforeAll(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
  })
  afterAll(() => {
    vi.restoreAllMocks()
  })

  function reparserSetup() {
    const store = new FakeStore()
    const cfg = { network: 'regtest' as const, seenEnabled: true }
    const hexToTx = new Map<string, DecodedTx>()
    const rpcTxs = new Map<string, { blockhash?: string; hex: string }>()
    const mempool: string[] = []
    const evaluate = makeTxEvaluator({ store, rpc: { getMempoolEntry: async (txid: string) => (mempool.includes(txid) ? {} : null) }, cfg })
    /** register a tx with the fake node; `blockhash` = the node has mined it since the snapshot */
    const seed = (tx: DecodedTx, blockhash?: string) => {
      hexToTx.set(tx.hex, tx)
      rpcTxs.set(tx.txid, blockhash === undefined ? { hex: tx.hex } : { blockhash, hex: tx.hex })
    }
    const rpc = {
      getRawMempool: async () => [...mempool],
      getRawTransactionVerbose: async (txid: string) => rpcTxs.get(txid) ?? null,
    }
    const reparse = makeMempoolReparser({
      rpc,
      store,
      cfg,
      decodeRawTx: (raw) => {
        const tx = hexToTx.get(String(raw))
        if (!tx) throw new Error(`no fake tx for hex ${String(raw)}`)
        return tx
      },
      evaluate,
    })
    return { store, seed, mempool, reparse }
  }

  it('evaluates only txids not already evaluated (mempool − evaluated)', async () => {
    const { store, seed, mempool, reparse } = reparserSetup()
    store.watches.add(ADDR)
    const fresh = mkTx('fresh')
    const done = mkTx('done')
    seed(fresh)
    seed(done)
    mempool.push('fresh', 'done')
    store.evaluated = new Set(['done'])

    await reparse()

    expect(seenEvents(store).map((e) => e.txid)).toEqual(['fresh'])
  })

  it('a txid that vanishes between snapshot and fetch is skipped, not fatal', async () => {
    const { store, seed, mempool, reparse } = reparserSetup()
    store.watches.add(ADDR)
    seed(mkTx('kept'))
    mempool.push('kept', 'vanished') // 'vanished' has no rpc entry → verbose null

    await reparse()

    expect(seenEvents(store).map((e) => e.txid)).toEqual(['kept'])
  })

  it('a txid mined between snapshot and fetch (verbose.blockhash set) is skipped — no false seen', async () => {
    const { store, seed, mempool, reparse } = reparserSetup()
    store.watches.add(ADDR)
    seed(mkTx('mined'), 'b101') // in the snapshot, but getrawtransaction now reports a block
    seed(mkTx('still'))
    mempool.push('mined', 'still')

    await reparse()

    expect(seenEvents(store).map((e) => e.txid)).toEqual(['still'])
    // the block pipeline owns mined txs: nothing recorded for it here
    expect(store.evaluated.has('mined')).toBe(false)
    expect(store.pending.has('mined')).toBe(false)
    expect(store.records.has('mined')).toBe(false)
  })

  it('evaluations run one after the other even though fetches are batched (two spenders of one prevout: the second replaces the first)', async () => {
    const { store, seed, mempool, reparse } = reparserSetup()
    store.watches.add(ADDR)
    const prev = { txid: 'prev1', vout: 0 }
    seed(mkTx('A', ADDR, 5000, [prev]))
    seed(mkTx('B', ADDR, 4900, [prev]))
    mempool.push('A', 'B')

    await reparse()

    expect(store.outboxEvents().map((e) => e.event)).toEqual(['seen', 'dropped', 'seen'])
    expect(store.pending.has('A')).toBe(false)
    expect(store.pending.has('B')).toBe(true)
  })
})
