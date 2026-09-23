import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { makeRawTxHandler, makeTxEvaluator } from '../src/engine/txPipeline'
import { makeMempoolReparser } from '../src/engine/mempool'
import type { DecodedTx, TxEvent } from '../src/lib/types'
import { ADDR, FakeStore, mkTx } from './fakes'

function setup(overrides: { seenEnabled?: boolean } = {}) {
  const store = new FakeStore()
  const cfg = { network: 'regtest' as const, seenEnabled: overrides.seenEnabled ?? true }
  /** the node: every tx these tests evaluate is in its mempool (a packet is a mempool sighting) */
  const rpc = { getMempoolEntry: async () => ({}) }
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
    expect(ev.matched).toEqual([
      { address: ADDR, vout: 0, valueSats: 5000 },
      { address: ADDR, vout: 2, valueSats: 7000 },
    ])

    expect(store.pending.has('tx1')).toBe(true)
    expect(store.evaluated.has('tx1')).toBe(true)
    // seen-time record: height 0, blockHash '', so dropped/mined transitions have hex+matched
    expect(store.records.get('tx1')).toMatchObject({ height: 0, blockHash: '', fired: [], hex: 'hex-tx1' })
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
