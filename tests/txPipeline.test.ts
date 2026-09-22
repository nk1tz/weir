import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { makeRawTxHandler, makeTxEvaluator } from '../src/engine/txPipeline'
import { makeMempoolReparser } from '../src/engine/mempool'
import type { DecodedTx, TxEvent } from '../src/lib/types'
import { ADDR, FakeSink, FakeStore, mkTx } from './fakes'

function setup(overrides: { seenEnabled?: boolean } = {}) {
  const store = new FakeStore()
  const sink = new FakeSink()
  const cfg = { network: 'regtest' as const, seenEnabled: overrides.seenEnabled ?? true }
  const evaluate = makeTxEvaluator({ store, sink, cfg })
  return { store, sink, cfg, evaluate }
}

describe('txPipeline', () => {
  beforeAll(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
  })
  afterAll(() => {
    vi.restoreAllMocks()
  })

  it('seen fires with ALL matched outputs, the right idempotency key, and persists record + pending + evaluated', async () => {
    const { store, sink, evaluate } = setup()
    store.watches.add(ADDR)
    // tx pays the watched address twice — both outputs must appear in matched
    const tx: DecodedTx = {
      txid: 'tx1',
      hex: 'hex-tx1',
      outputs: [
        { vout: 0, valueSats: 5000, address: ADDR, scriptType: 'p2wpkh' },
        { vout: 2, valueSats: 7000, address: ADDR, scriptType: 'p2wpkh' },
        { vout: 1, valueSats: 111, address: 'bcrt1qother', scriptType: 'p2wpkh' },
      ],
    }

    await evaluate(tx)

    expect(sink.delivered).toHaveLength(1)
    const ev = sink.delivered[0] as TxEvent
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

  it('failed seen delivery leaves the tx un-evaluated (reparse retries), no pending, no record', async () => {
    const { store, sink, evaluate } = setup()
    store.watches.add(ADDR)
    sink.deliverResult = false

    await evaluate(mkTx('tx1'))

    expect(store.evaluated.has('tx1')).toBe(false)
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)

    // retry path: reparse re-evaluates and succeeds
    sink.deliverResult = true
    await evaluate(mkTx('tx1'))
    expect(store.pending.has('tx1')).toBe(true)
    expect(sink.attempts.filter((e) => e.event === 'seen')).toHaveLength(2) // both attempts captured
    expect(sink.delivered.filter((e) => e.event === 'seen')).toHaveLength(1) // only the retry succeeded
  })

  it('seen disabled (no 0 milestone): tracks silently — evaluated + pending + record, nothing delivered', async () => {
    const { store, sink, evaluate } = setup({ seenEnabled: false })
    store.watches.add(ADDR)

    await evaluate(mkTx('tx1'))

    expect(sink.delivered).toHaveLength(0)
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.evaluated.has('tx1')).toBe(true)
    expect(store.records.has('tx1')).toBe(true)
  })

  it('already-evaluated txs are skipped entirely', async () => {
    const { store, sink, evaluate } = setup()
    store.watches.add(ADDR)
    store.evaluated.add('tx1')

    await evaluate(mkTx('tx1'))

    expect(sink.delivered).toHaveLength(0)
    expect(store.pending.has('tx1')).toBe(false)
  })

  it('no matched outputs → markEvaluated only', async () => {
    const { store, sink, evaluate } = setup()
    // nothing watched

    await evaluate(mkTx('tx1'))

    expect(sink.delivered).toHaveLength(0)
    expect(store.evaluated.has('tx1')).toBe(true)
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.records.has('tx1')).toBe(false)
  })

  it('makeRawTxHandler decodes then evaluates', async () => {
    const { store, sink, cfg } = setup()
    store.watches.add(ADDR)
    const tx = mkTx('tx1')
    const handler = makeRawTxHandler({
      store,
      sink,
      cfg,
      decodeRawTx: (raw) => {
        expect(Buffer.isBuffer(raw)).toBe(true)
        return tx
      },
    })

    await handler(Buffer.from('00', 'hex'))

    expect(sink.delivered).toHaveLength(1)
    expect((sink.delivered[0] as TxEvent).txid).toBe('tx1')
  })
})

describe('mempool reparser', () => {
  function reparserSetup() {
    const store = new FakeStore()
    const sink = new FakeSink()
    const cfg = { network: 'regtest' as const, seenEnabled: true }
    const evaluate = makeTxEvaluator({ store, sink, cfg })
    const hexToTx = new Map<string, DecodedTx>()
    const rpcTxs = new Map<string, { blockhash?: string; hex: string }>()
    const mempool: string[] = []
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
    return { store, sink, seed, mempool, reparse }
  }

  it('evaluates only txids not already evaluated (current − evaluated), then drops the snapshot', async () => {
    const { store, sink, seed, mempool, reparse } = reparserSetup()
    store.watches.add(ADDR)
    const fresh = mkTx('fresh')
    const done = mkTx('done')
    seed(fresh)
    seed(done)
    mempool.push('fresh', 'done')
    store.evaluated = new Set(['done'])

    await reparse()

    const seen = sink.delivered.filter((e): e is TxEvent => e.event === 'seen')
    expect(seen.map((e) => e.txid)).toEqual(['fresh'])
    expect(store.mempoolCurrent.size).toBe(0)
  })

  it('a failed seen delivery is retried on the NEXT reparse of the same mempool', async () => {
    // Regression: `current − previous − evaluated` excluded a failed-delivery tx forever,
    // because rotateMempool moved it into `previous` even though it was never evaluated.
    const { store, sink, seed, mempool, reparse } = reparserSetup()
    store.watches.add(ADDR)
    seed(mkTx('tx1'))
    mempool.push('tx1')

    sink.deliverResult = false
    await reparse()
    expect(sink.attempts.filter((e) => e.event === 'seen')).toHaveLength(1)
    expect(store.evaluated.has('tx1')).toBe(false)
    expect(store.pending.has('tx1')).toBe(false)

    sink.deliverResult = true
    await reparse() // same mempool — nothing changed on the node

    expect(sink.attempts.filter((e) => e.event === 'seen')).toHaveLength(2)
    expect(sink.delivered.filter((e) => e.event === 'seen')).toHaveLength(1)
    expect(store.evaluated.has('tx1')).toBe(true)
    expect(store.pending.has('tx1')).toBe(true)
  })

  it('a txid that vanishes between snapshot and fetch is skipped, not fatal', async () => {
    const { store, sink, seed, mempool, reparse } = reparserSetup()
    store.watches.add(ADDR)
    seed(mkTx('kept'))
    mempool.push('kept', 'vanished') // 'vanished' has no rpc entry → verbose null

    await reparse()

    const seen = sink.delivered.filter((e): e is TxEvent => e.event === 'seen')
    expect(seen.map((e) => e.txid)).toEqual(['kept'])
  })

  it('a txid mined between snapshot and fetch (verbose.blockhash set) is skipped — no false seen', async () => {
    const { store, sink, seed, mempool, reparse } = reparserSetup()
    store.watches.add(ADDR)
    seed(mkTx('mined'), 'b101') // in the snapshot, but getrawtransaction now reports a block
    seed(mkTx('still'))
    mempool.push('mined', 'still')

    await reparse()

    const seen = sink.attempts.filter((e): e is TxEvent => e.event === 'seen')
    expect(seen.map((e) => e.txid)).toEqual(['still'])
    // the block pipeline owns mined txs: nothing recorded for it here
    expect(store.evaluated.has('mined')).toBe(false)
    expect(store.pending.has('mined')).toBe(false)
    expect(store.records.has('mined')).toBe(false)
  })

  it('mutex: overlapping invocations are skipped, not queued', async () => {
    const store = new FakeStore()
    const sink = new FakeSink()
    const cfg = { network: 'regtest' as const, seenEnabled: true }
    let mempoolCalls = 0
    let release!: () => void
    const gate = new Promise<void>((r) => {
      release = r
    })
    const reparse = makeMempoolReparser({
      rpc: {
        getRawMempool: async () => {
          mempoolCalls++
          await gate // hold the first run open
          return []
        },
        getRawTransactionVerbose: async () => null,
      },
      store,
      cfg,
      decodeRawTx: () => {
        throw new Error('unreachable')
      },
      evaluate: makeTxEvaluator({ store, sink, cfg }),
    })

    const first = reparse()
    const second = reparse() // must be skipped while the first holds the mutex
    release()
    await Promise.all([first, second])

    expect(mempoolCalls).toBe(1)

    // and after release, a fresh call runs again
    await reparse()
    expect(mempoolCalls).toBe(2)
  })
})
