import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { makeRawTxHandler, makeTxEvaluator } from '../src/engine/txPipeline'
import { makeMempoolReparser } from '../src/engine/mempool'
import { TOMBSTONE_TTL_MS } from '../src/engine/blockPipeline'
import { MAX_EVALUATION_AGE_MS } from '../src/store/redis'
import type { DecodedTx, TxEvent } from '../src/lib/types'
import { ADDR, FakeStore, mkTx } from './fakes'

function setup(overrides: { seenEnabled?: boolean } = {}) {
  const store = new FakeStore()
  const cfg = { network: 'regtest' as const, seenEnabled: overrides.seenEnabled ?? true }
  const evaluate = makeTxEvaluator({ store, cfg })
  return { store, cfg, evaluate }
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

    await evaluate(tx, Date.now())

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

  it('recordSeen guard: a tx mined (promoted) while its evaluation was in flight is NOT overwritten with a height-0 record', async () => {
    // The evaluator read `isEvaluated` (false: the tip prune forgot the txid when it left the
    // mempool) before the block pipeline promoted the tx. Its write must be a no-op.
    const { store, evaluate } = setup()
    store.watches.add(ADDR)
    const tx = mkTx('tx1')
    const isEvaluated = store.isEvaluated.bind(store)
    store.isEvaluated = async (txid) => {
      const was = await isEvaluated(txid)
      // the block lands between the evaluator's read and its write
      await store.promoteToMaturing({ txid: 'tx1', height: 101, blockHash: 'b101', matched: [{ address: ADDR, vout: 0, valueSats: 5000 }], fired: [1], hex: tx.hex, inputs: [] })
      await store.pruneEvaluated()
      return was
    }

    await evaluate(tx, Date.now())

    expect(store.records.get('tx1')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.outboxEvents()).toHaveLength(0) // no seen for a tx that is already confirmed
  })

  describe('evaluation fence', () => {
    it('MAX_EVALUATION_AGE_MS stays far below TOMBSTONE_TTL_MS (the fence must outlast no tombstone)', () => {
      expect(MAX_EVALUATION_AGE_MS).toBe(600_000)
      expect(MAX_EVALUATION_AGE_MS * 6).toBeLessThanOrEqual(TOMBSTONE_TTL_MS)
    })

    it('an evaluation just under the fence records; one just over is refused and leaves the txid un-evaluated', async () => {
      // The clock is pinned: the boundary is exact, and a millisecond tick between capturing
      // `now` and the fence check must not turn the "under" case into a refusal.
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      try {
        const { store, evaluate } = setup()
        store.watches.add(ADDR)
        const now = Date.now()

        await evaluate(mkTx('over'), now - MAX_EVALUATION_AGE_MS - 1)
        expect(store.evaluated.has('over')).toBe(false) // the next reparse redoes it
        expect(store.pending.has('over')).toBe(false)
        expect(store.records.has('over')).toBe(false)
        expect(store.outboxEvents()).toHaveLength(0)

        await evaluate(mkTx('under'), now - MAX_EVALUATION_AGE_MS)
        expect(store.evaluated.has('under')).toBe(true)
        expect(store.pending.has('under')).toBe(true)
        expect(seenEvents(store).map((e) => e.txid)).toEqual(['under'])

        await evaluate(mkTx('over'), Date.now()) // a fresh evaluation of the refused txid succeeds
        expect(seenEvents(store).map((e) => e.txid)).toEqual(['under', 'over'])
      } finally {
        vi.useRealTimers()
      }
    })

    it('makeRawTxHandler stamps startedAtMs at receipt, before decoding', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      try {
        const { store, cfg } = setup()
        store.watches.add(ADDR)
        const recordSeen = vi.spyOn(store, 'recordSeen')
        const handler = makeRawTxHandler({
          store,
          cfg,
          decodeRawTx: () => {
            vi.setSystemTime(1_700_000_000_000 + 4000) // decoding takes "4s"
            return mkTx('tx1')
          },
        })
        await handler(Buffer.from('00', 'hex'))
        expect(recordSeen).toHaveBeenCalledTimes(1)
        expect(recordSeen.mock.calls[0]![2]).toBe(1_700_000_000_000)
      } finally {
        vi.useRealTimers()
      }
    })
  })

  it('recordSeen guard: two concurrent evaluations of the same tx (ZMQ + reparse) enqueue exactly one seen', async () => {
    const { store, evaluate } = setup()
    store.watches.add(ADDR)

    await Promise.all([evaluate(mkTx('tx1'), Date.now()), evaluate(mkTx('tx1'), Date.now())])

    expect(seenEvents(store).map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:seen'])
    expect(store.pending.has('tx1')).toBe(true)
  })

  it('seen disabled (no 0 milestone): tracks silently — evaluated + pending + record, nothing enqueued', async () => {
    const { store, evaluate } = setup({ seenEnabled: false })
    store.watches.add(ADDR)

    await evaluate(mkTx('tx1'), Date.now())

    expect(store.outboxEvents()).toHaveLength(0)
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.evaluated.has('tx1')).toBe(true)
    expect(store.records.has('tx1')).toBe(true)
  })

  it('already-evaluated txs are skipped entirely', async () => {
    const { store, evaluate } = setup()
    store.watches.add(ADDR)
    store.evaluated.add('tx1')

    await evaluate(mkTx('tx1'), Date.now())

    expect(store.outboxEvents()).toHaveLength(0)
    expect(store.pending.has('tx1')).toBe(false)
  })

  it('no matched outputs → markEvaluated only', async () => {
    const { store, evaluate } = setup()
    // nothing watched

    await evaluate(mkTx('tx1'), Date.now())

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
  function reparserSetup() {
    const store = new FakeStore()
    const cfg = { network: 'regtest' as const, seenEnabled: true }
    const evaluate = makeTxEvaluator({ store, cfg })
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
    return { store, seed, mempool, reparse }
  }

  it('evaluates only txids not already evaluated (current − evaluated), then drops the snapshot', async () => {
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
    expect(store.mempoolCurrent.size).toBe(0)
  })

  it('the fence clock starts BEFORE getrawtransaction is issued (a slow RPC counts against the evaluation)', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_700_000_000_000)
    try {
      const store = new FakeStore()
      const cfg = { network: 'regtest' as const, seenEnabled: true }
      const startedAts: number[] = []
      const tx = mkTx('tx1')
      const reparse = makeMempoolReparser({
        rpc: {
          getRawMempool: async () => ['tx1'],
          getRawTransactionVerbose: async () => {
            vi.setSystemTime(1_700_000_000_000 + 30_000) // the node took 30s to answer
            return { hex: tx.hex }
          },
        },
        store,
        cfg,
        decodeRawTx: () => tx,
        evaluate: async (_tx, startedAtMs) => {
          startedAts.push(startedAtMs)
        },
      })
      await reparse()
      expect(startedAts).toEqual([1_700_000_000_000])
    } finally {
      vi.useRealTimers()
    }
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

  it('mutex: overlapping invocations are skipped, not queued', async () => {
    const store = new FakeStore()
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
      evaluate: makeTxEvaluator({ store, cfg }),
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
