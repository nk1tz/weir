import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { makeBlockProcessor } from '../src/engine/blockPipeline'
import { makeMempoolReparser } from '../src/engine/mempool'
import { makeTxEvaluator } from '../src/engine/txPipeline'
import type { DecodedTx, TxEvent } from '../src/lib/types'
import { ADDR, FakeChain, FakeSink, FakeStore, mkTx } from './fakes'

/**
 * Whole-lifecycle tests: the tx pipeline, the mempool reparser and the block pipeline wired
 * together over ONE shared FakeStore/FakeSink/FakeChain, so state handed from one module
 * to the next (evaluated, pending, records, limbo) is exercised the way the daemon uses it.
 */
function wire() {
  const store = new FakeStore()
  const chain = new FakeChain()
  const sink = new FakeSink()
  const cfg = { network: 'regtest' as const, seenEnabled: true, confirmMilestones: [1, 3], maxMilestone: 3, ringSize: 12 }
  const byHex = new Map<string, DecodedTx>()
  /** make the node know a tx (getrawtransaction answers with its hex) */
  const register = (tx: DecodedTx): DecodedTx => {
    byHex.set(tx.hex, tx)
    chain.rawTxs.set(tx.txid, { hex: tx.hex })
    return tx
  }
  const rpc = chain.rpc()
  const evaluate = makeTxEvaluator({ store, sink, cfg })
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
  const process = makeBlockProcessor({ cfg, store, rpc, sink, decodeBlock: chain.decode })
  return { store, chain, sink, register, reparse, process }
}

const delivered = (sink: FakeSink): string[] => sink.delivered.map((e) => e.event)
const seenAttempts = (sink: FakeSink): TxEvent[] => sink.attempts.filter((e): e is TxEvent => e.event === 'seen')

describe('lifecycle', () => {
  beforeAll(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
  })
  afterAll(() => {
    vi.restoreAllMocks()
  })

  it('seen → confirmed → reorg demotion → reparse emits exactly seen, confirmed, demoted — NO second seen', async () => {
    // Regression: after mining, the tip prune forgets the txid from `evaluated` (it left the
    // mempool). Demotion put it back in the mempool + pending WITHOUT re-marking it evaluated,
    // so the next reparse re-evaluated it and emitted a duplicate `seen` (same idempotency
    // key) while overwriting its record. Demotion must be one atomic step that also SADDs
    // `evaluated`.
    const { store, chain, sink, register, reparse, process } = wire()
    store.watches.add(ADDR)
    chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
    store.tip = { hash: 'b100', height: 100 }
    store.ring.set('b100', 100)
    const tx1 = register(mkTx('tx1'))

    // 1. tx1 enters the mempool; a reparse delivers `seen`
    chain.mempool = ['tx1']
    await reparse()
    expect(delivered(sink)).toEqual(['seen'])
    expect(store.pending.has('tx1')).toBe(true)

    // 2. mined in b101a and gone from the mempool → confirmed:1; the tip prune drops it from `evaluated`
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [tx1] }, { main: false })
    chain.mempool = []
    await process(chain.raw('b101a'))
    expect(delivered(sink)).toEqual(['seen', 'confirmed'])
    expect(store.evaluated.has('tx1')).toBe(false)

    // 3. b101a is reorged out by b101b (tx1 not re-included); the node returns tx1 to its mempool → demoted
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    chain.mempoolEntries.set('tx1', { time: 1 })
    chain.mempool = ['tx1']
    await process(chain.raw('b101b'))
    expect(delivered(sink)).toEqual(['seen', 'confirmed', 'demoted'])
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.limbo.size).toBe(0)
    const recordAfterDemotion = structuredClone(store.records.get('tx1'))
    expect(recordAfterDemotion).toMatchObject({ height: 0, blockHash: '', fired: [] })

    // 4. the next reparse (gap-triggered, or at boot) sees tx1 in the mempool again — already tracked
    await reparse()
    expect(delivered(sink)).toEqual(['seen', 'confirmed', 'demoted'])
    expect(seenAttempts(sink)).toHaveLength(1)
    expect(store.records.get('tx1')).toEqual(recordAfterDemotion)
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.evaluated.has('tx1')).toBe(true)

    // 5. re-mined in b102 → a fresh confirmed:1 under the new block hash, still no seen
    chain.addBlock({ hash: 'b102', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [tx1] })
    chain.mempool = []
    await process(chain.raw('b102'))
    expect(delivered(sink)).toEqual(['seen', 'confirmed', 'demoted', 'confirmed'])
    const confirmed = sink.delivered.filter((e): e is TxEvent => e.event === 'confirmed')
    expect(confirmed.map((e) => e.idempotencyKey)).toEqual(['regtest:tx1:confirmed:1:b101a', 'regtest:tx1:confirmed:1:b102'])
    expect(store.pending.has('tx1')).toBe(false)
    expect(store.maturingIndex.get('tx1')).toBe(102)
  })
})
