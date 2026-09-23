import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { makeBlockProcessor } from '../src/engine/blockPipeline'
import { makeMempoolReparser } from '../src/engine/mempool'
import { makeTxEvaluator } from '../src/engine/txPipeline'
import type { DecodedTx, Outpoint, TxEvent, WeirEvent } from '../src/lib/types'
import { ADDR, FakeChain, FakeStore, mkTx } from './fakes'

/**
 * Outpoint tracking (DESIGN "Outpoint tracking", rules 1-7): every prevout has a SET of
 * claimant txids; a claim is SADD, a release is SREM of one's own txid. Replacements are
 * detected the moment the replacing tx is decoded (mempool or block), proven conflicts come
 * from the new chain's inputs, and EVERY claimant of a spent prevout is adjudicated. The tx
 * pipeline, the reparser and the block pipeline run over ONE shared FakeStore/FakeChain, as
 * in lifecycle.test.ts.
 */
function wire(cfgOverrides: Partial<{ confirmMilestones: number[]; maxMilestone: number; seenEnabled: boolean }> = {}) {
  const store = new FakeStore()
  const chain = new FakeChain()
  const cfg = { network: 'regtest' as const, seenEnabled: true, confirmMilestones: [1, 3], maxMilestone: 3, ringSize: 12, ...cfgOverrides }
  const byHex = new Map<string, DecodedTx>()
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
  store.watches.add(ADDR)
  chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
  store.tip = { hash: 'b100', height: 100 }
  store.ring.set('b100', 100)
  return { store, chain, rpc, register, evaluate, evaluateRaw, reparse, process }
}

/** prevout `prevN:0` — two txs given the same one conflict */
const O = (n: number, vout = 0): Outpoint => ({ txid: `prev${n}`, vout })
const names = (events: WeirEvent[]): string[] => events.map((e) => e.event)
const enqueued = (store: FakeStore): string[] => names(store.outboxEvents())
const txEvents = (store: FakeStore): TxEvent[] => store.outboxEvents().filter((e): e is TxEvent => e.event !== 'expired' && e.event !== 'heartbeat')
const keysOf = (store: FakeStore): string[] => txEvents(store).map((e) => e.idempotencyKey)
/** every claimant SET, members sorted — `{}` when no prevout is claimed */
const claims = (store: FakeStore): Record<string, string[]> =>
  Object.fromEntries([...store.outpoints].map(([field, set]) => [field, [...set].sort()]))

describe('outpoint tracking', () => {
  let warnLog: ReturnType<typeof vi.spyOn>
  let errorLog: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    warnLog = vi.spyOn(console, 'warn').mockImplementation(() => {})
    errorLog = vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  /** A seen + mined at b101 claiming prev1 (maturing, 1 conf) */
  async function maturingClaimant() {
    const w = wire()
    const a = mkTx('A', ADDR, 5000, [O(1)])
    await w.evaluate(a)
    w.chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
    w.chain.mempool = []
    await w.process(w.chain.raw('b101'))
    expect(enqueued(w.store)).toEqual(['seen', 'confirmed'])
    expect(claims(w.store)).toEqual({ 'prev1:0': ['A'] })
    return w
  }

  describe('mempool path (rule 3)', () => {
    it('RBF fee-bump: A seen → B (same input, pays the watch) seen → A is dropped IMMEDIATELY with reason replaced / replacedBy B, then B is seen', async () => {
      const { store, evaluate } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1), O(2)])
      const b = mkTx('B', ADDR, 4800, [O(1), O(2)]) // higher fee, same inputs, same payee

      await evaluate(a)
      expect(enqueued(store)).toEqual(['seen'])
      expect(claims(store)).toEqual({ 'prev1:0': ['A'], 'prev2:0': ['A'] })

      await evaluate(b) // no block needed

      expect(enqueued(store)).toEqual(['seen', 'dropped', 'seen'])
      const [, dropped, seenB] = txEvents(store) as [TxEvent, TxEvent, TxEvent]
      expect(dropped).toMatchObject({
        event: 'dropped',
        txid: 'A',
        confs: 0,
        matched: [{ address: ADDR, vout: 0, valueSats: 5000 }], // from A's seen-time record
        hex: 'hex-A',
        blockHeight: null,
        blockHash: null,
        reason: 'replaced',
        replacedBy: 'B',
        idempotencyKey: 'regtest:A:dropped:replaced:B',
      })
      expect(dropped.conflictingTxid).toBeUndefined()
      expect(seenB).toMatchObject({ event: 'seen', txid: 'B', idempotencyKey: 'regtest:B:seen' })
      // A is forgotten (un-evaluated: it may legitimately return if B is dropped); B holds the claims
      expect(store.pending.has('A')).toBe(false)
      expect(store.evaluated.has('A')).toBe(false)
      expect(store.records.has('A')).toBe(false)
      expect(store.pending.has('B')).toBe(true)
      expect(claims(store)).toEqual({ 'prev1:0': ['B'], 'prev2:0': ['B'] })
    })

    it("redirect: B spends A's input but pays nobody watched → A dropped/replaced, B evaluated with NO seen", async () => {
      const { store, evaluate } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]))
      const b = mkTx('B', 'bcrt1qsomeoneelse', 5000, [O(1)])

      await evaluate(b)

      expect(enqueued(store)).toEqual(['seen', 'dropped'])
      expect(txEvents(store)[1]).toMatchObject({ txid: 'A', reason: 'replaced', replacedBy: 'B', idempotencyKey: 'regtest:A:dropped:replaced:B' })
      expect(store.evaluated.has('B')).toBe(true)
      expect(store.pending.has('B')).toBe(false)
      expect(store.records.has('B')).toBe(false)
      expect(claims(store)).toEqual({}) // A's claim released; B tracks nothing
    })

    it('one replacement per claimant even when several of its inputs are spent; two claimants → two dropped', async () => {
      const { store, evaluate } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1), O(2)]))
      await evaluate(mkTx('C', ADDR, 5000, [O(3)]))

      await evaluate(mkTx('B', null, 5000, [O(2), O(1), O(3)]))

      const dropped = txEvents(store).filter((e) => e.event === 'dropped')
      expect(dropped.map((e) => e.txid).sort()).toEqual(['A', 'C'])
      expect(dropped.every((e) => e.reason === 'replaced' && e.replacedBy === 'B')).toBe(true)
      expect(store.pending.size).toBe(0)
      expect(claims(store)).toEqual({})
    })

    it('the reparser path replaces too (a fee-bump that arrived during a ZMQ gap)', async () => {
      const { store, chain, register, reparse } = wire()
      register(mkTx('A', ADDR, 5000, [O(1)]))
      register(mkTx('B', ADDR, 4900, [O(1)]))
      chain.mempool = ['A']
      await reparse()
      expect(enqueued(store)).toEqual(['seen'])

      chain.mempool = ['B'] // the node replaced A with B while weir missed the rawtx
      await reparse()

      expect(enqueued(store)).toEqual(['seen', 'dropped', 'seen'])
      expect(txEvents(store)[1]).toMatchObject({ txid: 'A', reason: 'replaced', replacedBy: 'B' })
      expect(store.pending.has('B')).toBe(true)
    })

    it('REGRESSION (historical packet after a gap reparse): a rawtx packet for a tx the node no longer holds writes NOTHING — the reconciled newer outcome stands', async () => {
      // Chain A → B → C of replacements. onTxGap queues the reparse ahead of the packets that
      // triggered it: the reparse evaluates C (the node's live mempool). The queued packet for
      // B would then "replace" C by B, and C's own later packet would re-fire seen:C.
      const { store, chain, register, evaluateRaw, reparse } = wire()
      register(mkTx('A', ADDR, 5000, [O(1)]))
      const b = register(mkTx('B', ADDR, 4900, [O(1)]))
      const c = register(mkTx('C', ADDR, 4800, [O(1)]))
      chain.mempool = ['C'] // A and B are gone from the node: C replaced them
      await reparse()
      expect(keysOf(store)).toEqual(['regtest:C:seen'])

      await evaluateRaw(b) // the historical packet: B is not in the node's mempool now
      await evaluateRaw(c) // C's packet: already evaluated

      expect(keysOf(store)).toEqual(['regtest:C:seen']) // no dropped:C, no second seen
      expect(store.pending.has('C')).toBe(true)
      expect(store.pending.has('B')).toBe(false)
      expect(store.evaluated.has('B')).toBe(false) // nothing written for B at all
      expect(claims(store)).toEqual({ 'prev1:0': ['C'] })
    })

    it('a non-matching, non-replacing tx never probes the node (marked evaluated without getmempoolentry)', async () => {
      const { store, rpc, evaluateRaw } = wire()
      const probe = vi.spyOn(rpc, 'getMempoolEntry')

      await evaluateRaw(mkTx('U', null, 1, [O(9)]))

      expect(probe).not.toHaveBeenCalled()
      expect(store.evaluated.has('U')).toBe(true)
    })

    it('a MATURING (mined) claimant is never touched by a mempool tx: warn, nothing emitted, claims untouched', async () => {
      const { store, evaluate } = await maturingClaimant()

      await evaluate(mkTx('E', null, 5000, [O(1)])) // bitcoind would not relay this

      expect(enqueued(store)).toEqual(['seen', 'confirmed'])
      expect(store.maturingIndex.get('A')).toBe(101)
      expect(store.records.get('A')).toMatchObject({ height: 101, blockHash: 'b101' })
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
      expect(store.evaluated.has('E')).toBe(true)
      expect(warnLog.mock.calls.some((c) => /E spends an input of mined tx A/.test(String(c[0])))).toBe(true)
    })

    it('a WATCHED replacement B against a MATURING claimant A: B gets its own seen and claims alongside A; A untouched; a warn is logged', async () => {
      const { store, evaluate } = await maturingClaimant()

      await evaluate(mkTx('B', ADDR, 4900, [O(1)]))

      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'seen'])
      expect(txEvents(store)[2]).toMatchObject({ txid: 'B', idempotencyKey: 'regtest:B:seen' })
      expect(store.pending.has('B')).toBe(true)
      expect(store.records.get('B')).toMatchObject({ height: 0, inputs: [O(1)] })
      expect(store.maturingIndex.get('A')).toBe(101)
      expect(claims(store)).toEqual({ 'prev1:0': ['A', 'B'] }) // claims never conflict
      expect(warnLog.mock.calls.some((c) => /B spends an input of mined tx A/.test(String(c[0])))).toBe(true)
      expect(txEvents(store).filter((e) => e.event === 'dropped')).toHaveLength(0)
    })

    it('a tx that spends nothing weir tracks is evaluated as before; a coinbase-like tx (no inputs) never queries claimants', async () => {
      const { store, evaluate } = wire()
      const owners = vi.spyOn(store, 'outpointOwners')
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]))
      await evaluate(mkTx('X', ADDR, 5000, [O(9)])) // unrelated input
      expect(owners).toHaveBeenCalledTimes(2)

      await evaluate(mkTx('CB', ADDR, 5000, []))

      expect(owners).toHaveBeenCalledTimes(2)
      expect(enqueued(store)).toEqual(['seen', 'seen', 'seen'])
      expect(store.pending.size).toBe(3)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'], 'prev9:0': ['X'] })
    })

    it('a replaced tx can come back: if the replacement is dropped, a rebroadcast of the original fires a fresh seen', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      await evaluate(mkTx('B', null, 5000, [O(1)]))
      expect(enqueued(store)).toEqual(['seen', 'dropped'])

      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
      chain.mempool = ['A'] // B evicted, A rebroadcast
      await process(chain.raw('b101'))
      await evaluate(a)

      expect(enqueued(store)).toEqual(['seen', 'dropped', 'seen'])
      expect(store.pending.has('A')).toBe(true)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
    })

    it('dropped, then mined without a mempool sighting → confirmed through the block path (drops are not terminal)', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
      chain.mempool = []
      await process(chain.raw('b101'))
      expect(enqueued(store)).toEqual(['seen', 'dropped'])

      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [a] }) // mined from a rebroadcast weir never saw
      await process(chain.raw('b102'))
      expect(enqueued(store)).toEqual(['seen', 'dropped', 'confirmed'])
      expect(store.maturingIndex.get('A')).toBe(102)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
    })
  })

  describe('block path (rule 4)', () => {
    it('a confirmed double-spend of a pending tx: dropped with reason replaced / replacedBy = the mined spender; the eviction check does not emit a second dropped', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1), O(2)]))
      const c = mkTx('C', 'bcrt1qsomeoneelse', 5000, [O(2)]) // never seen in the mempool by weir
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [c] })
      chain.mempool = [] // A is gone from the node's mempool too — without the input scan this would say "evicted"

      await process(chain.raw('b101'))

      expect(enqueued(store)).toEqual(['seen', 'dropped'])
      expect(txEvents(store)[1]).toMatchObject({
        txid: 'A',
        confs: 0,
        matched: [{ address: ADDR, vout: 0, valueSats: 5000 }],
        hex: 'hex-A',
        reason: 'replaced',
        replacedBy: 'C',
        idempotencyKey: 'regtest:A:dropped:replaced:C',
      })
      expect(store.pending.has('A')).toBe(false)
      expect(store.evaluated.has('A')).toBe(false)
      expect(store.records.has('A')).toBe(false)
      expect(claims(store)).toEqual({})
    })

    it('a block spender that PAYS the watch replaces the pending claimant and is promoted itself', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]))
      const b = mkTx('B', ADDR, 4900, [O(1)])
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [b] })
      chain.mempool = []

      await process(chain.raw('b101'))

      expect(enqueued(store).sort()).toEqual(['confirmed', 'dropped', 'seen'])
      expect(txEvents(store).find((e) => e.event === 'dropped')).toMatchObject({ txid: 'A', reason: 'replaced', replacedBy: 'B' })
      expect(txEvents(store).find((e) => e.event === 'confirmed')).toMatchObject({ txid: 'B', confs: 1, idempotencyKey: 'regtest:B:confirmed:1:b101' })
      expect(store.maturingIndex.get('B')).toBe(101)
      expect(claims(store)).toEqual({ 'prev1:0': ['B'] })
    })

    it('the pending tx itself being mined is not a replacement (claimant == spender): promoted, no dropped', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []

      await process(chain.raw('b101'))

      expect(enqueued(store)).toEqual(['seen', 'confirmed'])
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
    })

    it("REORG: the new chain spends a limbo tx's input → PROVEN conflicted (reason double-spend, conflictingTxid, block timestamp); resolveLimbo emits NO second verdict", async () => {
      const { store, chain, rpc, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []
      await process(chain.raw('b101a'))
      expect(enqueued(store)).toEqual(['seen', 'confirmed'])
      expect(store.maturingIndex.get('A')).toBe(101)

      // b101a is reorged out; b101b confirms D, which spends A's input
      const d = mkTx('D', 'bcrt1qsomeoneelse', 5000, [O(1)])
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [d] })
      const probe = vi.spyOn(rpc, 'getMempoolEntry')
      chain.mempoolEntries.set('A', { time: 1 }) // even if the node claimed A were in its mempool, the proof wins

      await process(chain.raw('b101b'))

      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'conflicted'])
      expect(txEvents(store)[2]).toMatchObject({
        event: 'conflicted',
        txid: 'A',
        confs: 1, // last confirmed depth
        blockHeight: 101,
        blockHash: 'b101a', // the OLD block
        hex: 'hex-A',
        reason: 'double-spend',
        conflictingTxid: 'D',
        idempotencyKey: 'regtest:A:conflicted', // one terminal verdict per txid — the key does not change
        timestamp: 1_700_000_111 * 1000,
      })
      expect(txEvents(store)[2]!.replacedBy).toBeUndefined()
      expect(probe).not.toHaveBeenCalled() // A left limbo inside the block's MULTI; resolveLimbo had nothing to adjudicate
      expect(store.limbo.size).toBe(0)
      expect(store.maturingIndex.has('A')).toBe(false)
      expect(store.records.has('A')).toBe(false)
      expect(store.pending.has('A')).toBe(false)
      expect(claims(store)).toEqual({})
      expect(store.tip).toEqual({ hash: 'b101b', height: 101 })
    })

    it('REORG: a proven conflict during the catch-up walk (non-tip block) is adjudicated there, and a later block confirms nothing for it', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []
      await process(chain.raw('b101a'))

      const d = mkTx('D', null, 5000, [O(1)])
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [d] })
      chain.addBlock({ hash: 'b102b', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [] })

      await process(chain.raw('b102b')) // b101b is walked as a non-tip block

      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'conflicted'])
      expect(txEvents(store)[2]).toMatchObject({ txid: 'A', reason: 'double-spend', conflictingTxid: 'D', timestamp: 1_700_000_111 * 1000 })
      expect(store.limbo.size).toBe(0)
      expect(store.maturingIndex.size).toBe(0)
    })

    it('REORG: re-inclusion of a limbo tx (claimant == spender) is not a conflict — promoted, milestones re-fire', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []
      await process(chain.raw('b101a'))
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [a] })

      await process(chain.raw('b101b'))

      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'confirmed'])
      expect(keysOf(store)).toEqual(['regtest:A:seen', 'regtest:A:confirmed:1:b101a', 'regtest:A:confirmed:1:b101b'])
      expect(store.limbo.size).toBe(0)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
    })

    it('REORG fallback unchanged: a limbo tx whose inputs the new chain did not spend is still adjudicated by resolveLimbo (demoted / conflicted by elimination)', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []
      await process(chain.raw('b101a'))
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [mkTx('U', null, 1, [O(7)])] })
      // no mempool entry → conflicted by elimination: no reason / conflictingTxid (not proven)

      await process(chain.raw('b101b'))

      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'conflicted'])
      const conflicted = txEvents(store)[2]!
      expect(conflicted.idempotencyKey).toBe('regtest:A:conflicted')
      expect(conflicted.reason).toBeUndefined()
      expect(conflicted.conflictingTxid).toBeUndefined()
      expect(claims(store)).toEqual({}) // conflict released them
      expect(store.records.has('A')).toBe(false)
    })

    it('a block spender hitting a MATURING claimant outside limbo is impossible on a valid chain: error log, skipped, nothing emitted', async () => {
      const { store, chain, process } = await maturingClaimant()
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [mkTx('D', null, 5000, [O(1)])] })

      await process(chain.raw('b102'))

      expect(enqueued(store)).toEqual(['seen', 'confirmed'])
      expect(store.maturingIndex.get('A')).toBe(101)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
      expect(errorLog.mock.calls.some((c) => /D in b102 spends an input of A, which is maturing/.test(String(c[0])))).toBe(true)
    })

    it('coinbase inputs are ignored: a coinbase paying the watch is promoted and claims nothing', async () => {
      const { store, chain, process } = wire()
      const cb = mkTx('CB', ADDR, 5000, []) // decodeRawTx omits the coinbase prevout
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [cb] })
      const owners = vi.spyOn(store, 'outpointOwners')

      await process(chain.raw('b101'))

      expect(owners).not.toHaveBeenCalled() // nothing to look up
      expect(enqueued(store)).toEqual(['confirmed'])
      expect(store.maturingIndex.get('CB')).toBe(101)
      expect(claims(store)).toEqual({})
    })
  })

  describe('claims follow the record (rule 2)', () => {
    it('claimed at seen, kept through promotion, released at the final milestone', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1), O(2, 5)])
      await evaluate(a)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'], 'prev2:5': ['A'] })
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
      chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
      chain.mempool = []

      await process(chain.raw('b101'))
      await process(chain.raw('b102'))
      expect(claims(store)).toEqual({ 'prev1:0': ['A'], 'prev2:5': ['A'] })
      expect(store.records.get('A')).toMatchObject({ inputs: [O(1), O(2, 5)] })

      await process(chain.raw('b103')) // 3 confs → tracking ends
      expect(store.maturingIndex.has('A')).toBe(false)
      expect(claims(store)).toEqual({})
    })

    it('a never-seen mined tx claims through promotion', async () => {
      const { store, chain, process } = wire()
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [mkTx('N', ADDR, 5000, [O(4)])] })
      await process(chain.raw('b101'))
      expect(claims(store)).toEqual({ 'prev4:0': ['N'] })
    })

    it('released by the tip-block eviction check (reason evicted)', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]))
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
      chain.mempool = []

      await process(chain.raw('b101'))

      expect(txEvents(store)[1]).toMatchObject({ event: 'dropped', txid: 'A', reason: 'evicted', idempotencyKey: 'regtest:A:dropped:101' })
      expect(txEvents(store)[1]!.replacedBy).toBeUndefined()
      expect(claims(store)).toEqual({})
    })

    it('kept through demotion (record kept), so a later double-spend of the demoted tx is still detected', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []
      await process(chain.raw('b101a'))
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
      chain.mempoolEntries.set('A', { time: 1 })
      chain.mempool = ['A']
      await process(chain.raw('b101b'))
      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'demoted'])
      expect(store.pending.has('A')).toBe(true)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })

      await evaluate(mkTx('B', ADDR, 4900, [O(1)])) // fee-bump of the demoted tx

      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'demoted', 'dropped', 'seen'])
      expect(txEvents(store)[3]).toMatchObject({ txid: 'A', reason: 'replaced', replacedBy: 'B', idempotencyKey: 'regtest:A:dropped:replaced:B' })
      expect(claims(store)).toEqual({ 'prev1:0': ['B'] })
    })

    it('released when tracking ends quietly (watch removed mid-flight)', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a)
      store.watches.delete(ADDR)
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []

      await process(chain.raw('b101'))

      expect(enqueued(store)).toEqual(['seen'])
      expect(store.records.has('A')).toBe(false)
      expect(claims(store)).toEqual({})
    })

    it('wiped by the prune-window reset', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]))
      for (let h = 101; h <= 104; h++) chain.addBlock({ hash: `b${h}`, prevHash: `b${h - 1}`, height: h, time: 1_700_000_000 + h, txs: [] })
      chain.pruned = true
      chain.pruneheight = 103

      await process(chain.raw('b104'))

      expect(store.pending.size).toBe(0)
      expect(claims(store)).toEqual({})
    })
  })
})
