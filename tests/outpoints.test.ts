import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { makeBlockProcessor } from '../src/engine/blockPipeline'
import { MEMPOOL_REPARSE_INTERVAL_MS, makeMempoolReparser } from '../src/engine/mempool'
import { makeTxEvaluator } from '../src/engine/txPipeline'
import type { DecodedTx, Outpoint, TxEvent, WeirEvent } from '../src/lib/types'
import { MAX_EVALUATION_AGE_MS } from '../src/store/redis'
import { TOMBSTONE_TTL_MS } from '../src/engine/blockPipeline'
import { ADDR, FakeChain, FakeStore, mkTx } from './fakes'

/**
 * Outpoint tracking (DESIGN "Outpoint tracking", rules 1-7): every prevout has a SET of
 * claimant txids; a claim is SADD (never conflicts), a release is SREM of one's own txid.
 * Replacements are detected the moment the replacing tx is decoded (mempool or block),
 * proven conflicts come from the new chain's inputs, and EVERY claimant of a spent prevout
 * is adjudicated. The tx pipeline, the reparser and the block pipeline run over ONE shared
 * FakeStore/FakeChain, as in lifecycle.test.ts.
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
  store.watches.add(ADDR)
  chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
  store.tip = { hash: 'b100', height: 100 }
  store.ring.set('b100', 100)
  return { store, chain, rpc, register, evaluate, reparse, process }
}

/**
 * Park the FIRST call of `store[method]` whose args satisfy `when` AFTER it has read its
 * result (the victim now holds a snapshot it will act on) until `release()`; the caller
 * awaits `parked` to know the read is done. Later calls pass through.
 */
function parkOnce<M extends 'readRecord' | 'outpointOwners'>(
  store: FakeStore,
  method: M,
  when: (...args: Parameters<FakeStore[M]>) => boolean,
): { parked: Promise<void>; release: () => void } {
  const original = (store[method] as (...args: unknown[]) => Promise<unknown>).bind(store)
  let release!: () => void
  const gate = new Promise<void>((r) => {
    release = r
  })
  let parkedResolve!: () => void
  const parked = new Promise<void>((r) => {
    parkedResolve = r
  })
  let armed = true
  ;(store as unknown as Record<M, unknown>)[method] = (async (...args: unknown[]) => {
    const result = await original(...args)
    if (armed && when(...(args as Parameters<FakeStore[M]>))) {
      armed = false
      parkedResolve()
      await gate
    }
    return result
  }) as FakeStore[M]
  return { parked, release }
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
    await w.evaluate(a, Date.now())
    w.chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] }, { main: false })
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

      await evaluate(a, Date.now())
      expect(enqueued(store)).toEqual(['seen'])
      expect(claims(store)).toEqual({ 'prev1:0': ['A'], 'prev2:0': ['A'] })

      await evaluate(b, Date.now()) // no block needed

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
      expect(store.tombstones.has('A')).toBe(false)
      expect(store.pending.has('B')).toBe(true)
      expect(claims(store)).toEqual({ 'prev1:0': ['B'], 'prev2:0': ['B'] })
    })

    it("redirect: B spends A's input but pays nobody watched → A dropped/replaced, B evaluated with NO seen", async () => {
      const { store, evaluate } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      const b = mkTx('B', 'bcrt1qsomeoneelse', 5000, [O(1)])

      await evaluate(b, Date.now())

      expect(enqueued(store)).toEqual(['seen', 'dropped'])
      expect(txEvents(store)[1]).toMatchObject({ txid: 'A', reason: 'replaced', replacedBy: 'B', idempotencyKey: 'regtest:A:dropped:replaced:B' })
      expect(store.evaluated.has('B')).toBe(true)
      expect(store.pending.has('B')).toBe(false)
      expect(store.records.has('B')).toBe(false)
      expect(claims(store)).toEqual({}) // A's claim released; B tracks nothing
    })

    it('one replacement per claimant even when several of its inputs are spent; two claimants → two dropped', async () => {
      const { store, evaluate } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1), O(2)]), Date.now())
      await evaluate(mkTx('C', ADDR, 5000, [O(3)]), Date.now())

      await evaluate(mkTx('B', null, 5000, [O(2), O(1), O(3)]), Date.now())

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

    it('a MATURING (mined) claimant is never touched by a mempool tx: warn, nothing emitted, claims untouched', async () => {
      const { store, evaluate } = await maturingClaimant()

      await evaluate(mkTx('E', null, 5000, [O(1)]), Date.now()) // bitcoind would not relay this

      expect(enqueued(store)).toEqual(['seen', 'confirmed'])
      expect(store.maturingIndex.get('A')).toBe(101)
      expect(store.records.get('A')).toMatchObject({ height: 101, blockHash: 'b101' })
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
      expect(store.evaluated.has('E')).toBe(true)
      expect(warnLog.mock.calls.some((c) => /E spends an input of mined tx A/.test(String(c[0])))).toBe(true)
    })

    it('a WATCHED replacement B against a MATURING claimant A: B gets its own seen and claims alongside A; A untouched; a warn is logged', async () => {
      const { store, evaluate } = await maturingClaimant()

      await evaluate(mkTx('B', ADDR, 4900, [O(1)]), Date.now())

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
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      await evaluate(mkTx('X', ADDR, 5000, [O(9)]), Date.now()) // unrelated input
      expect(owners).toHaveBeenCalledTimes(2)

      await evaluate(mkTx('CB', ADDR, 5000, []), Date.now())

      expect(owners).toHaveBeenCalledTimes(2)
      expect(enqueued(store)).toEqual(['seen', 'seen', 'seen'])
      expect(store.pending.size).toBe(3)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'], 'prev9:0': ['X'] })
    })

    it('a STALE evaluation (older than the fence) replaces nothing and leaves the txid un-evaluated', async () => {
      const { store, evaluate } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())

      await evaluate(mkTx('B', ADDR, 4900, [O(1)]), Date.now() - MAX_EVALUATION_AGE_MS - 1)

      expect(enqueued(store)).toEqual(['seen'])
      expect(store.pending.has('A')).toBe(true)
      expect(store.evaluated.has('B')).toBe(false)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
      expect(warnLog.mock.calls.some((c) => /refused stale evaluation of B/.test(String(c[0])))).toBe(true)
    })

    it('the PERIODIC reparse (rule 7) recovers a fence-refused tx: 300 s, the mutex handles overlap', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, chain, register, evaluate, reparse } = wire()
      const b = register(mkTx('B', ADDR, 4900, [O(1)]))
      await evaluate(b, Date.now() - MAX_EVALUATION_AGE_MS - 1) // refused
      expect(store.evaluated.has('B')).toBe(false)
      chain.mempool = ['B'] // still in the node's mempool, and no ZMQ gap will ever re-deliver it
      // what index.ts wires: one interval calling the reparser
      expect(MEMPOOL_REPARSE_INTERVAL_MS).toBe(300_000)
      const timer = setInterval(() => void reparse(), MEMPOOL_REPARSE_INTERVAL_MS)
      try {
        await vi.advanceTimersByTimeAsync(MEMPOOL_REPARSE_INTERVAL_MS - 1)
        expect(enqueued(store)).toEqual([])
        await vi.advanceTimersByTimeAsync(1)
        expect(enqueued(store)).toEqual(['seen'])
        expect(store.pending.has('B')).toBe(true)
        expect(claims(store)).toEqual({ 'prev1:0': ['B'] })
        await vi.advanceTimersByTimeAsync(MEMPOOL_REPARSE_INTERVAL_MS) // a later run finds nothing new
        expect(enqueued(store)).toEqual(['seen'])
      } finally {
        clearInterval(timer)
      }
    })

    it('a replaced tx can come back: if the replacement is dropped, a rebroadcast of the original (evaluated after the exit) fires a fresh seen', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a, Date.now())
      await evaluate(mkTx('B', null, 5000, [O(1)]), Date.now())
      expect(enqueued(store)).toEqual(['seen', 'dropped'])
      expect(store.retired.get('A')).toBe(1_700_000_000_000)

      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
      chain.mempool = ['A'] // B evicted, A rebroadcast
      await process(chain.raw('b101'))
      vi.setSystemTime(1_700_000_005_000)
      await evaluate(a, Date.now())

      expect(enqueued(store)).toEqual(['seen', 'dropped', 'seen'])
      expect(store.pending.has('A')).toBe(true)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
    })

    it('RETIREMENT WATERMARK: a duplicate evaluation of A (ZMQ + reparse) that was in flight when B replaced A cannot resurrect A — exactly one dropped verdict', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, evaluate } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      // the OLDER evaluation of A is parked right after its claimant lookup (nothing written yet)
      const { parked, release } = parkOnce(store, 'outpointOwners', () => true)
      const older = evaluate(a, Date.now())
      await parked
      vi.setSystemTime(1_700_000_000_100)
      await evaluate(a, Date.now()) // the newer duplicate records A
      expect(keysOf(store)).toEqual(['regtest:A:seen'])
      vi.setSystemTime(1_700_000_000_200)
      await evaluate(mkTx('B', null, 5000, [O(1)]), Date.now()) // B replaces A
      expect(keysOf(store)).toEqual(['regtest:A:seen', 'regtest:A:dropped:replaced:B'])
      expect(store.retired.get('A')).toBe(1_700_000_000_200)

      release()
      await older // resumes well inside the fence, but its view predates A's exit

      expect(keysOf(store)).toEqual(['regtest:A:seen', 'regtest:A:dropped:replaced:B']) // no resurrection → no second verdict later
      expect(store.pending.has('A')).toBe(false)
      expect(store.records.has('A')).toBe(false)
      expect(store.evaluated.has('A')).toBe(false) // a real rebroadcast is still picked up by the reparser
      expect(claims(store)).toEqual({})
    })

    for (const watched of [false, true]) {
      it(`SPENDER GUARD: an old evaluation of A parked BEFORE its claimant lookup resumes after B (${watched ? 'watched' : 'unwatched'}) replaced A → its pre-pass replaces nothing; B stays pending; verdicts are exactly seen A → dropped A by B${watched ? ' → seen B' : ''}`, async () => {
        vi.useFakeTimers()
        vi.setSystemTime(1_700_000_000_000)
        const { store, evaluate } = wire()
        const a = mkTx('A', ADDR, 5000, [O(1)])
        // the OLD evaluation of A is parked before it reads anything but `isEvaluated`
        let release!: () => void
        const gate = new Promise<void>((r) => {
          release = r
        })
        let parkedResolve!: () => void
        const parked = new Promise<void>((r) => {
          parkedResolve = r
        })
        const isEvaluated = store.isEvaluated.bind(store)
        let armed = true
        store.isEvaluated = async (txid) => {
          const was = await isEvaluated(txid)
          if (armed && txid === 'A') {
            armed = false
            parkedResolve()
            await gate
          }
          return was
        }
        const old = evaluate(a, Date.now())
        await parked
        vi.setSystemTime(1_700_000_000_100)
        await evaluate(a, Date.now()) // the newer duplicate records A
        vi.setSystemTime(1_700_000_000_200)
        const b = mkTx('B', watched ? ADDR : null, 4900, [O(1)])
        await evaluate(b, Date.now()) // B replaces A (and, when watched, records itself)
        const expected = ['regtest:A:seen', 'regtest:A:dropped:replaced:B', ...(watched ? ['regtest:B:seen'] : [])]
        expect(keysOf(store)).toEqual(expected)

        release()
        await old // resumes: its lookup now finds B as the claimant, but its view predates A's own exit

        expect(keysOf(store)).toEqual(expected) // B was NOT replaced by the stale pass; A not resurrected
        if (watched) {
          expect(store.pending.has('B')).toBe(true)
          expect(store.records.get('B')).toMatchObject({ height: 0 })
          expect(claims(store)).toEqual({ 'prev1:0': ['B'] })
        } else {
          expect(store.evaluated.has('B')).toBe(true)
          expect(claims(store)).toEqual({})
        }
        expect(store.pending.has('A')).toBe(false)
        expect(store.evaluated.has('A')).toBe(false) // a real rebroadcast of A is still picked up later
        // watched: the stale pass found B as a claimant and the Lua refused it (the spender guard);
        // unwatched: B claimed nothing, so the old pass had nothing to adjudicate and its own
        // recordSeen was refused by A's watermark instead — either way nothing was written.
        expect(warnLog.mock.calls.some((c) => /A: a replacement it implies was refused as stale/.test(String(c[0])))).toBe(watched)
      })
    }

    it('RETIREMENT WATERMARK: dropped, then mined without a mempool sighting → confirmed through the block path (drops never tombstone); the watermark is pruned by TTL', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a, Date.now())
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
      chain.mempool = []
      await process(chain.raw('b101'))
      expect(enqueued(store)).toEqual(['seen', 'dropped'])
      expect(store.retired.has('A')).toBe(true)
      expect(store.tombstones.has('A')).toBe(false)

      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [a] }) // mined from a rebroadcast weir never saw
      await process(chain.raw('b102'))
      expect(enqueued(store)).toEqual(['seen', 'dropped', 'confirmed'])
      expect(store.maturingIndex.get('A')).toBe(102)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
      expect(store.retired.has('A')).toBe(true) // still within the TTL

      vi.setSystemTime(1_700_000_000_000 + TOMBSTONE_TTL_MS)
      chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
      await process(chain.raw('b103'))
      expect(store.retired.size).toBe(0) // pruned with the tombstones
    })
  })

  describe('TOCTOU: a decision from a stale read never mutates (replacePending / dropPending are guarded Luas)', () => {
    it('BLOCKER repro: B reads A as pending, the block promotes A meanwhile, B resumes → replacePending is a no-op: seen, confirmed, NO dropped, index intact', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a, Date.now())
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []
      // B (spends A's input, pays nobody) is parked right AFTER it read A's record (height 0,
      // pending) — it holds that snapshot and will act on it.
      const { parked, release } = parkOnce(store, 'readRecord', (txid) => txid === 'A')
      const replace = vi.spyOn(store, 'replacePending')
      const evaluation = evaluate(mkTx('B', null, 5000, [O(1)]), Date.now())
      await parked
      expect(replace).not.toHaveBeenCalled()

      await process(chain.raw('b101')) // A promoted → maturing@101, confirmed:1 enqueued
      expect(store.maturingIndex.get('A')).toBe(101)

      release()
      await evaluation // its replacePending lands AFTER the promotion: the Lua guard refuses

      expect(replace).toHaveBeenCalledTimes(1)
      expect(replace.mock.calls[0]![0]).toBe('A')
      await expect(replace.mock.results[0]!.value).resolves.toBe('skipped') // the losing call did nothing
      expect(enqueued(store)).toEqual(['seen', 'confirmed'])
      expect(store.maturingIndex.get('A')).toBe(101)
      expect(store.records.get('A')).toMatchObject({ height: 101, blockHash: 'b101', fired: [1] })
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
      expect(store.evaluated.has('B')).toBe(true)
    })

    it('the block scan and the mempool path both try to replace A: exactly ONE dropped, the loser is a no-op', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      const c = mkTx('C', null, 5000, [O(1)])
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [c] })
      chain.mempool = []
      // the block scan is parked right AFTER it read A's record (it will try replacedBy = C) ...
      const { parked, release } = parkOnce(store, 'readRecord', (txid) => txid === 'A')
      const replace = vi.spyOn(store, 'replacePending')
      const processing = process(chain.raw('b101'))
      await parked
      expect(replace).not.toHaveBeenCalled()
      // ... while a mempool tx B replaces A first
      await evaluate(mkTx('B', null, 5000, [O(1)]), Date.now())
      expect(keysOf(store)).toEqual(['regtest:A:seen', 'regtest:A:dropped:replaced:B'])
      expect(replace).toHaveBeenCalledTimes(1)

      release()
      await processing

      expect(replace).toHaveBeenCalledTimes(2)
      expect(replace.mock.calls[1]![1].replacedBy).toBe('C')
      await expect(replace.mock.results[1]!.value).resolves.toBe('skipped') // the block's stale attempt did nothing
      const dropped = txEvents(store).filter((e) => e.event === 'dropped')
      expect(dropped).toHaveLength(1)
      expect(dropped[0]!.replacedBy).toBe('B')
      expect(store.pending.size).toBe(0)
      expect(claims(store)).toEqual({})
    })

    it('DUPLICATE VERDICTS: the tip check snapshots A as dropped-candidate, a mempool B replaces A meanwhile → dropPending(A) is a no-op (no dropped:evicted after dropped:replaced)', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
      chain.mempool = [] // A is gone from the node's mempool (it was replaced there)
      // the dropped check is parked right AFTER it read A's record for the evicted payload ...
      const { parked, release } = parkOnce(store, 'readRecord', (txid) => txid === 'A')
      const drop = vi.spyOn(store, 'dropPending')
      const processing = process(chain.raw('b101'))
      await parked
      // ... while the ZMQ path delivers B, the replacement
      await evaluate(mkTx('B', null, 5000, [O(1)]), Date.now())
      expect(keysOf(store)).toEqual(['regtest:A:seen', 'regtest:A:dropped:replaced:B'])

      release()
      await processing

      expect(drop).toHaveBeenCalledTimes(1)
      await expect(drop.mock.results[0]!.value).resolves.toBe(false)
      expect(keysOf(store)).toEqual(['regtest:A:seen', 'regtest:A:dropped:replaced:B']) // no evicted verdict
      expect(store.pending.size).toBe(0)
      expect(store.records.size).toBe(0)
    })

    it('replacePending / dropPending of a pending member with a MISSING record are no-ops (nothing enqueued)', async () => {
      const { store, evaluate } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      store.records.delete('A') // corrupt: pending without a record
      const ev = { ...(store.outboxEvents()[0] as TxEvent), event: 'dropped' as const, reason: 'replaced' as const, replacedBy: 'B', idempotencyKey: 'regtest:A:dropped:replaced:B' }
      await expect(store.replacePending('A', ev, Date.now(), 'B')).resolves.toBe('skipped')
      await expect(store.dropPending('A', { ...ev, reason: 'evicted', idempotencyKey: 'regtest:A:dropped:101' })).resolves.toBe(false)
      expect(enqueued(store)).toEqual(['seen'])
      expect(store.pending.has('A')).toBe(true)
    })

    it('a stale adjudication leaves an UNWATCHED spender un-evaluated: D (pays nobody) ages past the fence at replacePending → not marked; the next reparse re-adjudicates and A is replaced', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, chain, register, evaluate, reparse } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      const d = register(mkTx('D', null, 5000, [O(1)]))
      const startedAtMs = Date.now() - MAX_EVALUATION_AGE_MS + 10 // 10ms of budget left
      const readRecord = store.readRecord.bind(store)
      store.readRecord = async (txid) => {
        vi.setSystemTime(Date.now() + 20) // the read outlives the budget
        return readRecord(txid)
      }

      await evaluate(d, startedAtMs)

      expect(enqueued(store)).toEqual(['seen'])
      expect(store.pending.has('A')).toBe(true) // NOT replaced yet ...
      expect(store.evaluated.has('D')).toBe(false) // ... and D is NOT hidden from the reparser
      expect(warnLog.mock.calls.some((c) => /D: a replacement it implies was refused as stale/.test(String(c[0])))).toBe(true)

      store.readRecord = readRecord
      chain.mempool = ['D']
      await reparse() // a fresh startedAt: the whole evaluation is redone

      expect(keysOf(store)).toEqual(['regtest:A:seen', 'regtest:A:dropped:replaced:D'])
      expect(store.pending.has('A')).toBe(false)
      expect(store.evaluated.has('D')).toBe(true)
      expect(claims(store)).toEqual({})
    })

    it('an unwatched tx whose view aged past the fence during the pre-pass (no stale adjudication) is not marked evaluated either', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, evaluate } = wire()
      const startedAtMs = Date.now() - MAX_EVALUATION_AGE_MS + 10
      const owners = store.outpointOwners.bind(store)
      store.outpointOwners = async (o) => {
        vi.setSystemTime(Date.now() + 20) // the lookup outlives the budget; nothing to adjudicate
        return owners(o)
      }

      await evaluate(mkTx('U', null, 5000, [O(9)]), startedAtMs)

      expect(store.evaluated.has('U')).toBe(false)
      expect(warnLog.mock.calls.some((c) => /U aged past the fence while being evaluated/.test(String(c[0])))).toBe(true)
    })

    it('the FENCE inside the destructive step: under the fence at the claimant lookup, over it at replacePending → nothing dropped, B (watched) left un-evaluated', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(1_700_000_000_000)
      const { store, evaluate } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      const startedAtMs = Date.now() - MAX_EVALUATION_AGE_MS + 10 // 10ms of budget left
      // the record read between the lookup and the replace takes "longer" than that
      const readRecord = store.readRecord.bind(store)
      store.readRecord = async (txid) => {
        vi.setSystemTime(Date.now() + 20)
        return readRecord(txid)
      }

      await evaluate(mkTx('B', ADDR, 4900, [O(1)]), startedAtMs)

      expect(enqueued(store)).toEqual(['seen'])
      expect(store.pending.has('A')).toBe(true)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
      expect(store.evaluated.has('B')).toBe(false)
      expect(store.records.has('B')).toBe(false)
    })

    it('DOCUMENTED IMPRECISION: two spenders of one prevout evaluated concurrently are both recorded; the loser is dropped/evicted at the tip check, never lost', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      const b = mkTx('B', ADDR, 4900, [O(1)])

      await Promise.all([evaluate(a, Date.now()), evaluate(b, Date.now())]) // both lookups see no claimant

      expect(keysOf(store)).toEqual(['regtest:A:seen', 'regtest:B:seen'])
      expect(claims(store)).toEqual({ 'prev1:0': ['A', 'B'] })

      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
      chain.mempool = ['B'] // bitcoind kept the higher fee
      await process(chain.raw('b101'))

      expect(txEvents(store)[2]).toMatchObject({ event: 'dropped', txid: 'A', reason: 'evicted', idempotencyKey: 'regtest:A:dropped:101' })
      expect(txEvents(store)[2]!.replacedBy).toBeUndefined()
      expect(claims(store)).toEqual({ 'prev1:0': ['B'] }) // A released only its own claim
      expect(store.pending.has('B')).toBe(true)
    })
  })

  describe('several claimants per prevout (round-3 regressions under the SET model)', () => {
    it('mined A + concurrent B and C all claim X; A reorgs out and C confirms → B dropped:replaced by C, A proven conflicted by C, C claims intact; C later demoted, a spender of X finds C', async () => {
      const { store, chain, evaluate, process } = await maturingClaimant()
      const b = mkTx('B', ADDR, 4900, [O(1)])
      const c = mkTx('C', ADDR, 4800, [O(1)])
      await Promise.all([evaluate(b, Date.now()), evaluate(c, Date.now())]) // both see only mined A: warned, both recorded
      expect(claims(store)).toEqual({ 'prev1:0': ['A', 'B', 'C'] })
      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'seen', 'seen'])

      // reorg: b101 out, b101b confirms C (spending X)
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [c] })
      chain.mempool = []
      await process(chain.raw('b101b'))

      const verdicts = txEvents(store).slice(4)
      expect(verdicts.map((e) => `${e.event}:${e.txid}`).sort()).toEqual(['confirmed:C', 'conflicted:A', 'dropped:B'])
      expect(verdicts.find((e) => e.txid === 'A')).toMatchObject({ reason: 'double-spend', conflictingTxid: 'C', idempotencyKey: 'regtest:A:conflicted' })
      expect(verdicts.find((e) => e.txid === 'B')).toMatchObject({ reason: 'replaced', replacedBy: 'C', idempotencyKey: 'regtest:B:dropped:replaced:C' })
      expect(claims(store)).toEqual({ 'prev1:0': ['C'] }) // A and B released only themselves; C's claim intact
      expect(store.maturingIndex.get('C')).toBe(101)
      expect(store.limbo.size).toBe(0)

      // reorg again: C's block is orphaned, C returns to the mempool → demoted, claim kept
      chain.addBlock({ hash: 'b101c', prevHash: 'b100', height: 101, time: 1_700_000_121, txs: [] })
      chain.addBlock({ hash: 'b102c', prevHash: 'b101c', height: 102, time: 1_700_000_122, txs: [] })
      chain.mempoolEntries.set('C', { time: 1 })
      chain.mempool = ['C']
      await process(chain.raw('b102c'))
      expect(txEvents(store).slice(-1)[0]).toMatchObject({ event: 'demoted', txid: 'C' })
      expect(store.pending.has('C')).toBe(true)
      expect(claims(store)).toEqual({ 'prev1:0': ['C'] })

      // an unwatched D spends X: the lookup finds C
      await evaluate(mkTx('D', null, 5000, [O(1)]), Date.now())
      expect(txEvents(store).slice(-1)[0]).toMatchObject({ event: 'dropped', txid: 'C', reason: 'replaced', replacedBy: 'D', idempotencyKey: 'regtest:C:dropped:replaced:D' })
      expect(claims(store)).toEqual({})
      expect(store.pending.size).toBe(0)
    })

    it('B and C both claim X (concurrently); an unwatched D spends X → BOTH are dropped:replaced by D in one pass', async () => {
      const { store, evaluate } = wire()
      await Promise.all([evaluate(mkTx('B', ADDR, 4900, [O(1)]), Date.now()), evaluate(mkTx('C', ADDR, 4800, [O(1), O(2)]), Date.now())])
      expect(claims(store)).toEqual({ 'prev1:0': ['B', 'C'], 'prev2:0': ['C'] })

      await evaluate(mkTx('D', null, 5000, [O(1)]), Date.now())

      const dropped = txEvents(store).filter((e) => e.event === 'dropped')
      expect(dropped.map((e) => e.idempotencyKey).sort()).toEqual(['regtest:B:dropped:replaced:D', 'regtest:C:dropped:replaced:D'])
      expect(store.pending.size).toBe(0)
      expect(claims(store)).toEqual({}) // C released prev2 too
      expect(store.evaluated.has('D')).toBe(true)
    })

    it('a demoted claimant keeps its claim; a concurrent claimant of another prevout is untouched by its later conflict', async () => {
      const { store, chain, evaluate, process } = await maturingClaimant()
      await evaluate(mkTx('B', ADDR, 4900, [O(1), O(2)]), Date.now()) // claims alongside mined A
      expect(claims(store)).toEqual({ 'prev1:0': ['A', 'B'], 'prev2:0': ['B'] })
      // reorg: A displaced, not re-included, not in the mempool → conflicted by elimination
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
      chain.mempool = ['B']
      await process(chain.raw('b101b'))

      expect(txEvents(store).filter((e) => e.event === 'conflicted')).toHaveLength(1)
      expect(claims(store)).toEqual({ 'prev1:0': ['B'], 'prev2:0': ['B'] }) // A released only itself
      expect(store.pending.has('B')).toBe(true)
    })
  })

  describe('block path (rule 4)', () => {
    it('a confirmed double-spend of a pending tx: dropped with reason replaced / replacedBy = the mined spender; the tip dropped check does not emit a second dropped', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1), O(2)]), Date.now())
      const c = mkTx('C', 'bcrt1qsomeoneelse', 5000, [O(2)]) // never seen in the mempool by weir
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [c] })
      chain.mempool = [] // A is gone from the node's mempool too — the old code would have said "evicted"

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
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      const b = mkTx('B', ADDR, 4900, [O(1)])
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [b] })
      chain.mempool = []

      await process(chain.raw('b101'))

      expect(enqueued(store)).toEqual(['seen', 'dropped', 'confirmed'])
      expect(txEvents(store)[1]).toMatchObject({ txid: 'A', reason: 'replaced', replacedBy: 'B' })
      expect(txEvents(store)[2]).toMatchObject({ txid: 'B', confs: 1, idempotencyKey: 'regtest:B:confirmed:1:b101' })
      expect(store.maturingIndex.get('B')).toBe(101)
      expect(claims(store)).toEqual({ 'prev1:0': ['B'] })
    })

    it('the pending tx itself being mined is not a replacement (claimant == spender): promoted, no dropped', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a, Date.now())
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []

      await process(chain.raw('b101'))

      expect(enqueued(store)).toEqual(['seen', 'confirmed'])
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })
    })

    it("REORG: the new chain spends a limbo tx's input → PROVEN conflicted (reason double-spend, conflictingTxid, block timestamp); resolveLimbo emits NO second verdict", async () => {
      const { store, chain, rpc, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a, Date.now())
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] }, { main: false })
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
      expect(probe).not.toHaveBeenCalled() // A left limbo inside `conflict`; resolveLimbo had nothing to adjudicate
      expect(store.limbo.size).toBe(0)
      expect(store.maturingIndex.has('A')).toBe(false)
      expect(store.records.has('A')).toBe(false)
      expect(store.pending.has('A')).toBe(false)
      expect(store.tombstones.has('A')).toBe(true)
      expect(claims(store)).toEqual({})
      expect(store.tip).toEqual({ hash: 'b101b', height: 101 })
    })

    it('REORG: a proven conflict during the catch-up walk (non-tip block) is adjudicated there, and a later block confirms nothing for it', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a, Date.now())
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] }, { main: false })
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
      await evaluate(a, Date.now())
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] }, { main: false })
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
      await evaluate(a, Date.now())
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] }, { main: false })
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
      expect(store.tombstones.has('A')).toBe(true)
    })

    it('a block spender hitting a MATURING claimant outside limbo is impossible on a valid chain: error log, skipped, nothing emitted', async () => {
      const { store, chain, process } = await maturingClaimant()
      chain.mainChain.set(101, 'b101')
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
      await evaluate(a, Date.now())
      expect(claims(store)).toEqual({ 'prev1:0': ['A'], 'prev2:5': ['A'] })
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
      chain.addBlock({ hash: 'b103', prevHash: 'b102', height: 103, time: 1_700_000_103, txs: [] })
      chain.mempool = []

      await process(chain.raw('b101'))
      await process(chain.raw('b102'))
      expect(claims(store)).toEqual({ 'prev1:0': ['A'], 'prev2:5': ['A'] })
      expect(store.records.get('A')).toMatchObject({ inputs: [O(1), O(2, 5)] })

      await process(chain.raw('b103')) // 3 confs → finishMaturing
      expect(store.maturingIndex.has('A')).toBe(false)
      expect(claims(store)).toEqual({})
    })

    it('a never-seen mined tx claims through promotion', async () => {
      const { store, chain, process } = wire()
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [mkTx('N', ADDR, 5000, [O(4)])] })
      await process(chain.raw('b101'))
      expect(claims(store)).toEqual({ 'prev4:0': ['N'] })
    })

    it('released by the tip-block dropped check (reason evicted)', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
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
      await evaluate(a, Date.now())
      chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] }, { main: false })
      chain.mempool = []
      await process(chain.raw('b101a'))
      chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
      chain.mempoolEntries.set('A', { time: 1 })
      chain.mempool = ['A']
      await process(chain.raw('b101b'))
      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'demoted'])
      expect(store.pending.has('A')).toBe(true)
      expect(claims(store)).toEqual({ 'prev1:0': ['A'] })

      await evaluate(mkTx('B', ADDR, 4900, [O(1)]), Date.now()) // fee-bump of the demoted tx

      expect(enqueued(store)).toEqual(['seen', 'confirmed', 'demoted', 'dropped', 'seen'])
      expect(txEvents(store)[3]).toMatchObject({ txid: 'A', reason: 'replaced', replacedBy: 'B', idempotencyKey: 'regtest:A:dropped:replaced:B' })
      expect(claims(store)).toEqual({ 'prev1:0': ['B'] })
    })

    it('released by endTracking (watch removed mid-flight)', async () => {
      const { store, chain, evaluate, process } = wire()
      const a = mkTx('A', ADDR, 5000, [O(1)])
      await evaluate(a, Date.now())
      store.watches.delete(ADDR)
      chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] })
      chain.mempool = []

      await process(chain.raw('b101'))

      expect(enqueued(store)).toEqual(['seen'])
      expect(store.records.has('A')).toBe(false)
      expect(claims(store)).toEqual({})
    })

    it('wiped by clearTracking (prune-window guard)', async () => {
      const { store, chain, evaluate, process } = wire()
      await evaluate(mkTx('A', ADDR, 5000, [O(1)]), Date.now())
      for (let h = 101; h <= 104; h++) chain.addBlock({ hash: `b${h}`, prevHash: `b${h - 1}`, height: h, time: 1_700_000_000 + h, txs: [] })
      chain.pruned = true
      chain.pruneheight = 103

      await process(chain.raw('b104'))

      expect(store.pending.size).toBe(0)
      expect(claims(store)).toEqual({})
    })
  })
})
