import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { reconcile } from '../src/boot/reconcile'
import { makeBlockPipeline } from '../src/engine/blockPipeline'
import type { MatchedOutput, TxEvent } from '../src/lib/types'
import { ADDR, FakeChain, FakeStore, mkTx } from './fakes'

const MATCHED: MatchedOutput[] = [{ address: ADDR, vout: 0, valueSats: 5000 }]

/** Boot reconciliation wired to the REAL block pipeline over a FakeStore/FakeChain. */
function setup() {
  const store = new FakeStore()
  const chain = new FakeChain()
  const cfg = { network: 'regtest' as const, confirmMilestones: [1, 3], maxMilestone: 3, ringSize: 12 }
  const rpc = chain.rpc()
  const getBlockRaw = vi.spyOn(rpc, 'getBlockRaw')
  const pipeline = makeBlockPipeline({ cfg, store, rpc, decodeBlock: chain.decode })
  const processBlock = vi.fn(pipeline.processBlock)
  const settleTip = vi.fn(pipeline.settleTip)
  chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
  chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
  chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
  store.watches.add(ADDR)
  return { store, chain, rpc, getBlockRaw, processBlock, settleTip, run: () => reconcile({ cfg, store, rpc, processBlock, settleTip }) }
}

const events = (store: FakeStore) => store.outboxEvents().map((e) => (e.event === 'expired' || e.event === 'heartbeat' ? e.event : `${e.event}:${(e as TxEvent).txid}`))

describe('reconcile', () => {
  beforeAll(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
  })
  afterAll(() => {
    vi.restoreAllMocks()
  })

  it('first run (no tip): initializes tip + ring to the node best block, forward-only — no processBlock, settle is a no-op', async () => {
    const { store, getBlockRaw, processBlock, settleTip, run } = setup()

    await run()

    expect(store.tip).toEqual({ hash: 'b102', height: 102 })
    expect(await store.ringAll()).toEqual([{ height: 102, hash: 'b102' }])
    expect(processBlock).not.toHaveBeenCalled()
    expect(getBlockRaw).not.toHaveBeenCalled() // no backfill of b100/b101
    expect(settleTip).toHaveBeenCalledTimes(1)
  })

  it('tip == best: no block processed, but the tip is settled (limbo left by a crash between rewind and resolution is adjudicated)', async () => {
    const { store, chain, processBlock, run } = setup()
    store.tip = { hash: 'b102', height: 102 }
    store.ring.set('b102', 102)
    // a crash right after a rewind: tx1 sits in limbo with its old record; the node has it in the mempool
    store.limbo.add('tx1')
    store.records.set('tx1', { txid: 'tx1', height: 101, blockHash: 'b101x', matched: MATCHED, fired: [1], hex: 'hex-tx1', inputs: [] })
    chain.mempool = ['tx1']

    await run()

    expect(processBlock).not.toHaveBeenCalled()
    expect(events(store)).toEqual(['demoted:tx1'])
    expect(store.pending.has('tx1')).toBe(true)
    expect(store.limbo.size).toBe(0)
  })

  it('tip behind best: the best block is handed to the processor once (the pipeline walks the gap), then settled', async () => {
    const { store, chain, getBlockRaw, processBlock, run } = setup()
    store.tip = { hash: 'b100', height: 100 }
    store.ring.set('b100', 100)

    await run()

    expect(getBlockRaw).toHaveBeenCalledWith('b102')
    expect(processBlock).toHaveBeenCalledTimes(1)
    expect(processBlock.mock.calls[0]![0]).toEqual(chain.raw('b102'))
    expect(store.tip).toEqual({ hash: 'b102', height: 102 })
    expect(chain.getBlockHashCalls).toEqual([101]) // the gap walk
  })

  it('tip diverged from best on a chain the node has built PAST our tip: the processor handles the reorg itself', async () => {
    const { store, chain, processBlock, run } = setup()
    chain.addBlock({ hash: 'b101x', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] }, { main: false })
    store.tip = { hash: 'b101x', height: 101 }
    store.ring.set('b100', 100)
    store.ring.set('b101x', 101)

    await run()

    expect(processBlock).toHaveBeenCalledTimes(1)
    expect(processBlock.mock.calls[0]![0]).toEqual(chain.raw('b102'))
    expect(await store.ringAll()).toEqual([
      { height: 100, hash: 'b100' },
      { height: 101, hash: 'b101' },
      { height: 102, hash: 'b102' },
    ])
  })

  it('REGRESSION (livelock on a stable node): a100 → z100 → a100 with the node then stable converges — the ring keeps one hash per height, z100 is disconnected, a100 applied, limbo empty', async () => {
    const { store, chain, processBlock, settleTip, run } = setup()
    chain.mainChain.clear()
    chain.addBlock({ hash: 'b99', prevHash: '', height: 99, time: 1_700_000_099, txs: [] })
    const a = mkTx('A')
    chain.addBlock({ hash: 'a100', prevHash: 'b99', height: 100, time: 1_700_000_100, txs: [a] })
    // weir booted at a100: A maturing there, milestone 1 fired
    store.tip = { hash: 'a100', height: 100 }
    store.ring.set('b99', 99)
    store.ring.set('a100', 100)
    store.maturingIndex.set('A', 100)
    store.records.set('A', { txid: 'A', height: 100, blockHash: 'a100', matched: MATCHED, fired: [1], hex: 'hex-A', inputs: [] })

    // reorg to z100 (same parent, A back in the mempool): rewind → limbo → demoted
    chain.addBlock({ hash: 'z100', prevHash: 'b99', height: 100, time: 1_700_000_110, txs: [] })
    chain.mempool = ['A']
    await processBlock(chain.raw('z100'))
    expect(events(store)).toEqual(['demoted:A'])
    expect(await store.ringAll()).toEqual([{ height: 99, hash: 'b99' }, { height: 100, hash: 'z100' }])

    // back to a100 (A mined there again), node stable; weir restarts: reconcile must converge
    chain.mainChain.set(100, 'a100')
    chain.mempool = []
    await run()

    expect(settleTip.mock.results.length).toBeGreaterThan(0)
    await expect(settleTip.mock.results[settleTip.mock.results.length - 1]!.value).resolves.toBe(true)
    expect(store.tip).toEqual({ hash: 'a100', height: 100 })
    expect(await store.ringAll()).toEqual([{ height: 99, hash: 'b99' }, { height: 100, hash: 'a100' }]) // exactly one hash at 100
    expect(store.limbo.size).toBe(0)
    expect(store.pending.size).toBe(0)
    expect(store.maturingIndex.get('A')).toBe(100)
    expect(events(store)).toEqual(['demoted:A', 'confirmed:A']) // re-included under a100 again
    expect(store.outboxEvents()[1]).toMatchObject({ idempotencyKey: 'regtest:A:confirmed:1:a100' })

    // and the same flip as a ZMQ packet (weir up the whole time): z100 again, then a100 again
    chain.mainChain.set(100, 'z100')
    chain.mempool = ['A']
    await processBlock(chain.raw('z100'))
    expect(events(store)).toEqual(['demoted:A', 'confirmed:A', 'demoted:A'])
    chain.mainChain.set(100, 'a100')
    chain.mempool = []
    await processBlock(chain.raw('a100')) // a different hash at a known height is a replacement, not a duplicate
    expect(store.tip).toEqual({ hash: 'a100', height: 100 })
    expect(await store.ringAll()).toEqual([{ height: 99, hash: 'b99' }, { height: 100, hash: 'a100' }])
    expect(events(store)).toEqual(['demoted:A', 'confirmed:A', 'demoted:A', 'confirmed:A'])
    expect(store.limbo.size).toBe(0)
  })

  it("REGRESSION (legacy ring): a ring written before the one-per-height invariant — b99, {a100, z100}, o101 — is rebuilt from the stored tip's ancestry at boot, so the fork search and the rewind agree; a tx mined in z100 is `demoted` when the node later reorgs to a100→a101→a102, never `confirmed:3:z100`", async () => {
    const { store, chain, processBlock, run } = setup()
    chain.mainChain.clear()
    chain.addBlock({ hash: 'b99', prevHash: '', height: 99, time: 1_700_000_099, txs: [] })
    const t = mkTx('T')
    chain.addBlock({ hash: 'a100', prevHash: 'b99', height: 100, time: 1_700_000_100, txs: [] }, { main: false })
    chain.addBlock({ hash: 'z100', prevHash: 'b99', height: 100, time: 1_700_000_110, txs: [t] })
    chain.addBlock({ hash: 'o101', prevHash: 'z100', height: 101, time: 1_700_000_111, txs: [] }, { main: false })
    chain.addBlock({ hash: 'y101', prevHash: 'z100', height: 101, time: 1_700_000_121, txs: [] }) // the node's best: o101 was replaced
    // weir's stored state from before the invariant: tip o101, BOTH a100 and z100 at 100, T maturing in z100
    store.tip = { hash: 'o101', height: 101 }
    store.ring.set('b99', 99)
    store.ring.set('a100', 100)
    store.ring.set('z100', 100)
    store.ring.set('o101', 101)
    store.maturingIndex.set('T', 100)
    store.records.set('T', { txid: 'T', height: 100, blockHash: 'z100', matched: MATCHED, fired: [1], hex: 'hex-T', inputs: [] })
    chain.mempool = []

    await run()

    // rebuilt from o101's ancestry (a100 gone), then o101 → y101 reorg: z100 kept as the fork point, T untouched
    expect(await store.ringAll()).toEqual([{ height: 99, hash: 'b99' }, { height: 100, hash: 'z100' }, { height: 101, hash: 'y101' }])
    expect(store.tip).toEqual({ hash: 'y101', height: 101 })
    expect(store.maturingIndex.get('T')).toBe(100)
    expect(events(store)).toEqual([])

    // the node reorgs to a100 → a101 → a102: z100 is orphaned, T returns to the mempool
    chain.mainChain.set(100, 'a100')
    chain.addBlock({ hash: 'a101', prevHash: 'a100', height: 101, time: 1_700_000_131, txs: [] })
    chain.addBlock({ hash: 'a102', prevHash: 'a101', height: 102, time: 1_700_000_132, txs: [] })
    chain.mempool = ['T']
    await processBlock(chain.raw('a102'))

    expect(events(store)).toEqual(['demoted:T'])
    expect(store.outboxEvents()[0]).toMatchObject({ blockHash: 'z100', idempotencyKey: 'regtest:T:demoted:z100' })
    expect(store.outboxEvents().map((e) => e.idempotencyKey)).not.toContain('regtest:T:confirmed:3:z100')
    expect(store.pending.has('T')).toBe(true)
    expect(store.limbo.size).toBe(0)
    expect(await store.ringAll()).toEqual([
      { height: 99, hash: 'b99' },
      { height: 100, hash: 'a100' },
      { height: 101, hash: 'a101' },
      { height: 102, hash: 'a102' },
    ])
  })

  it('a stored tip the node does not know at all (legacy ring) falls back to the prune-window reset: tracking wiped, tip/ring jumped to the node\'s best', async () => {
    const { store, chain, processBlock, run } = setup()
    store.tip = { hash: 'ghost101', height: 101 } // no such block anywhere
    store.ring.set('b100', 100)
    store.ring.set('x101', 101)
    store.ring.set('ghost101', 101)
    store.pending.add('P')
    store.records.set('P', { txid: 'P', height: 0, blockHash: '', matched: MATCHED, fired: [], hex: 'hex-P', inputs: [] })

    await run()

    expect(store.tip).toEqual({ hash: 'b102', height: 102 })
    expect(await store.ringAll()).toEqual([{ height: 100, hash: 'b100' }, { height: 101, hash: 'b101' }, { height: 102, hash: 'b102' }]) // the best's ancestry, one per height
    expect(store.pending.size).toBe(0)
    expect(store.records.size).toBe(0)
    expect(processBlock).not.toHaveBeenCalled()
  })

  it('REGRESSION (reconcile round cap): a node that keeps advancing is followed past 20 rounds — reconcile resolves only when the stored tip IS the best and settle succeeded', async () => {
    const { store, chain, rpc, processBlock, settleTip, run } = setup()
    store.tip = { hash: 'b100', height: 100 }
    store.ring.set('b100', 100)
    chain.mainChain.delete(101)
    chain.mainChain.delete(102)
    chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
    // every time weir fetches the best block, the node has mined the next one — 25 times
    let mined = 0
    const getBlockRaw = rpc.getBlockRaw
    rpc.getBlockRaw = async (hash: string) => {
      const raw = await getBlockRaw(hash)
      if (mined < 25) {
        mined++
        const h = 101 + mined
        chain.addBlock({ hash: `b${h}`, prevHash: `b${h - 1}`, height: h, time: 1_700_000_000 + h, txs: [] })
      }
      return raw
    }

    await run()

    expect(mined).toBe(25)
    expect(processBlock).toHaveBeenCalledTimes(26) // b101 … b126, one per round
    expect(settleTip).toHaveBeenCalledTimes(26)
    expect(settleTip.mock.results.map((r) => r.value)).toHaveLength(26)
    await expect(settleTip.mock.results[25]!.value).resolves.toBe(true) // only the last round settled
    for (const r of settleTip.mock.results.slice(0, 25)) await expect(r.value).resolves.toBe(false)
    expect(store.tip).toEqual({ hash: 'b126', height: 126 })
    expect(await rpc.getBestBlockHash()).toBe('b126')
  })

  it("REGRESSION (restart onto an ancestor): the stored tip is no longer on the node's chain and the node has NOT built past it — reconcile rewinds to the fork point and settles limbo from the mempool snapshot", async () => {
    // Mine A in 101, invalidateblock 101, restart weir before a replacement is mined: node
    // best = 100 (in our ring), stored tip = 101. Handing b100 to the processor would hit its
    // "already in the ring" duplicate check and A would stay maturing forever.
    const { store, chain, processBlock, run } = setup()
    const a = mkTx('A')
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] }, { main: false })
    chain.mainChain.delete(101) // invalidated: the node's active chain ends at b100
    chain.mainChain.delete(102)
    store.tip = { hash: 'b101a', height: 101 }
    store.ring.set('b100', 100)
    store.ring.set('b101a', 101)
    store.maturingIndex.set('A', 101)
    store.records.set('A', { txid: 'A', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-A', inputs: [] })
    chain.mempool = ['A'] // back in the node's mempool

    await run()

    expect(processBlock).not.toHaveBeenCalled() // the fork point IS the node's best: nothing to process
    expect(store.tip).toEqual({ hash: 'b100', height: 100 })
    expect(await store.ringAll()).toEqual([{ height: 100, hash: 'b100' }])
    expect(events(store)).toEqual(['demoted:A'])
    expect(store.outboxEvents()[0]).toMatchObject({ blockHash: 'b101a', idempotencyKey: 'regtest:A:demoted:b101a' })
    expect(store.pending.has('A')).toBe(true)
    expect(store.maturingIndex.has('A')).toBe(false)
    expect(store.limbo.size).toBe(0)

    // the replacement block re-mines A → confirmed:1 under the new hash
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_121, txs: [a] })
    chain.mempool = []
    await processBlock(chain.raw('b101b'))
    expect(events(store)).toEqual(['demoted:A', 'confirmed:A'])
    expect(store.outboxEvents()[1]).toMatchObject({ idempotencyKey: 'regtest:A:confirmed:1:b101b' })
  })

  it('REGRESSION (boot bypasses the tip gate): the node advances while reconcile walks — limbo is NOT resolved from a stale view; reconcile goes another round and the new block re-includes the tx', async () => {
    // weir's tip is b101a (A maturing there). While down, the node reorged to b101b (empty).
    // Reconcile reads best = b101b and processes it (rewind: A → limbo); by the time that
    // block's tip work runs, the node is already at b102b, which re-includes A. Resolving
    // limbo from the live mempool now would call A `conflicted` (it is mined, not in the
    // mempool). The tip gate defers, and the next round processes b102b: A is re-included.
    const { store, chain, rpc, processBlock, run } = setup()
    const a = mkTx('A')
    chain.addBlock({ hash: 'b101a', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [a] }, { main: false })
    store.tip = { hash: 'b101a', height: 101 }
    store.ring.set('b100', 100)
    store.ring.set('b101a', 101)
    store.maturingIndex.set('A', 101)
    store.records.set('A', { txid: 'A', height: 101, blockHash: 'b101a', matched: MATCHED, fired: [1], hex: 'hex-A', inputs: [] })
    chain.mainChain.delete(102)
    chain.addBlock({ hash: 'b101b', prevHash: 'b100', height: 101, time: 1_700_000_111, txs: [] })
    chain.mempool = [] // A is not in the mempool: from the node's point of view it is mined in b102b
    // the node mines b102b (re-including A) the moment weir fetches b101b's raw bytes
    const getBlockRaw = rpc.getBlockRaw
    rpc.getBlockRaw = async (hash: string) => {
      const raw = await getBlockRaw(hash)
      if (hash === 'b101b') chain.addBlock({ hash: 'b102b', prevHash: 'b101b', height: 102, time: 1_700_000_112, txs: [a] })
      return raw
    }

    await run()

    expect(processBlock).toHaveBeenCalledTimes(2) // b101b, then (next round) b102b
    expect(store.tip).toEqual({ hash: 'b102b', height: 102 })
    expect(events(store)).toEqual(['confirmed:A']) // re-included: no conflicted, no demoted
    expect(store.outboxEvents()[0]).toMatchObject({ idempotencyKey: 'regtest:A:confirmed:1:b102b' })
    expect(store.maturingIndex.get('A')).toBe(102)
    expect(store.limbo.size).toBe(0)
  })
})
