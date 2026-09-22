import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { reconcile } from '../src/boot/reconcile'
import { FakeChain, FakeStore } from './fakes'

function setup() {
  const store = new FakeStore()
  const chain = new FakeChain()
  let best = ''
  const base = chain.rpc()
  const rpc = {
    getBestBlockHash: async () => best,
    getBlockHeader: base.getBlockHeader,
    getBlockRaw: vi.fn(base.getBlockRaw),
  }
  const processBlock = vi.fn(async (_raw: Buffer) => {})
  chain.addBlock({ hash: 'b100', prevHash: '', height: 100, time: 1_700_000_100, txs: [] })
  chain.addBlock({ hash: 'b101', prevHash: 'b100', height: 101, time: 1_700_000_101, txs: [] })
  chain.addBlock({ hash: 'b102', prevHash: 'b101', height: 102, time: 1_700_000_102, txs: [] })
  return {
    store,
    chain,
    rpc,
    processBlock,
    setBest: (hash: string) => {
      best = hash
    },
    run: () => reconcile({ store, rpc, processBlock }),
  }
}

describe('reconcile', () => {
  beforeAll(() => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
  })
  afterAll(() => {
    vi.restoreAllMocks()
  })

  it('first run (no tip): initializes tip + ring to the node best block, forward-only — no processBlock', async () => {
    const { store, rpc, processBlock, setBest, run } = setup()
    setBest('b102')

    await run()

    expect(store.tip).toEqual({ hash: 'b102', height: 102 })
    expect(await store.ringAll()).toEqual([{ height: 102, hash: 'b102' }])
    expect(processBlock).not.toHaveBeenCalled()
    expect(rpc.getBlockRaw).not.toHaveBeenCalled() // no backfill of b100/b101
  })

  it('tip == best: no-op', async () => {
    const { store, rpc, processBlock, setBest, run } = setup()
    store.tip = { hash: 'b102', height: 102 }
    store.ring.set('b102', 102)
    setBest('b102')

    await run()

    expect(store.tip).toEqual({ hash: 'b102', height: 102 })
    expect(await store.ringAll()).toEqual([{ height: 102, hash: 'b102' }])
    expect(processBlock).not.toHaveBeenCalled()
    expect(rpc.getBlockRaw).not.toHaveBeenCalled()
  })

  it('tip behind best: processBlock is called once with the best block raw bytes (the pipeline walks the gap)', async () => {
    const { store, chain, rpc, processBlock, setBest, run } = setup()
    store.tip = { hash: 'b100', height: 100 }
    store.ring.set('b100', 100)
    setBest('b102')

    await run()

    expect(rpc.getBlockRaw).toHaveBeenCalledTimes(1)
    expect(rpc.getBlockRaw).toHaveBeenCalledWith('b102')
    expect(processBlock).toHaveBeenCalledTimes(1)
    expect(processBlock.mock.calls[0]![0]).toEqual(chain.raw('b102'))
    // reconcile itself touches nothing else: the processor owns tip/ring updates
    expect(store.tip).toEqual({ hash: 'b100', height: 100 })
  })

  it('tip diverged from best (reorg while down): still just hands best to the processor', async () => {
    const { store, chain, processBlock, setBest, run } = setup()
    chain.addBlock({ hash: 'b102x', prevHash: 'b101', height: 102, time: 1_700_000_112, txs: [] }, { main: false })
    store.tip = { hash: 'b102x', height: 102 }
    store.ring.set('b102x', 102)
    setBest('b102')

    await run()

    expect(processBlock).toHaveBeenCalledTimes(1)
    expect(processBlock.mock.calls[0]![0]).toEqual(chain.raw('b102'))
  })
})
