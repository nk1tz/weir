import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { preflight } from '../src/boot/preflight'
import type { Network } from '../src/lib/types'
import { FakeStore } from './fakes'

const BOTH_ZMQ = [
  { type: 'pubrawtx', address: 'tcp://0.0.0.0:28332' },
  { type: 'pubrawblock', address: 'tcp://0.0.0.0:28332' },
]

function setup(network: Network = 'regtest') {
  const store = new FakeStore()
  const state = {
    info: { chain: 'regtest', blocks: 150, pruned: false } as { chain: string; blocks: number; pruned: boolean; pruneheight?: number },
    zmq: BOTH_ZMQ.map((n) => ({ ...n })),
    rpcDown: false,
  }
  const rpc = {
    getBlockchainInfo: async () => {
      if (state.rpcDown) throw new Error('fetch failed: connect ECONNREFUSED 127.0.0.1:18443')
      return state.info
    },
    getZmqNotifications: async () => state.zmq,
  }
  return { store, rpc, state, run: () => preflight({ cfg: { network }, store, rpc }) }
}

const logged = (spy: ReturnType<typeof vi.spyOn>): string[] => spy.mock.calls.map((c) => String(c[0]))

describe('preflight', () => {
  let logSpy: ReturnType<typeof vi.spyOn>
  let warnSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    logSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('happy path resolves and logs every check, including the capacity estimate', async () => {
    const { store, state, run } = setup()
    store.memory = { usedBytes: 10 * 1024 * 1024, maxBytes: 256 * 1024 * 1024 }
    state.info.pruned = true

    await expect(run()).resolves.toBeUndefined()

    const lines = logged(logSpy)
    expect(lines.some((l) => /redis reachable/.test(l))).toBe(true)
    expect(lines.some((l) => /maxmemory-policy=noeviction — OK/.test(l))).toBe(true)
    expect(lines.some((l) => /bitcoind reachable: chain=regtest blocks=150 pruned=true — pruned is fine/.test(l))).toBe(true)
    expect(lines.some((l) => /zmq publishers OK/.test(l))).toBe(true)
    // (256MB/1.5 − 45MB) / 330 bytes
    const expected = Math.floor(((256 * 1024 * 1024) / 1.5 - 45 * 1024 * 1024) / 330)
    expect(lines.some((l) => l.includes(`estimated watch capacity ~${expected} addresses`))).toBe(true)
    expect(warnSpy).not.toHaveBeenCalled()
  })

  it('no maxmemory → capacity estimate skipped', async () => {
    const { store, run } = setup()
    store.memory = { usedBytes: 1, maxBytes: null }

    await run()

    expect(logged(logSpy).some((l) => /skipping capacity estimate/.test(l))).toBe(true)
  })

  it('FATAL: maxmemory-policy other than noeviction', async () => {
    const { store, run } = setup()
    store.policy = 'allkeys-lru'

    await expect(run()).rejects.toThrow(/maxmemory-policy is "allkeys-lru" — must be "noeviction"/)
    await expect(run()).rejects.toThrow(/CONFIG SET maxmemory-policy noeviction/)
  })

  it('CONFIG blocked (policy null) only warns and continues', async () => {
    const { store, run } = setup()
    store.policy = null

    await expect(run()).resolves.toBeUndefined()

    expect(logged(warnSpy).some((l) => /CONFIG is blocked/.test(l) && /ensure it is noeviction/.test(l))).toBe(true)
  })

  it('FATAL: redis unreachable (memoryInfo rejects) — first check, nothing else runs', async () => {
    const { store, rpc, run } = setup()
    store.memoryInfo = async () => {
      throw new Error('redis unreachable after 5 connection attempts')
    }
    const infoSpy = vi.spyOn(rpc, 'getBlockchainInfo')

    await expect(run()).rejects.toThrow(/redis unreachable/)
    expect(infoSpy).not.toHaveBeenCalled()
  })

  it.each([
    ['mainnet', 'main', 'regtest'],
    ['testnet', 'test', 'main'],
    ['signet', 'signet', 'test'],
    ['regtest', 'regtest', 'signet'],
  ] as const)('FATAL: NETWORK=%s expects chain "%s", node reports "%s"', async (network, expected, actual) => {
    const { state, run } = setup(network)
    state.info.chain = actual

    await expect(run()).rejects.toThrow(
      `preflight: bitcoind chain "${actual}" does not match NETWORK=${network} (expected chain "${expected}")`,
    )
  })

  it('FATAL: bitcoind unreachable', async () => {
    const { state, run } = setup()
    state.rpcDown = true
    await expect(run()).rejects.toThrow(/ECONNREFUSED/)
  })

  it('FATAL: missing pubrawtx names the problem and the bitcoin.conf lines', async () => {
    const { state, run } = setup()
    state.zmq = [{ type: 'pubrawblock', address: 'tcp://0.0.0.0:28332' }]

    const err = await run().catch((e: unknown) => e as Error)
    expect(err).toBeInstanceOf(Error)
    expect((err as Error).message).toMatch(/missing required ZMQ publishers \(found: pubrawblock\)/)
    expect((err as Error).message).toContain('zmqpubrawtx=tcp://0.0.0.0:28332')
    expect((err as Error).message).toContain('zmqpubrawblock=tcp://0.0.0.0:28332')
  })

  it('FATAL: missing pubrawblock', async () => {
    const { state, run } = setup()
    state.zmq = [
      { type: 'pubrawtx', address: 'tcp://0.0.0.0:28332' },
      { type: 'pubhashblock', address: 'tcp://0.0.0.0:28332' },
    ]

    await expect(run()).rejects.toThrow(/missing required ZMQ publishers \(found: pubrawtx, pubhashblock\)/)
  })

  it('FATAL: no ZMQ publishers at all reports "none"', async () => {
    const { state, run } = setup()
    state.zmq = []
    await expect(run()).rejects.toThrow(/\(found: none\)/)
  })
})
