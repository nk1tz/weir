import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { startHeartbeat } from '../src/engine/heartbeat'
import type { HeartbeatEvent, TxEvent } from '../src/lib/types'
import { FakeChain, FakeSink, FakeStore } from './fakes'

const T0 = 1_700_000_000_000

const queued: TxEvent = {
  version: 1,
  event: 'dropped',
  network: 'regtest',
  txid: 'tx1',
  confs: 0,
  matched: [],
  blockHeight: null,
  blockHash: null,
  hex: '',
  idempotencyKey: 'regtest:tx1:dropped:1',
  timestamp: T0,
}

describe('heartbeat', () => {
  let stops: Array<() => void> = []
  let warnLog: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(T0)
    vi.spyOn(console, 'log').mockImplementation(() => {})
    warnLog = vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
  })

  afterEach(() => {
    for (const s of stops.splice(0)) s()
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  function setup(interval = 5) {
    const store = new FakeStore()
    const chain = new FakeChain()
    const sink = new FakeSink()
    const hb = startHeartbeat({ cfg: { network: 'regtest', heartbeatInterval: interval }, store, rpc: chain.rpc(), sink })
    stops.push(hb.stop)
    return { store, chain, sink, hb }
  }

  it('sends the documented payload DIRECTLY via the sink every interval — including the outbox fields, nodeHeight and chainLag', async () => {
    const { store, chain, sink } = setup(5)
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 101
    store.watches.add('bcrt1qa')
    store.watches.add('bcrt1qb')
    store.memory = { usedBytes: 64 * 1024 * 1024, maxBytes: 256 * 1024 * 1024 }
    store.enqueue(queued, T0 - 30_000)
    store.enqueue({ ...queued, txid: 'tx2', idempotencyKey: 'regtest:tx2:dropped:1' }, T0 - 5_000)
    store.outboxDeadSet.set('dead-1', T0 - 1)
    store.outbox.set('dead-1', { event: queued, attempts: 50, createdAt: T0 - 4 * 86_400_000, lastError: 'HTTP 503' })

    await vi.advanceTimersByTimeAsync(4999)
    expect(sink.attempts).toHaveLength(0)
    await vi.advanceTimersByTimeAsync(1) // the fake clock is now exactly T0 + 5000

    expect(sink.attempts).toHaveLength(1)
    const ev = sink.attempts[0] as HeartbeatEvent
    expect(ev).toEqual({
      version: 1,
      event: 'heartbeat',
      network: 'regtest',
      tipHeight: 100,
      nodeHeight: 101,
      chainLag: 1,
      watchCount: 2,
      memoryUsedPct: 25,
      outboxDepth: 2,
      outboxOldestAgeSec: 35, // oldest createdAt is 30s before T0; the tick ran 5s after T0
      deadLetterCount: 1,
      idempotencyKey: `regtest:heartbeat:${T0 + 5000}`,
      timestamp: T0 + 5000,
    })
    // it never went through the outbox — depth is still the two queued tx events
    expect(store.outboxQueue.size).toBe(2)
    expect(store.outboxEvents().every((e) => e.event !== 'heartbeat')).toBe(true)

    await vi.advanceTimersByTimeAsync(5000)
    expect(sink.attempts).toHaveLength(2)
    expect(chain.getBlockCountCalls).toBe(2) // one getblockcount per tick
  })

  it('empty outbox → outboxDepth 0, outboxOldestAgeSec null, deadLetterCount 0; no maxmemory → memoryUsedPct null; no tip → chainLag null even with a nodeHeight', async () => {
    const { store, chain, sink } = setup(1)
    store.memory = { usedBytes: 10, maxBytes: null }
    chain.blockCount = 7

    await vi.advanceTimersByTimeAsync(1000)

    expect(sink.attempts[0]).toMatchObject({ tipHeight: null, nodeHeight: 7, chainLag: null, watchCount: 0, memoryUsedPct: null, outboxDepth: 0, outboxOldestAgeSec: null, deadLetterCount: 0 })
  })

  it('a getblockcount failure does not fail the tick: nodeHeight and chainLag are null, warned, the heartbeat still goes out', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never)
    const { store, chain, sink } = setup(1)
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCountError = new Error('bitcoind unreachable')

    await vi.advanceTimersByTimeAsync(1000)

    expect(sink.delivered).toHaveLength(1)
    expect(sink.delivered[0]).toMatchObject({ tipHeight: 100, nodeHeight: null, chainLag: null })
    expect(warnLog.mock.calls.some((c) => /getblockcount failed — nodeHeight\/chainLag null this tick: bitcoind unreachable/.test(String(c[0])))).toBe(true)
    expect(exit).not.toHaveBeenCalled()

    // the node comes back: the next tick reports the lag again
    chain.blockCountError = null
    chain.blockCount = 103
    await vi.advanceTimersByTimeAsync(1000)
    expect(sink.delivered[1]).toMatchObject({ tipHeight: 100, nodeHeight: 103, chainLag: 3 })
  })

  it('a failed send is warned with the sink error and retried on the next interval — never fatal', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never)
    const { sink } = setup(1)
    sink.deliverResult = false

    await vi.advanceTimersByTimeAsync(1000)

    expect(sink.attempts).toHaveLength(1)
    expect(sink.delivered).toHaveLength(0)
    expect(warnLog.mock.calls.some((c) => /heartbeat delivery failed: HTTP 503/.test(String(c[0])))).toBe(true)
    expect(exit).not.toHaveBeenCalled()

    sink.deliverResult = true
    await vi.advanceTimersByTimeAsync(1000)
    expect(sink.delivered).toHaveLength(1)
  })

  it('a rejected tick (store failure) is fatal', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never)
    const { store } = setup(1)
    store.outboxStats = async () => {
      throw new Error('redis went away')
    }

    await vi.advanceTimersByTimeAsync(1000)

    expect(exit).toHaveBeenCalledWith(1)
  })

  it('ticks never overlap: while one is in flight the interval skips (warned once per stall), then resumes', async () => {
    const { store, chain, sink } = setup(1)
    chain.blockCount = 100
    let release: (tip: { hash: string; height: number }) => void = () => {}
    store.getTip = () => new Promise((resolve) => (release = resolve))

    await vi.advanceTimersByTimeAsync(1000) // tick 1 starts, parked on getTip
    expect(sink.attempts).toHaveLength(0)
    await vi.advanceTimersByTimeAsync(1000) // skipped
    await vi.advanceTimersByTimeAsync(1000) // skipped
    expect(sink.attempts).toHaveLength(0)
    const skips = () => warnLog.mock.calls.filter((c) => /previous heartbeat tick still in flight/.test(String(c[0]))).length
    expect(skips()).toBe(1)

    release({ hash: 'b100', height: 100 })
    await vi.advanceTimersByTimeAsync(0)
    expect(sink.attempts).toHaveLength(1) // tick 1 completed, exactly one heartbeat — nothing piled up
    expect(sink.attempts[0]).toMatchObject({ tipHeight: 100, chainLag: 0 })

    store.getTip = FakeStore.prototype.getTip
    await vi.advanceTimersByTimeAsync(1000)
    expect(sink.attempts).toHaveLength(2)
    expect(skips()).toBe(1)
  })

  it('HEARTBEAT_INTERVAL=0 → disabled: nothing is ever sent', async () => {
    const { sink } = setup(0)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(sink.attempts).toHaveLength(0)
  })

  it('stop() ends the ticks', async () => {
    const { sink, hb } = setup(1)
    await vi.advanceTimersByTimeAsync(1000)
    expect(sink.attempts).toHaveLength(1)
    hb.stop()
    await vi.advanceTimersByTimeAsync(5000)
    expect(sink.attempts).toHaveLength(1)
  })
})
