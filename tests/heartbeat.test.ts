import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { startHeartbeat } from '../src/engine/heartbeat'
import type { HeartbeatEvent, TxEvent } from '../src/lib/types'
import { FakeSink, FakeStore } from './fakes'

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
    const sink = new FakeSink()
    const hb = startHeartbeat({ cfg: { network: 'regtest', heartbeatInterval: interval }, store, sink })
    stops.push(hb.stop)
    return { store, sink, hb }
  }

  it('sends the documented payload DIRECTLY via the sink every interval — including the outbox fields', async () => {
    const { store, sink } = setup(5)
    store.tip = { hash: 'b100', height: 100 }
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
  })

  it('empty outbox → outboxDepth 0, outboxOldestAgeSec null, deadLetterCount 0; no maxmemory → memoryUsedPct null', async () => {
    const { store, sink } = setup(1)
    store.memory = { usedBytes: 10, maxBytes: null }

    await vi.advanceTimersByTimeAsync(1000)

    expect(sink.attempts[0]).toMatchObject({ tipHeight: null, watchCount: 0, memoryUsedPct: null, outboxDepth: 0, outboxOldestAgeSec: null, deadLetterCount: 0 })
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
