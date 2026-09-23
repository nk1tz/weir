import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { OUTBOX_BATCH, OUTBOX_POLL_MS, backoffMs, startOutboxDrainer, type OutboxDrainer } from '../src/delivery/outbox'
import type { TxEvent, WeirEvent } from '../src/lib/types'
import { FakeSink, FakeStore } from './fakes'

const T0 = 1_700_000_000_000
const MAX_AGE_SEC = 3 * 24 * 3600

function ev(key: string): TxEvent {
  return {
    version: 1,
    event: 'dropped',
    network: 'regtest',
    txid: key,
    confs: 0,
    matched: [],
    blockHeight: null,
    blockHash: null,
    hex: '',
    idempotencyKey: `regtest:${key}:dropped:1`,
    timestamp: T0,
  }
}

const keys = (events: WeirEvent[]): string[] => events.map((e) => e.idempotencyKey)

describe('outbox drainer', () => {
  const started: OutboxDrainer[] = []
  let errorLog: ReturnType<typeof vi.spyOn>
  let warnLog: ReturnType<typeof vi.spyOn>

  function setup(cfg: Partial<{ outboxMaxAgeSec: number; outboxDeadMax: number }> = {}) {
    const store = new FakeStore()
    const sink = new FakeSink()
    const drainer = startOutboxDrainer({ cfg: { outboxMaxAgeSec: MAX_AGE_SEC, outboxDeadMax: 1000, ...cfg }, store, sink })
    started.push(drainer)
    return { store, sink, drainer }
  }

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(T0)
    vi.spyOn(console, 'log').mockImplementation(() => {})
    warnLog = vi.spyOn(console, 'warn').mockImplementation(() => {})
    errorLog = vi.spyOn(console, 'error').mockImplementation(() => {})
  })

  afterEach(async () => {
    for (const d of started.splice(0)) await d.stop()
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('backoffMs: min(1000·2^(attempts−1), 300000) plus up to 25% jitter', () => {
    expect(backoffMs(1, () => 0)).toBe(1000)
    expect(backoffMs(2, () => 0)).toBe(2000)
    expect(backoffMs(9, () => 0)).toBe(256_000)
    expect(backoffMs(10, () => 0)).toBe(300_000) // capped
    expect(backoffMs(30, () => 0)).toBe(300_000)
    expect(backoffMs(1, () => 0.999)).toBe(1249)
    expect(backoffMs(30, () => 0.999)).toBeLessThan(375_000)
    expect(backoffMs(0, () => 0)).toBe(1000) // defensive: attempts < 1 behaves as 1
  })

  it('delivers due events SERIALLY in score order (not insertion order), acks each, returns the count', async () => {
    const { store, sink, drainer } = setup()
    store.enqueue(ev('e1'), T0 - 3000)
    store.enqueue(ev('e2'), T0 - 1000)
    store.enqueue(ev('e3'), T0 - 2000)
    let inFlight = 0
    let maxInFlight = 0
    const send = sink.send.bind(sink)
    sink.send = async (e) => {
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      await Promise.resolve() // yield so an overlapping send would show up
      const r = await send(e)
      inFlight--
      return r
    }

    await expect(drainer.drainOnce()).resolves.toBe(3)

    expect(keys(sink.delivered)).toEqual(['regtest:e1:dropped:1', 'regtest:e3:dropped:1', 'regtest:e2:dropped:1'])
    expect(maxInFlight).toBe(1)
    expect(store.outboxQueue.size).toBe(0)
    expect(store.outbox.size).toBe(0) // acked: hash deleted too
  })

  it('leaves not-yet-due events alone', async () => {
    const { store, sink, drainer } = setup()
    store.enqueue(ev('later'), T0 + 500)

    await expect(drainer.drainOnce()).resolves.toBe(0)

    expect(sink.attempts).toHaveLength(0)
    expect(store.outboxQueue.size).toBe(1)
  })

  it('takes at most OUTBOX_BATCH (50) per pass', async () => {
    const { store, sink, drainer } = setup()
    for (let i = 0; i < 60; i++) store.enqueue(ev(`e${i}`), T0 - 60 + i)

    await expect(drainer.drainOnce()).resolves.toBe(OUTBOX_BATCH)
    expect(sink.delivered).toHaveLength(50)
    expect(store.outboxQueue.size).toBe(10)

    await expect(drainer.drainOnce()).resolves.toBe(10)
    expect(store.outboxQueue.size).toBe(0)
  })

  it('a failed attempt: attempts+1, lastError, rescheduled with backoff + jitter, not retried before it is due', async () => {
    const { store, sink, drainer } = setup()
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    sink.deliverResult = false
    const id = store.enqueue(ev('e1'), T0 - 10)

    await expect(drainer.drainOnce()).resolves.toBe(0)

    expect(store.outbox.get(id)).toMatchObject({ attempts: 1, lastError: 'HTTP 503' })
    expect(store.outbox.get(id)!.createdAt).toBe(T0 - 10) // the age clock is never reset
    expect(store.outboxQueue.get(id)).toBe(T0 + 1000 + 125) // 1000 base + 0.5 × 25% jitter
    expect(warnLog.mock.calls.some((c) => /attempt 1.*regtest:e1:dropped:1.*HTTP 503.*retry in 1125ms/.test(String(c[0])))).toBe(true)

    vi.setSystemTime(T0 + 1124)
    await drainer.drainOnce()
    expect(sink.attempts).toHaveLength(1) // not due yet

    vi.setSystemTime(T0 + 1125)
    await drainer.drainOnce()
    expect(sink.attempts).toHaveLength(2)
    expect(store.outbox.get(id)).toMatchObject({ attempts: 2 })
    expect(store.outboxQueue.get(id)).toBe(T0 + 1125 + 2000 + 250) // second failure: 2000 base

    // recovery: the endpoint comes back — delivered on the next due pass, hash gone
    sink.deliverResult = true
    vi.setSystemTime(T0 + 1125 + 2250)
    await expect(drainer.drainOnce()).resolves.toBe(1)
    expect(store.outbox.has(id)).toBe(false)
    expect(store.outboxQueue.has(id)).toBe(false)
  })

  it('backoff jitter stays within [base, base + 25%) across random draws', async () => {
    const { store, sink, drainer } = setup()
    sink.deliverResult = false
    const id = store.enqueue(ev('e1'), T0)
    for (let attempt = 1; attempt <= 12; attempt++) {
      vi.setSystemTime(store.outboxQueue.get(id)!)
      const now = Date.now()
      await drainer.drainOnce()
      const base = Math.min(1000 * 2 ** (attempt - 1), 300_000)
      const next = store.outboxQueue.get(id)!
      expect(next, `attempt ${attempt}`).toBeGreaterThanOrEqual(now + base)
      expect(next, `attempt ${attempt}`).toBeLessThan(now + base * 1.25)
    }
  })

  it('once older than OUTBOX_MAX_AGE a failed event is dead-lettered: out of the queue, hash kept, error log names the key', async () => {
    const { store, sink, drainer } = setup()
    sink.deliverResult = false
    const createdAt = T0 - MAX_AGE_SEC * 1000
    const id = store.enqueue(ev('old'), createdAt)
    store.outboxQueue.set(id, T0 - 1) // due now, after many retries
    store.outbox.get(id)!.attempts = 40

    await expect(drainer.drainOnce()).resolves.toBe(0)

    expect(store.outboxQueue.has(id)).toBe(false)
    expect(store.outboxCreated.has(id)).toBe(false) // no longer counts toward "oldest queued"
    expect(store.outboxDeadSet.get(id)).toBe(T0)
    expect(store.outbox.get(id)).toMatchObject({ attempts: 41, lastError: 'HTTP 503', createdAt })
    expect(errorLog.mock.calls.some((c) => /DEAD-LETTERED dropped regtest:old:dropped:1 after 41 attempt/.test(String(c[0])))).toBe(true)

    // a dead event is never retried
    vi.setSystemTime(T0 + 10 * 60_000)
    await drainer.drainOnce()
    expect(sink.attempts).toHaveLength(1)
  })

  it('an event one millisecond younger than the limit is retried, not dead-lettered', async () => {
    const { store, sink, drainer } = setup()
    sink.deliverResult = false
    const id = store.enqueue(ev('young'), T0 - MAX_AGE_SEC * 1000 + 1)
    store.outboxQueue.set(id, T0 - 1)

    await drainer.drainOnce()

    expect(store.outboxDeadSet.size).toBe(0)
    expect(store.outboxQueue.has(id)).toBe(true)
  })

  it('the dead set is capped at OUTBOX_DEAD_MAX: the oldest dead entries are dropped WITH their hashes', async () => {
    const { store, sink, drainer } = setup({ outboxDeadMax: 2 })
    sink.deliverResult = false
    const ids = ['a', 'b', 'c'].map((k) => store.enqueue(ev(k), T0 - MAX_AGE_SEC * 1000 - 1))
    for (const id of ids) store.outboxQueue.set(id, T0 - 1)

    await drainer.drainOnce()

    // all three died at T0; ties resolve by id, so the first enqueued is the one dropped
    expect([...store.outboxDeadSet.keys()]).toEqual([ids[1], ids[2]])
    expect(store.outbox.has(ids[0]!)).toBe(false)
    expect(store.outbox.has(ids[1]!)).toBe(true)
    expect(store.outboxQueue.size).toBe(0)
  })

  it('a dangling id (queue entry without a hash) is removed without a send', async () => {
    const { store, sink, drainer } = setup()
    store.outboxQueue.set('ghost', T0 - 1)
    store.enqueue(ev('real'), T0 - 1)

    await expect(drainer.drainOnce()).resolves.toBe(1)

    expect(store.outboxQueue.has('ghost')).toBe(false)
    expect(keys(sink.attempts)).toEqual(['regtest:real:dropped:1'])
    expect(warnLog.mock.calls.some((c) => /dangling outbox id ghost/.test(String(c[0])))).toBe(true)
  })

  it('drainOnce counts successes only', async () => {
    const { store, sink, drainer } = setup()
    sink.failWhen = (e) => e.idempotencyKey === 'regtest:bad:dropped:1'
    store.enqueue(ev('ok1'), T0 - 3)
    store.enqueue(ev('bad'), T0 - 2)
    store.enqueue(ev('ok2'), T0 - 1)

    await expect(drainer.drainOnce()).resolves.toBe(2)
    expect(store.outboxQueue.size).toBe(1)
  })

  it('the poll loop runs every OUTBOX_POLL_MS and never overlaps a slow pass', async () => {
    const { store, sink, drainer } = setup()
    store.enqueue(ev('slow'), T0 - 1)
    store.enqueue(ev('next'), T0 - 1)
    const due = vi.spyOn(store, 'outboxDue')
    let release!: () => void
    sink.send = () =>
      new Promise((resolve) => {
        release = () => resolve({ ok: true })
      })

    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS)
    expect(due).toHaveBeenCalledTimes(1) // pass 1 started, parked inside the first send

    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS * 5)
    expect(due).toHaveBeenCalledTimes(1) // five ticks later: still one pass — no overlap

    release() // first send resolves; the pass continues to the second event
    await vi.advanceTimersByTimeAsync(0)
    release()
    await vi.advanceTimersByTimeAsync(0)
    expect(store.outboxQueue.size).toBe(0)

    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS)
    expect(due).toHaveBeenCalledTimes(2) // the loop resumed once the pass ended
    await drainer.stop()
  })

  it('stop() waits for the in-flight pass, which stops between events', async () => {
    const { store, sink, drainer } = setup()
    const first = store.enqueue(ev('first'), T0 - 1)
    const second = store.enqueue(ev('second'), T0 - 1)
    let release!: () => void
    sink.send = () =>
      new Promise((resolve) => {
        release = () => resolve({ ok: true })
      })

    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS) // pass in flight, parked in send('first')
    let stopped = false
    const stopping = drainer.stop().then(() => {
      stopped = true
    })
    await vi.advanceTimersByTimeAsync(0)
    expect(stopped).toBe(false) // waiting on the in-flight send

    release()
    await stopping
    expect(stopped).toBe(true)
    expect(store.outboxQueue.has(first)).toBe(false) // 'first' was acked after its send
    expect(store.outboxQueue.has(second)).toBe(true) // 'second' was NOT started
    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS * 3)
    expect(store.outboxQueue.has(second)).toBe(true) // and the loop is gone
  })

  it('equal scores deliver in ENQUEUE order (monotonic ids): two events in the same millisecond', async () => {
    const { store, sink, drainer } = setup()
    store.enqueue(ev('first'), T0 - 1)
    store.enqueue(ev('second'), T0 - 1)
    store.enqueue(ev('third'), T0 - 1)

    await drainer.drainOnce()

    expect(keys(sink.delivered)).toEqual(['regtest:first:dropped:1', 'regtest:second:dropped:1', 'regtest:third:dropped:1'])
  })

  it('overlapping direct drainOnce() calls are serialized through one in-flight pass — never a double send', async () => {
    const { store, sink, drainer } = setup()
    store.enqueue(ev('only'), T0 - 1)
    let release!: () => void
    let started = 0
    const send = sink.send.bind(sink)
    sink.send = async (e) => {
      started++
      await new Promise<void>((r) => {
        release = r
      })
      return send(e)
    }

    const a = drainer.drainOnce()
    const b = drainer.drainOnce() // must wait for `a`, then find nothing due
    await vi.advanceTimersByTimeAsync(0)
    expect(started).toBe(1)
    release()
    await expect(a).resolves.toBe(1)
    await expect(b).resolves.toBe(0)
    expect(started).toBe(1)
    expect(sink.delivered).toHaveLength(1)
    expect(store.outboxQueue.size).toBe(0)
  })

  it('a direct drainOnce() during an interval pass waits for it (and the interval never runs during a direct call)', async () => {
    const { store, sink, drainer } = setup()
    store.enqueue(ev('e1'), T0 - 1)
    store.enqueue(ev('e2'), T0 - 1)
    const releases: Array<() => void> = []
    sink.send = () =>
      new Promise((resolve) => {
        releases.push(() => resolve({ ok: true }))
      })

    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS) // interval pass in flight, parked in e1
    const direct = drainer.drainOnce()
    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS * 2)
    expect(sink.attempts).toHaveLength(0) // FakeSink.send is replaced; count via releases
    expect(releases).toHaveLength(1) // nothing else started while the pass is parked
    releases[0]!()
    await vi.advanceTimersByTimeAsync(0)
    expect(releases).toHaveLength(2) // the SAME pass continues with e2
    releases[1]!()
    await expect(direct).resolves.toBe(0) // ran after the pass, found nothing due
    expect(store.outboxQueue.size).toBe(0)
  })

  it('the interval pass drains a backlog in one go: batches keep coming while they are full', async () => {
    const { store, sink, drainer } = setup()
    for (let i = 0; i < 120; i++) store.enqueue(ev(`e${i}`), T0 - 200 + i)
    const due = vi.spyOn(store, 'outboxDue')

    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS)

    expect(sink.delivered).toHaveLength(120)
    expect(store.outboxQueue.size).toBe(0)
    expect(due).toHaveBeenCalledTimes(3) // 50 + 50 + 20 (the third batch was not full → stop)
    await drainer.stop()
  })

  it('a direct drainOnce() is exactly one batch', async () => {
    const { store, sink, drainer } = setup()
    for (let i = 0; i < 120; i++) store.enqueue(ev(`e${i}`), T0 - 200 + i)
    await expect(drainer.drainOnce()).resolves.toBe(OUTBOX_BATCH)
    expect(sink.delivered).toHaveLength(50)
  })

  it('outboxRetry / outboxDead on a missing id are no-ops: nothing partial is ever created', async () => {
    const { store } = setup()
    await expect(store.outboxRetry('nope', T0 + 1000, 1, 'HTTP 503')).resolves.toBe(false)
    await expect(store.outboxDead('nope', T0, 1, 'HTTP 503', 1000)).resolves.toBe(false)
    expect(store.outbox.size).toBe(0)
    expect(store.outboxQueue.size).toBe(0)
    expect(store.outboxDeadSet.size).toBe(0)
  })

  it('a concurrent ack during a failed send leaves nothing behind (retry becomes a no-op, warned)', async () => {
    const { store, sink, drainer } = setup()
    const id = store.enqueue(ev('e1'), T0 - 1)
    sink.send = async () => {
      await store.outboxAck(id) // someone else delivered + acked it meanwhile
      return { ok: false, error: 'HTTP 503' }
    }

    await expect(drainer.drainOnce()).resolves.toBe(0)

    expect(store.outbox.has(id)).toBe(false)
    expect(store.outboxQueue.has(id)).toBe(false)
    expect(store.outboxCreated.has(id)).toBe(false)
    expect(warnLog.mock.calls.some((c) => /vanished before it could be rescheduled/.test(String(c[0])))).toBe(true)
  })

  it('outboxStats.oldestCreatedAt is EXACT: an old event rescheduled far ahead is still the oldest', async () => {
    const { store, sink, drainer } = setup()
    const old = store.enqueue(ev('old'), T0 - 600_000)
    sink.deliverResult = false
    for (let i = 0; i < 8; i++) {
      // keep failing until its next attempt is minutes away
      vi.setSystemTime(store.outboxQueue.get(old)!)
      await drainer.drainOnce()
    }
    const newer = store.enqueue(ev('newer'), Date.now())
    expect(store.outboxQueue.get(old)!).toBeGreaterThan(store.outboxQueue.get(newer)!) // old is due AFTER newer

    await expect(store.outboxStats()).resolves.toEqual({ depth: 2, oldestCreatedAt: T0 - 600_000, dead: 0 })
  })

  it('a store failure on the poll path is fatal; on a direct drainOnce it rejects', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never)
    const { store, drainer } = setup()
    store.outboxDue = async () => {
      throw new Error('redis went away')
    }

    await expect(drainer.drainOnce()).rejects.toThrow('redis went away')
    expect(exit).not.toHaveBeenCalled()

    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS)
    expect(exit).toHaveBeenCalledWith(1)
    expect(errorLog.mock.calls.some((c) => /\[outbox\] fatal: redis went away/.test(String(c[0])))).toBe(true)
  })

  it('the sink never throws — but if a fake did, the pass would be fatal rather than swallowed', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never)
    const { store, sink } = setup()
    store.enqueue(ev('e1'), T0 - 1)
    sink.send = async () => {
      throw new Error('sink bug')
    }

    await vi.advanceTimersByTimeAsync(OUTBOX_POLL_MS)
    expect(exit).toHaveBeenCalledWith(1)
  })
})
