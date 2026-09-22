import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { WebhookSink, type WebhookSinkConfig } from '../src/delivery/webhook'
import { verifySignature } from '../src/lib/hmac'
import type { TxEvent } from '../src/lib/types'

const URL_ = 'https://hook.test/weir'
const SECRET = 'weir-test-secret'
const T0_MS = 1_700_000_000_000

const EVENT: TxEvent = {
  version: 1,
  event: 'seen',
  network: 'regtest',
  txid: 'ab'.repeat(32),
  confs: 0,
  matched: [{ address: 'bcrt1qwatched', vout: 0, valueSats: 5000 }],
  blockHeight: null,
  blockHash: null,
  hex: 'deadbeef',
  idempotencyKey: 'regtest:abab:seen',
  timestamp: T0_MS,
}
const BODY = JSON.stringify(EVENT)

type FetchInit = { method: string; headers: Record<string, string>; body: string; redirect: string; signal: AbortSignal }

function mkSink(over: Partial<WebhookSinkConfig> = {}): WebhookSink {
  return new WebhookSink({ url: URL_, secret: SECRET, maxAttempts: 3, timeoutMs: 1000, ...over })
}

/** Stub globalThis.fetch with `impl`; returns the mock for call inspection. */
function stubFetch(impl: (url: string, init: FetchInit) => unknown) {
  const mock = vi.fn(impl)
  vi.stubGlobal('fetch', mock)
  return mock
}

const ok = (status: number) => new Response(null, { status })

/** Parse `t=` out of an x-weir-signature header value. */
function tOf(header: string): number {
  const m = /t=(\d+)/.exec(header)
  if (!m) throw new Error(`no t in ${header}`)
  return Number(m[1])
}

describe('WebhookSink', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(T0_MS)
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('POSTs the exact JSON body once, signed, with content-type and redirect:manual', async () => {
    const fetchMock = stubFetch(() => ok(200))

    const delivered = await mkSink().deliver(EVENT)

    expect(delivered).toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0] as [string, FetchInit]
    expect(url).toBe(URL_)
    expect(init.method).toBe('POST')
    expect(init.body).toBe(BODY)
    expect(init.headers['content-type']).toBe('application/json')
    expect(init.redirect).toBe('manual')
    expect(init.signal).toBeInstanceOf(AbortSignal)
    const sig = init.headers['x-weir-signature']!
    expect(tOf(sig)).toBe(Math.floor(T0_MS / 1000))
    expect(verifySignature(SECRET, BODY, sig, 300, Math.floor(T0_MS / 1000))).toBe(true)
    expect(verifySignature('other-secret', BODY, sig, 300, Math.floor(T0_MS / 1000))).toBe(false)
  })

  it.each([200, 201, 204, 299])('HTTP %i counts as delivered (true) with a single attempt', async (status) => {
    const fetchMock = stubFetch(() => ok(status))
    expect(await mkSink().deliver(EVENT)).toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it.each([300, 400, 500])('HTTP %i retries up to maxAttempts, then false', async (status) => {
    const fetchMock = stubFetch(() => ok(status))

    const p = mkSink({ maxAttempts: 3 }).deliver(EVENT)
    await vi.runAllTimersAsync() // backoff sleeps between attempts
    expect(await p).toBe(false)

    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it('a rejected fetch (network error) retries, then false', async () => {
    const fetchMock = stubFetch(() => Promise.reject(new Error('connect ECONNREFUSED')))

    const p = mkSink({ maxAttempts: 4 }).deliver(EVENT)
    await vi.runAllTimersAsync()
    expect(await p).toBe(false)

    expect(fetchMock).toHaveBeenCalledTimes(4)
  })

  it('a failure followed by a 2xx returns true and stops retrying', async () => {
    const fetchMock = stubFetch(() => ok(200))
    fetchMock.mockImplementationOnce(() => Promise.reject(new Error('ECONNRESET')))
    fetchMock.mockImplementationOnce(() => ok(503))

    const p = mkSink({ maxAttempts: 5 }).deliver(EVENT)
    await vi.runAllTimersAsync()
    expect(await p).toBe(true)

    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it('backs off between attempts: 500ms base, x2 per attempt (jitter pinned to 0)', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0) // backoffs are exactly 500ms then 1000ms
    const fetchMock = stubFetch(() => ok(500))

    const p = mkSink({ maxAttempts: 3 }).deliver(EVENT)
    await vi.advanceTimersByTimeAsync(0)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(499)
    expect(fetchMock).toHaveBeenCalledTimes(1) // t=499: first backoff not elapsed
    await vi.advanceTimersByTimeAsync(1)
    expect(fetchMock).toHaveBeenCalledTimes(2) // t=500
    await vi.advanceTimersByTimeAsync(999)
    expect(fetchMock).toHaveBeenCalledTimes(2) // t=1499: second backoff (1000ms) not elapsed
    await vi.advanceTimersByTimeAsync(1)
    expect(fetchMock).toHaveBeenCalledTimes(3) // t=1500
    expect(await p).toBe(false)
  })

  it('a fetch that never resolves is aborted after timeoutMs — the attempt fails and is retried', async () => {
    const signals: AbortSignal[] = []
    const fetchMock = stubFetch(
      (_url, init) =>
        new Promise((_resolve, reject) => {
          signals.push(init.signal)
          init.signal.addEventListener('abort', () => reject(new DOMException('This operation was aborted', 'AbortError')))
        }),
    )

    const p = mkSink({ maxAttempts: 2, timeoutMs: 1000 }).deliver(EVENT)
    await vi.advanceTimersByTimeAsync(999)
    expect(signals[0]!.aborted).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    expect(signals[0]!.aborted).toBe(true) // per-request timeout fired
    await vi.runAllTimersAsync() // backoff, second attempt, its timeout
    expect(await p).toBe(false)

    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(signals.map((s) => s.aborted)).toEqual([true, true])
  })

  it('every attempt is re-signed with the current time (t differs) and each signature verifies', async () => {
    const headers: string[] = []
    stubFetch((_url, init) => {
      headers.push(init.headers['x-weir-signature']!)
      vi.setSystemTime(Date.now() + 10_000) // the clock moves on between attempts
      return ok(500)
    })

    const p = mkSink({ maxAttempts: 3 }).deliver(EVENT)
    await vi.runAllTimersAsync()
    expect(await p).toBe(false)

    expect(headers).toHaveLength(3)
    const ts = headers.map(tOf)
    expect(new Set(ts).size).toBe(3) // all different
    expect(ts[1]! - ts[0]!).toBeGreaterThanOrEqual(10)
    for (const h of headers) {
      expect(verifySignature(SECRET, BODY, h, 300, tOf(h))).toBe(true)
    }
    // the body is serialized once: the first signature does not verify the third's t
    expect(verifySignature(SECRET, BODY, headers[0]!, 0, tOf(headers[2]!))).toBe(false)
  })

  it('never throws: a synchronously throwing fetch is a failed attempt', async () => {
    const fetchMock = stubFetch(() => {
      throw new Error('sync boom')
    })

    const p = mkSink({ maxAttempts: 2 }).deliver(EVENT)
    await vi.runAllTimersAsync()
    await expect(p).resolves.toBe(false)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('never throws: an unserializable event returns false without a request', async () => {
    const fetchMock = stubFetch(() => ok(200))
    const bad = { ...EVENT, confs: 1n as unknown as number } // BigInt: JSON.stringify throws

    await expect(mkSink().deliver(bad)).resolves.toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('never throws: a response body whose cancel() rejects still counts as delivered', async () => {
    stubFetch(() => ({ status: 200, body: { cancel: () => Promise.reject(new Error('cancel boom')) } }))

    await expect(mkSink().deliver(EVENT)).resolves.toBe(true)
    await vi.runAllTimersAsync()
  })

  it('cancels the unread response body so the socket is released', async () => {
    const cancel = vi.fn(() => Promise.resolve())
    stubFetch(() => ({ status: 200, body: { cancel } }))

    await mkSink().deliver(EVENT)

    expect(cancel).toHaveBeenCalledTimes(1)
  })

  it('maxAttempts below 1 still makes exactly one attempt', async () => {
    const fetchMock = stubFetch(() => ok(500))

    const p = mkSink({ maxAttempts: 0 }).deliver(EVENT)
    await vi.runAllTimersAsync()
    expect(await p).toBe(false)

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})
