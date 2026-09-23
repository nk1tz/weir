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
  return new WebhookSink({ url: URL_, secret: SECRET, timeoutMs: 1000, ...over })
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

describe('WebhookSink.send — ONE attempt, never throws', () => {
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

  it('POSTs the exact JSON body once, signed with the current time, content-type json, redirect:manual → {ok:true}', async () => {
    const fetchMock = stubFetch(() => ok(200))

    await expect(mkSink().send(EVENT)).resolves.toEqual({ ok: true })

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

  it.each([200, 201, 204, 299])('HTTP %i is delivered', async (status) => {
    const fetchMock = stubFetch(() => ok(status))
    await expect(mkSink().send(EVENT)).resolves.toEqual({ ok: true })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it.each([300, 301, 400, 404, 500, 503])('HTTP %i is a failed attempt naming the status — no retry here', async (status) => {
    const fetchMock = stubFetch(() => ok(status))
    await expect(mkSink().send(EVENT)).resolves.toEqual({ ok: false, error: `HTTP ${status}` })
    await vi.runAllTimersAsync() // a retry loop would fire again; it must not
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('a rejected fetch (network error) is a failed attempt naming the error', async () => {
    const fetchMock = stubFetch(() => Promise.reject(new Error('connect ECONNREFUSED 10.0.0.1:443')))
    await expect(mkSink().send(EVENT)).resolves.toEqual({ ok: false, error: 'connect ECONNREFUSED 10.0.0.1:443' })
    await vi.runAllTimersAsync()
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('follows .cause so undici\'s "fetch failed" names the real network error', async () => {
    stubFetch(() => Promise.reject(new TypeError('fetch failed', { cause: new Error('getaddrinfo ENOTFOUND hook.test') })))
    await expect(mkSink().send(EVENT)).resolves.toEqual({ ok: false, error: 'fetch failed: getaddrinfo ENOTFOUND hook.test' })
  })

  it('a fetch that never resolves is aborted after timeoutMs and reported as a timeout', async () => {
    const signals: AbortSignal[] = []
    stubFetch(
      (_url, init) =>
        new Promise((_resolve, reject) => {
          signals.push(init.signal)
          init.signal.addEventListener('abort', () => reject(new DOMException('This operation was aborted', 'AbortError')))
        }),
    )

    const p = mkSink({ timeoutMs: 1000 }).send(EVENT)
    await vi.advanceTimersByTimeAsync(999)
    expect(signals[0]!.aborted).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    expect(signals[0]!.aborted).toBe(true)
    await expect(p).resolves.toEqual({ ok: false, error: 'timeout after 1000ms' })
  })

  it('the abort timer is cleared after a fast response (no stray abort later)', async () => {
    const signals: AbortSignal[] = []
    stubFetch((_url, init) => {
      signals.push(init.signal)
      return ok(200)
    })
    await mkSink({ timeoutMs: 1000 }).send(EVENT)
    await vi.advanceTimersByTimeAsync(5000)
    expect(signals[0]!.aborted).toBe(false)
  })

  it('never throws: a synchronously throwing fetch is a failed attempt', async () => {
    stubFetch(() => {
      throw new Error('sync boom')
    })
    await expect(mkSink().send(EVENT)).resolves.toEqual({ ok: false, error: 'sync boom' })
  })

  it('never throws: an unserializable event fails without a request', async () => {
    const fetchMock = stubFetch(() => ok(200))
    const bad = { ...EVENT, confs: 1n as unknown as number } // BigInt: JSON.stringify throws

    const res = await mkSink().send(bad)
    expect(res.ok).toBe(false)
    expect((res as { error: string }).error).toMatch(/^serialize failed: /)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('cancels the unread response body so the socket is released; a rejecting cancel still counts as delivered', async () => {
    const cancel = vi.fn(() => Promise.resolve())
    stubFetch(() => ({ status: 200, body: { cancel } }))
    await expect(mkSink().send(EVENT)).resolves.toEqual({ ok: true })
    expect(cancel).toHaveBeenCalledTimes(1)

    stubFetch(() => ({ status: 200, body: { cancel: () => Promise.reject(new Error('cancel boom')) } }))
    await expect(mkSink().send(EVENT)).resolves.toEqual({ ok: true })
    await vi.runAllTimersAsync()
  })

  it('each send is signed with the clock at that moment, and every signature verifies for its own t', async () => {
    const headers: string[] = []
    stubFetch((_url, init) => {
      headers.push(init.headers['x-weir-signature']!)
      return ok(500)
    })
    const sink = mkSink()
    await sink.send(EVENT)
    vi.setSystemTime(T0_MS + 10_000)
    await sink.send(EVENT)

    expect(headers).toHaveLength(2)
    const ts = headers.map(tOf)
    expect(ts[1]! - ts[0]!).toBe(10)
    for (const h of headers) expect(verifySignature(SECRET, BODY, h, 300, tOf(h))).toBe(true)
    expect(verifySignature(SECRET, BODY, headers[0]!, 0, tOf(headers[1]!))).toBe(false)
  })
})
