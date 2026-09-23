import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { RPC_PROBE_TIMEOUT_MS, RPC_TIMEOUT_MS, Rpc } from '../src/bitcoin/rpc'

/**
 * The per-call deadline, against a stubbed global fetch and fake timers: a node that never
 * answers (or never finishes the body) is a rejection at the deadline, not a hang; the
 * probe call uses the short deadline; an HTTP response is never retried.
 */
type FetchInit = { signal: AbortSignal; body: string }

interface Call {
  init: FetchInit
  /** fake-clock ms at which this attempt's signal aborted, null if it never did */
  abortedAt: number | null
}

const T0 = 1_700_000_000_000

describe('Rpc deadlines', () => {
  const calls: Call[] = []
  let fetchMock: ReturnType<typeof vi.fn>

  /** fetch that never resolves until its signal aborts (then rejects like undici does) */
  function neverAnswers(): void {
    fetchMock.mockImplementation((_url: string, init: FetchInit) => {
      const call: Call = { init, abortedAt: null }
      calls.push(call)
      return new Promise((_resolve, reject) => {
        init.signal.addEventListener('abort', () => {
          call.abortedAt = Date.now()
          reject(new DOMException('This operation was aborted', 'AbortError'))
        })
      })
    })
  }

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(T0)
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
    calls.length = 0
    fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('a request that never answers rejects at RPC_TIMEOUT_MS per attempt; 3 attempts with backoff bound the total', async () => {
    neverAnswers()
    const rpc = new Rpc('http://u:p@node:8332')
    const p = rpc.getBlockHash(1)
    const settled = p.then(
      () => 'resolved',
      (err: Error) => err.message,
    )

    await vi.advanceTimersByTimeAsync(RPC_TIMEOUT_MS - 1)
    expect(calls).toHaveLength(1)
    expect(calls[0]!.abortedAt).toBeNull()
    await vi.advanceTimersByTimeAsync(1)
    expect(calls[0]!.abortedAt).toBe(T0 + RPC_TIMEOUT_MS)

    await vi.advanceTimersByTimeAsync(250) // backoff → attempt 2
    expect(calls).toHaveLength(2)
    await vi.advanceTimersByTimeAsync(RPC_TIMEOUT_MS)
    await vi.advanceTimersByTimeAsync(500) // backoff → attempt 3
    expect(calls).toHaveLength(3)
    await vi.advanceTimersByTimeAsync(RPC_TIMEOUT_MS)

    expect(await settled).toMatch(/^rpc getblockhash failed after 3 attempts: timeout after 30000ms/)
    expect(fetchMock).toHaveBeenCalledTimes(3)
    expect(vi.getTimerCount()).toBe(0) // nothing left ticking
  })

  it('getBlockCount (the probe/heartbeat call) uses the short RPC_PROBE_TIMEOUT_MS deadline', async () => {
    neverAnswers()
    const rpc = new Rpc('http://u:p@node:8332')
    const settled = rpc.getBlockCount().then(
      () => 'resolved',
      (err: Error) => err.message,
    )

    await vi.advanceTimersByTimeAsync(RPC_PROBE_TIMEOUT_MS - 1)
    expect(calls[0]!.abortedAt).toBeNull()
    await vi.advanceTimersByTimeAsync(1)
    expect(calls[0]!.abortedAt).toBe(T0 + RPC_PROBE_TIMEOUT_MS)

    await vi.advanceTimersByTimeAsync(250 + RPC_PROBE_TIMEOUT_MS + 500 + RPC_PROBE_TIMEOUT_MS)
    expect(await settled).toMatch(/^rpc getblockcount failed after 3 attempts: timeout after 5000ms/)
    expect(RPC_PROBE_TIMEOUT_MS).toBeLessThan(RPC_TIMEOUT_MS)
  })

  it('the deadline covers the response body too: a body that never ends is a timeout', async () => {
    fetchMock.mockImplementation((_url: string, init: FetchInit) => {
      const call: Call = { init, abortedAt: null }
      calls.push(call)
      return Promise.resolve({
        status: 200,
        ok: true,
        text: () =>
          new Promise((_resolve, reject) => {
            init.signal.addEventListener('abort', () => {
              call.abortedAt = Date.now()
              reject(new DOMException('The operation was aborted', 'AbortError'))
            })
          }),
      })
    })
    const rpc = new Rpc('http://u:p@node:8332')
    const settled = rpc.getBlockCount().then(
      () => 'resolved',
      (err: Error) => err.message,
    )

    await vi.advanceTimersByTimeAsync(RPC_PROBE_TIMEOUT_MS)
    expect(calls[0]!.abortedAt).toBe(T0 + RPC_PROBE_TIMEOUT_MS)
    await vi.advanceTimersByTimeAsync(250 + RPC_PROBE_TIMEOUT_MS + 500 + RPC_PROBE_TIMEOUT_MS)
    expect(await settled).toMatch(/timeout after 5000ms/)
  })

  it('a prompt answer resolves with the result, clears its deadline timer, and the signal never aborts', async () => {
    fetchMock.mockImplementation((_url: string, init: FetchInit) => {
      calls.push({ init, abortedAt: null })
      return Promise.resolve({ status: 200, ok: true, text: async () => JSON.stringify({ result: 812_345, error: null, id: 1 }) })
    })
    const rpc = new Rpc('http://u:p@node:8332')

    await expect(rpc.getBlockCount()).resolves.toBe(812_345)
    expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(RPC_TIMEOUT_MS)
    expect(calls[0]!.init.signal.aborted).toBe(false)
  })

  it('an HTTP response is never retried: a JSON-RPC error rejects once, -5 maps to null where documented', async () => {
    fetchMock.mockImplementation(async () => ({
      status: 500,
      ok: false,
      text: async () => JSON.stringify({ result: null, error: { code: -5, message: 'No such mempool or blockchain transaction' }, id: 1 }),
    }))
    const rpc = new Rpc('http://u:p@node:8332')
    await expect(rpc.getRawTransactionVerbose('aa')).resolves.toBeNull()
    await expect(rpc.getBlockHeader('bb')).rejects.toThrow(/No such mempool/)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })
})
