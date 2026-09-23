import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { request as httpRequest } from 'node:http'
import { startAdminServer } from '../src/admin/server'
import type { TxEvent } from '../src/lib/types'
import { FakeStore } from './fakes'

const TOKEN = 'test-admin-token'
const GOOD = 'bcrt1qgoodaddress'

interface Reply {
  status: number
  headers: Record<string, string | string[] | undefined>
  text: string
  json: unknown
}

/** One real HTTP request against the ephemeral-port server. Rejects on a socket error (e.g. ECONNRESET). */
function call(
  port: number,
  method: string,
  path: string,
  opts: { token?: string; body?: string; headers?: Record<string, string> } = {},
): Promise<Reply> {
  return new Promise((resolve, reject) => {
    const headers: Record<string, string> = { ...(opts.headers ?? {}) }
    if (opts.token !== undefined) headers['authorization'] = `Bearer ${opts.token}`
    if (opts.body !== undefined) {
      headers['content-type'] = 'application/json'
      headers['content-length'] = String(Buffer.byteLength(opts.body))
    }
    const req = httpRequest({ host: '127.0.0.1', port, method, path, headers }, (res) => {
      const chunks: Buffer[] = []
      res.on('data', (c: Buffer) => chunks.push(c))
      res.on('error', reject)
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8')
        let json: unknown = undefined
        if (text.length > 0) {
          try {
            json = JSON.parse(text)
          } catch {
            json = undefined
          }
        }
        resolve({ status: res.statusCode ?? 0, headers: res.headers, text, json })
      })
    })
    req.on('error', reject)
    req.end(opts.body)
  })
}

describe('admin server', () => {
  const store = new FakeStore()
  let rpcOk = true
  let lastBlockAt: number | null = null
  let port = 0
  let handle: { close(): Promise<void>; port: Promise<number> }

  beforeAll(async () => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
    handle = startAdminServer({
      store,
      isValidAddress: (a) => a.startsWith('bcrt1q'),
      rpcPing: async () => {
        if (!rpcOk) throw new Error('bitcoind unreachable')
      },
      lastBlockAtMs: () => lastBlockAt,
      config: { network: 'regtest', adminToken: TOKEN, adminPort: 0, watchDefaultTtl: 0 },
    })
    // rejects with the real listen error (EADDRINUSE, ...) instead of hanging the whole file
    port = await handle.port
    expect(port).toBeGreaterThan(0)
  })

  it('a listen failure rejects `port` with the EADDRINUSE error and takes the (injected) fatal path', async () => {
    const { createServer: createNetServer } = await import('node:net')
    const holder = createNetServer()
    await new Promise<void>((resolve) => holder.listen(0, resolve)) // all interfaces, like the admin server
    const held = (holder.address() as { port: number }).port
    const onFatal = vi.fn()
    try {
      const clashing = startAdminServer(
        {
          store,
          isValidAddress: () => true,
          rpcPing: async () => {},
          lastBlockAtMs: () => null,
          config: { network: 'regtest', adminToken: TOKEN, adminPort: held, watchDefaultTtl: 0 },
        },
        onFatal,
      )
      await expect(clashing.port).rejects.toThrow(/EADDRINUSE/)
      expect(onFatal).toHaveBeenCalledTimes(1)
      expect(onFatal.mock.calls[0]![0]).toBe('admin')
      expect(String((onFatal.mock.calls[0]![1] as Error).message)).toMatch(/EADDRINUSE/)
    } finally {
      await new Promise<void>((resolve) => holder.close(() => resolve()))
    }
  })

  afterAll(async () => {
    await handle.close()
    vi.restoreAllMocks()
  })

  beforeEach(() => {
    store.watches.clear()
    store.expiries.clear()
    store.outbox.clear()
    store.outboxQueue.clear()
    store.outboxDeadSet.clear()
    store.tip = null
    rpcOk = true
    lastBlockAt = null
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('GET /health needs no auth and returns the documented shape', async () => {
    store.tip = { hash: 'b100', height: 100 }
    store.watches.add(GOOD)
    lastBlockAt = 1_700_000_000_000
    // Pin the clock the handler reads (not the timers — the socket stays real).
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_700_000_005_000)
    let r: Reply
    try {
      r = await call(port, 'GET', '/health')
    } finally {
      now.mockRestore()
    }

    expect(r.status).toBe(200)
    expect(r.headers['content-type']).toBe('application/json')
    expect(r.json).toEqual({
      ok: true,
      redis: true,
      rpc: true,
      tipHeight: 100,
      secondsSinceLastBlock: 5,
      watchCount: 1,
      outboxDepth: 0,
      outboxOldestAgeSec: null,
      deadLetterCount: 0,
    })
  })

  it('GET /health reports the outbox (depth, oldest age, dead count) and stays 200: a down consumer is not a readiness failure', async () => {
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
      timestamp: 1_700_000_000_000,
    }
    store.enqueue(queued, 1_700_000_000_000 - 90_000)
    store.enqueue({ ...queued, txid: 'tx2', idempotencyKey: 'regtest:tx2:dropped:1' }, 1_700_000_000_000 - 1_000)
    store.outboxDeadSet.set('dead-1', 1)
    store.outboxDeadSet.set('dead-2', 2)
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_700_000_000_000)
    let r: Reply
    try {
      r = await call(port, 'GET', '/health')
    } finally {
      now.mockRestore()
    }

    expect(r.status).toBe(200)
    expect(r.json).toMatchObject({ ok: true, outboxDepth: 2, outboxOldestAgeSec: 90, deadLetterCount: 2 })
  })

  it('GET /health is 503 with rpc:false when the rpc ping rejects', async () => {
    rpcOk = false

    const r = await call(port, 'GET', '/health')

    expect(r.status).toBe(503)
    expect(r.json).toMatchObject({ ok: false, redis: true, rpc: false, tipHeight: null, secondsSinceLastBlock: null, outboxDepth: 0 })
  })

  it('POST /watches without a token is 401', async () => {
    const r = await call(port, 'POST', '/watches', { body: JSON.stringify({ address: GOOD }) })
    expect(r.status).toBe(401)
    expect(r.json).toEqual({ error: 'unauthorized' })
    expect(store.watches.size).toBe(0)
  })

  it('POST /watches with a wrong token is 401', async () => {
    const r = await call(port, 'POST', '/watches', { token: 'nope', body: JSON.stringify({ address: GOOD }) })
    expect(r.status).toBe(401)
  })

  it('POST /watches valid → 201 with expiresAt null (no ttl, no default)', async () => {
    const r = await call(port, 'POST', '/watches', { token: TOKEN, body: JSON.stringify({ address: GOOD }) })

    expect(r.status).toBe(201)
    expect(r.json).toEqual({ address: GOOD, network: 'regtest', expiresAt: null })
    expect(store.watches.has(GOOD)).toBe(true)
    expect(store.expiries.has(GOOD)).toBe(false)
  })

  it('POST /watches with ttl → 201 with a numeric expiresAt = now + ttl seconds', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_700_000_000_000)

    const r = await call(port, 'POST', '/watches', { token: TOKEN, body: JSON.stringify({ address: GOOD, ttl: 3600 }) })

    expect(r.status).toBe(201)
    expect(r.json).toEqual({ address: GOOD, network: 'regtest', expiresAt: 1_700_000_000_000 + 3600 * 1000 })
    expect(store.expiries.get(GOOD)).toBe(1_700_000_000_000 + 3600 * 1000)
  })

  it('POST /watches with an invalid address is 422', async () => {
    const r = await call(port, 'POST', '/watches', { token: TOKEN, body: JSON.stringify({ address: 'not-an-address' }) })
    expect(r.status).toBe(422)
    expect(r.json).toEqual({ error: 'invalid regtest address' })
    expect(store.watches.size).toBe(0)
  })

  it('POST /watches with ttl out of range is 422', async () => {
    for (const ttl of [-1, 1.5, 10 * 365 * 24 * 3600 + 1, 'soon']) {
      const r = await call(port, 'POST', '/watches', { token: TOKEN, body: JSON.stringify({ address: GOOD, ttl }) })
      expect(r.status, `ttl=${String(ttl)}`).toBe(422)
      expect((r.json as { error: string }).error).toMatch(/^ttl must be/)
    }
    expect(store.watches.size).toBe(0)
  })

  it('POST /watches with a 5 KB body gets an HTTP 413 JSON response, not a connection reset', async () => {
    // Regression: readBody destroyed the socket on overflow before the 413 was written,
    // so clients saw ECONNRESET instead of a status code.
    const body = JSON.stringify({ address: GOOD, pad: 'x'.repeat(5 * 1024) })
    expect(Buffer.byteLength(body)).toBeGreaterThan(4096)

    const r = await call(port, 'POST', '/watches', { token: TOKEN, body })

    expect(r.status).toBe(413)
    expect(r.json).toEqual({ error: 'body exceeds 4096 byte limit' })
    expect(r.headers['connection']).toBe('close')
    expect(store.watches.size).toBe(0)
  })

  it('POST /watches with invalid JSON is 400', async () => {
    const r = await call(port, 'POST', '/watches', { token: TOKEN, body: '{nope' })
    expect(r.status).toBe(400)
    expect((r.json as { error: string }).error).toMatch(/^invalid JSON body/)
  })

  it('DELETE /watches/:address is 204 even when absent, and removes an existing watch', async () => {
    const absent = await call(port, 'DELETE', `/watches/${GOOD}`, { token: TOKEN })
    expect(absent.status).toBe(204)
    expect(absent.text).toBe('')

    store.watches.add(GOOD)
    store.expiries.set(GOOD, 1)
    const present = await call(port, 'DELETE', `/watches/${GOOD}`, { token: TOKEN })
    expect(present.status).toBe(204)
    expect(store.watches.has(GOOD)).toBe(false)
    expect(store.expiries.has(GOOD)).toBe(false)
  })

  it('GET /watches/:address is 404 when not watched, then 200 with expiresAt', async () => {
    const missing = await call(port, 'GET', `/watches/${GOOD}`, { token: TOKEN })
    expect(missing.status).toBe(404)
    expect(missing.json).toEqual({ error: 'not watched' })

    store.watches.add(GOOD)
    const forever = await call(port, 'GET', `/watches/${GOOD}`, { token: TOKEN })
    expect(forever.status).toBe(200)
    expect(forever.json).toEqual({ address: GOOD, watched: true, expiresAt: null })

    store.expiries.set(GOOD, 1_700_000_000_000)
    const ttld = await call(port, 'GET', `/watches/${GOOD}`, { token: TOKEN })
    expect(ttld.json).toEqual({ address: GOOD, watched: true, expiresAt: 1_700_000_000_000 })
  })

  it('GET /watches?cursor=abc is 400', async () => {
    // Regression: a non-numeric cursor reached SSCAN (parseInt → NaN) instead of being rejected.
    const r = await call(port, 'GET', '/watches?cursor=abc', { token: TOKEN })
    expect(r.status).toBe(400)
    expect((r.json as { error: string }).error).toMatch(/cursor/)
  })

  it('GET /watches lists the watch set with the SSCAN cursor', async () => {
    store.watches.add(GOOD)
    store.watches.add('bcrt1qsecond')

    const first = await call(port, 'GET', '/watches', { token: TOKEN })
    expect(first.status).toBe(200)
    expect(first.json).toEqual({ addresses: [GOOD, 'bcrt1qsecond'], cursor: '0' })

    const explicit = await call(port, 'GET', '/watches?cursor=0', { token: TOKEN })
    expect(explicit.status).toBe(200)
  })

  it('GET /watches paginates: following the cursor yields every address exactly once', async () => {
    const all = ['bcrt1qa', 'bcrt1qb', 'bcrt1qc', 'bcrt1qd', 'bcrt1qe']
    for (const a of all) store.watches.add(a)
    store.scanPageSize = 2

    const pages: string[][] = []
    let cursor = '0'
    do {
      const r = await call(port, 'GET', `/watches?cursor=${cursor}`, { token: TOKEN })
      expect(r.status).toBe(200)
      const page = r.json as { addresses: string[]; cursor: string }
      pages.push(page.addresses)
      cursor = page.cursor
    } while (cursor !== '0')

    expect(pages.map((p) => p.length)).toEqual([2, 2, 1])
    const seen = pages.flat()
    expect(seen).toHaveLength(all.length)
    expect(new Set(seen)).toEqual(new Set(all))
  })

  it('unknown routes are 404 (after auth)', async () => {
    expect((await call(port, 'GET', '/nope')).status).toBe(401)
    expect((await call(port, 'GET', '/nope', { token: TOKEN })).status).toBe(404)
    expect((await call(port, 'PUT', '/watches', { token: TOKEN })).status).toBe(404)
  })
})
