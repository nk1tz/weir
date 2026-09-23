import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { request as httpRequest } from 'node:http'
import { startAdminServer } from '../src/admin/server'
import { metrics } from '../src/lib/metrics'
import type { Runtime, TxEvent } from '../src/lib/types'
import { FakeChain, FakeStore } from './fakes'

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

/** Parse an exposition body into {line → value} for the samples (TYPE lines dropped). */
function samples(text: string): Map<string, string> {
  const out = new Map<string, string>()
  for (const line of text.split('\n')) {
    if (line === '' || line.startsWith('#')) continue
    const sp = line.lastIndexOf(' ')
    out.set(line.slice(0, sp), line.slice(sp + 1))
  }
  return out
}

describe('admin server', () => {
  const store = new FakeStore()
  const chain = new FakeChain()
  const runtime: Runtime = { reconciled: true, shuttingDown: false, lastZmqTxAt: null, lastZmqBlockAt: null }
  let warnLog: ReturnType<typeof vi.spyOn>
  let port = 0
  let handle: { close(): Promise<void>; port: Promise<number> }

  beforeAll(async () => {
    vi.spyOn(console, 'log').mockImplementation(() => {})
    warnLog = vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation(() => {})
    handle = startAdminServer({
      store,
      rpc: chain.rpc(),
      runtime,
      isValidAddress: (a) => a.startsWith('bcrt1q'),
      config: { network: 'regtest', adminToken: TOKEN, adminPort: 0, watchDefaultTtl: 0, readyMaxLag: 2 },
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
          rpc: chain.rpc(),
          runtime,
          isValidAddress: () => true,
          config: { network: 'regtest', adminToken: TOKEN, adminPort: held, watchDefaultTtl: 0, readyMaxLag: 2 },
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
    store.outboxCreated.clear()
    store.outboxDeadSet.clear()
    store.tip = null
    store.memory = { usedBytes: 0, maxBytes: null }
    store.outboxStats = FakeStore.prototype.outboxStats
    chain.blockCount = null
    chain.blockCountError = null
    chain.getBlockCountCalls = 0
    runtime.reconciled = true
    runtime.shuttingDown = false
    runtime.lastZmqTxAt = null
    runtime.lastZmqBlockAt = null
    metrics.reset()
    warnLog.mockClear()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // ── /live ────────────────────────────────────────────────────────────────────────────

  it('GET /live is 200 {ok:true} without auth, consulting neither redis nor rpc', async () => {
    store.getTip = async () => {
      throw new Error('redis down')
    }
    chain.blockCountError = new Error('bitcoind down')
    try {
      const r = await call(port, 'GET', '/live')
      expect(r.status).toBe(200)
      expect(r.headers['content-type']).toBe('application/json')
      expect(r.json).toEqual({ ok: true })
      expect(chain.getBlockCountCalls).toBe(0)
    } finally {
      delete (store as { getTip?: unknown }).getTip
    }
  })

  it('GET /live is 503 {ok:false} during shutdown', async () => {
    runtime.shuttingDown = true
    const r = await call(port, 'GET', '/live')
    expect(r.status).toBe(503)
    expect(r.json).toEqual({ ok: false })
  })

  // ── /ready ───────────────────────────────────────────────────────────────────────────

  it('GET /ready needs no auth and returns the documented shape: 200 when redis, rpc, reconciled and lag ≤ READY_MAX_LAG', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 101 // lag 1: normal right after a block
    store.watches.add(GOOD)
    runtime.lastZmqTxAt = 1_700_000_000_000 - 4_000
    runtime.lastZmqBlockAt = 1_700_000_000_000 - 65_000
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_700_000_000_000)
    let r: Reply
    try {
      r = await call(port, 'GET', '/ready')
    } finally {
      now.mockRestore()
    }

    expect(r.status).toBe(200)
    expect(r.headers['content-type']).toBe('application/json')
    expect(r.json).toEqual({
      ok: true,
      redis: true,
      rpc: true,
      reconciled: true,
      shuttingDown: false,
      tipHeight: 100,
      nodeHeight: 101,
      chainLag: 1,
      watchCount: 1,
      outboxDepth: 0,
      outboxOldestAgeSec: null,
      deadLetterCount: 0,
      lastZmqTxAgeSec: 4,
      lastZmqBlockAgeSec: 65,
    })
    expect(chain.getBlockCountCalls).toBe(1) // one getblockcount per probe
  })

  it('GET /ready is exactly at the bound → 200; one block past it → 503 with the lag reported', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 102
    expect((await call(port, 'GET', '/ready')).status).toBe(200)

    chain.blockCount = 103
    const r = await call(port, 'GET', '/ready')
    expect(r.status).toBe(503)
    expect(r.json).toMatchObject({ ok: false, redis: true, rpc: true, reconciled: true, tipHeight: 100, nodeHeight: 103, chainLag: 3 })
  })

  it('GET /ready is 503 {reconciled:false} before boot reconciliation finishes, with every other field still reported', async () => {
    runtime.reconciled = false
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 100
    const r = await call(port, 'GET', '/ready')
    expect(r.status).toBe(503)
    expect(r.json).toMatchObject({ ok: false, redis: true, rpc: true, reconciled: false, tipHeight: 100, nodeHeight: 100, chainLag: 0 })
  })

  it('GET /ready is 503 with rpc:false, nodeHeight and chainLag null when getblockcount rejects', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCountError = new Error('bitcoind unreachable')
    const r = await call(port, 'GET', '/ready')
    expect(r.status).toBe(503)
    expect(r.json).toMatchObject({ ok: false, redis: true, rpc: false, reconciled: true, tipHeight: 100, nodeHeight: null, chainLag: null, outboxDepth: 0 })
    expect(warnLog.mock.calls.some((c) => /ready: rpc check failed: bitcoind unreachable/.test(String(c[0])))).toBe(true)
  })

  it('GET /ready is 503 with redis:false, tipHeight and chainLag null when a store read rejects', async () => {
    chain.blockCount = 100
    store.outboxStats = async () => {
      throw new Error('redis went away')
    }
    const r = await call(port, 'GET', '/ready')
    expect(r.status).toBe(503)
    expect(r.json).toMatchObject({ ok: false, redis: false, rpc: true, tipHeight: null, nodeHeight: 100, chainLag: null, watchCount: 0, outboxDepth: 0, outboxOldestAgeSec: null, deadLetterCount: 0 })
    expect(warnLog.mock.calls.some((c) => /ready: redis check failed: redis went away/.test(String(c[0])))).toBe(true)
  })

  it('GET /ready is 503 with chainLag null when weir has no tip yet (fresh boot) — an unmeasurable lag is not within bound', async () => {
    chain.blockCount = 100
    const r = await call(port, 'GET', '/ready')
    expect(r.status).toBe(503)
    expect(r.json).toMatchObject({ ok: false, redis: true, rpc: true, tipHeight: null, nodeHeight: 100, chainLag: null })
  })

  it('GET /ready is 503 {shuttingDown:true} once shutdown has begun — while /live is 503 too', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 100
    runtime.shuttingDown = true
    const r = await call(port, 'GET', '/ready')
    expect(r.status).toBe(503)
    expect(r.json).toMatchObject({ ok: false, redis: true, rpc: true, reconciled: true, shuttingDown: true, chainLag: 0 })
    expect((await call(port, 'GET', '/live')).status).toBe(503)
  })

  it('GET /ready reads the shutdown flag AFTER its async reads: a shutdown that begins mid-probe still yields 503', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 100
    const realStats = store.outboxStats
    store.outboxStats = async () => {
      runtime.shuttingDown = true // flips while the probe is awaiting redis
      return realStats.call(store)
    }
    const r = await call(port, 'GET', '/ready')
    expect(r.status).toBe(503)
    expect(r.json).toMatchObject({ ok: false, redis: true, rpc: true, shuttingDown: true })
  })

  it('GET /ready reports the outbox (depth, oldest age, dead count) and stays 200: a down consumer is not a readiness failure', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 100
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
      r = await call(port, 'GET', '/ready')
    } finally {
      now.mockRestore()
    }

    expect(r.status).toBe(200)
    expect(r.json).toMatchObject({ ok: true, outboxDepth: 2, outboxOldestAgeSec: 90, deadLetterCount: 2 })
  })

  it('GET /health is an alias of /ready: same status, same body, no secondsSinceLastBlock', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 101
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_700_000_000_000)
    let health: Reply
    let ready: Reply
    try {
      health = await call(port, 'GET', '/health')
      ready = await call(port, 'GET', '/ready')
    } finally {
      now.mockRestore()
    }
    expect(health.status).toBe(200)
    expect(health.json).toEqual(ready.json)
    expect(health.json).not.toHaveProperty('secondsSinceLastBlock')

    runtime.reconciled = false
    expect((await call(port, 'GET', '/health')).status).toBe(503)
  })

  // ── /metrics ─────────────────────────────────────────────────────────────────────────

  it('GET /metrics needs no auth, is Prometheus text 0.0.4, and carries the registry plus every scrape-time gauge', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCount = 102
    store.watches.add(GOOD)
    store.watches.add('bcrt1qsecond')
    store.memory = { usedBytes: 12_345, maxBytes: 256 * 1024 * 1024 }
    store.outboxDeadSet.set('dead-1', 1)
    const queued: TxEvent = {
      version: 1,
      event: 'seen',
      network: 'regtest',
      txid: 'tx1',
      confs: 0,
      matched: [],
      blockHeight: null,
      blockHash: null,
      hex: '',
      idempotencyKey: 'regtest:tx1:seen',
      timestamp: 1_700_000_000_000,
    }
    store.enqueue(queued, 1_700_000_000_000 - 30_000) // counts one seen enqueued (fake mirrors the Store)
    metrics.counters.inc('weir_blocks_processed_total')
    metrics.counters.inc('weir_blocks_processed_total')
    metrics.counters.inc('weir_reorgs_total')
    metrics.counters.inc('weir_webhook_deliveries_total', { result: 'ok' }, 3)
    metrics.counters.inc('weir_webhook_deliveries_total', { result: 'fail' })
    metrics.counters.inc('weir_events_dead_lettered_total')
    metrics.gauges.set('weir_last_zmq_tx_timestamp_seconds', 1_699_999_990)
    metrics.gauges.set('weir_last_zmq_block_timestamp_seconds', 1_699_999_900)
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_700_000_000_000)
    let r: Reply
    try {
      r = await call(port, 'GET', '/metrics')
    } finally {
      now.mockRestore()
    }

    expect(r.status).toBe(200)
    expect(r.headers['content-type']).toBe('text/plain; version=0.0.4; charset=utf-8')
    expect(r.text.endsWith('\n')).toBe(true)
    // one # TYPE per family
    const typeLines = r.text.split('\n').filter((l) => l.startsWith('# TYPE '))
    expect(new Set(typeLines).size).toBe(typeLines.length)
    expect(typeLines).toContain('# TYPE weir_events_enqueued_total counter')
    expect(typeLines).toContain('# TYPE weir_chain_lag gauge')

    const got = samples(r.text)
    expect(got.get('weir_up')).toBe('1')
    expect(got.get('weir_reconciled')).toBe('1')
    expect(got.get('weir_tip_height')).toBe('100')
    expect(got.get('weir_node_height')).toBe('102')
    expect(got.get('weir_chain_lag')).toBe('2')
    expect(got.get('weir_watch_count')).toBe('2')
    expect(got.get('weir_outbox_depth')).toBe('1')
    expect(got.get('weir_outbox_oldest_age_seconds')).toBe('30')
    expect(got.get('weir_dead_letter_count')).toBe('1')
    expect(got.get('weir_redis_memory_used_bytes')).toBe('12345')
    expect(got.get('weir_redis_memory_max_bytes')).toBe(String(256 * 1024 * 1024))
    expect(got.get('weir_last_zmq_tx_timestamp_seconds')).toBe('1699999990')
    expect(got.get('weir_last_zmq_block_timestamp_seconds')).toBe('1699999900')
    // counters reflect the registry, labelled families included
    expect(got.get('weir_events_enqueued_total{event="seen"}')).toBe('1')
    expect(got.get('weir_events_enqueued_total{event="confirmed"}')).toBe('0')
    expect(got.get('weir_webhook_deliveries_total{result="ok"}')).toBe('3')
    expect(got.get('weir_webhook_deliveries_total{result="fail"}')).toBe('1')
    expect(got.get('weir_events_dead_lettered_total')).toBe('1')
    expect(got.get('weir_blocks_processed_total')).toBe('2')
    expect(got.get('weir_reorgs_total')).toBe('1')
    // never addresses or txids
    expect(r.text).not.toContain(GOOD)
    expect(r.text).not.toContain('tx1')
  })

  it('GET /metrics: absent-when-unknown gauges — no tip, unlimited redis, no ZMQ message yet, empty outbox → oldest age 0', async () => {
    chain.blockCount = 5
    const r = await call(port, 'GET', '/metrics')
    expect(r.status).toBe(200)
    const got = samples(r.text)
    expect(got.has('weir_tip_height')).toBe(false)
    expect(got.has('weir_chain_lag')).toBe(false)
    expect(got.get('weir_node_height')).toBe('5')
    expect(got.has('weir_redis_memory_max_bytes')).toBe(false)
    expect(got.get('weir_redis_memory_used_bytes')).toBe('0')
    expect(got.has('weir_last_zmq_tx_timestamp_seconds')).toBe(false)
    expect(got.get('weir_outbox_oldest_age_seconds')).toBe('0')
    expect(got.get('weir_reconciled')).toBe('1')
  })

  it('GET /metrics never 500s: a failed scrape-time read only omits its own gauges (warned), weir_up stays 1', async () => {
    store.tip = { hash: 'b100', height: 100 }
    chain.blockCountError = new Error('bitcoind unreachable')
    store.outboxStats = async () => {
      throw new Error('redis hiccup')
    }
    runtime.reconciled = false
    const r = await call(port, 'GET', '/metrics')

    expect(r.status).toBe(200)
    const got = samples(r.text)
    expect(got.get('weir_up')).toBe('1')
    expect(got.get('weir_reconciled')).toBe('0')
    expect(got.get('weir_tip_height')).toBe('100') // the reads that worked are still there
    expect(got.get('weir_watch_count')).toBe('0')
    expect(got.has('weir_node_height')).toBe(false)
    expect(got.has('weir_chain_lag')).toBe(false)
    expect(got.has('weir_outbox_depth')).toBe(false)
    expect(got.has('weir_outbox_oldest_age_seconds')).toBe(false)
    expect(got.has('weir_dead_letter_count')).toBe(false)
    expect(got.get('weir_blocks_processed_total')).toBe('0') // the registry always renders
    const warned = warnLog.mock.calls.map((c) => String(c[0])).filter((l) => /metrics: scrape-time read\(s\) failed/.test(l))
    expect(warned).toHaveLength(1)
    expect(warned[0]).toMatch(/getBlockCount: bitcoind unreachable/)
    expect(warned[0]).toMatch(/outboxStats: redis hiccup/)
  })

  it('the probes are GET-only and unauthenticated; other methods on their paths are 401 like any route', async () => {
    for (const path of ['/live', '/ready', '/health', '/metrics']) {
      expect((await call(port, 'POST', path)).status, path).toBe(401)
      expect((await call(port, 'DELETE', path, { token: TOKEN })).status, path).toBe(404)
    }
  })

  // ── writes wait for reconciliation ───────────────────────────────────────────────────

  it('POST /watches and DELETE /watches/:address are 503 `not ready: reconciling` (retry-after: 1) until boot reconciliation is done; reads and probes stay available', async () => {
    runtime.reconciled = false
    store.watches.add('bcrt1qexisting')

    const post = await call(port, 'POST', '/watches', { token: TOKEN, body: JSON.stringify({ address: GOOD }) })
    expect(post.status).toBe(503)
    expect(post.json).toEqual({ error: 'not ready: reconciling' })
    expect(post.headers['retry-after']).toBe('1')
    expect(store.watches.has(GOOD)).toBe(false)

    const del = await call(port, 'DELETE', '/watches/bcrt1qexisting', { token: TOKEN })
    expect(del.status).toBe(503)
    expect(del.json).toEqual({ error: 'not ready: reconciling' })
    expect(store.watches.has('bcrt1qexisting')).toBe(true)

    // auth still comes first: an unauthenticated write in the window is 401, not 503
    expect((await call(port, 'POST', '/watches', { body: JSON.stringify({ address: GOOD }) })).status).toBe(401)
    // reads are fine
    expect((await call(port, 'GET', '/watches', { token: TOKEN })).status).toBe(200)
    expect((await call(port, 'GET', '/watches/bcrt1qexisting', { token: TOKEN })).status).toBe(200)
    expect((await call(port, 'GET', '/live')).status).toBe(200)
    expect((await call(port, 'GET', '/ready')).status).toBe(503)

    runtime.reconciled = true
    const again = await call(port, 'POST', '/watches', { token: TOKEN, body: JSON.stringify({ address: GOOD }) })
    expect(again.status).toBe(201)
    expect(store.watches.has(GOOD)).toBe(true)
    expect((await call(port, 'DELETE', '/watches/bcrt1qexisting', { token: TOKEN })).status).toBe(204)
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
