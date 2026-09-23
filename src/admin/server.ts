/**
 * The admin HTTP API (node:http, no framework), constructed only when ADMIN_TOKEN is set.
 * Spec: docs/DESIGN.md "src/admin/server.ts" and "Health from chain lag".
 *
 * Two kinds of route on one port:
 * - the watch API (`/watches…`), bearer-token authenticated;
 * - the probes (`/live`, `/ready`, `/health` = `/ready`, `/metrics`), UNAUTHENTICATED — they
 *   are for the platform and expose counts only, never addresses or txids.
 *   `/live` depends on nothing but the process (a restart cannot fix a dependency).
 *   `/ready` = redis ok AND rpc ok AND reconciled AND chainLag ≤ READY_MAX_LAG AND not
 *   shutting down; the webhook/outbox never fails readiness (weir is healthy when the
 *   consumer is down).
 * Until boot reconciliation is done, the WRITE routes (POST /watches, DELETE /watches/:address)
 * answer 503 `not ready: reconciling` (+ `retry-after: 1`): a watch added during the boot
 * window could otherwise be paid and mined in a block that first-run reconcile then
 * initialises the tip PAST — never processed. Reads and probes stay available.
 *   `/metrics` renders the process-local registry plus gauges read at scrape time; a read
 *   that fails drops its gauges (warned) and never turns the scrape into a 500.
 */
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { createHash, timingSafeEqual } from 'node:crypto'
import type { Network, Runtime } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import { describeError, fatal, log } from '../lib/log'
import { type GaugeSample, metrics } from '../lib/metrics'

const MAX_BODY_BYTES = 4096
/** ttl upper bound: 10 years in seconds */
const MAX_TTL_SECONDS = 10 * 365 * 24 * 3600

export interface AdminDeps {
  store: Pick<
    Store,
    | 'addWatch'
    | 'removeWatch'
    | 'isWatched'
    | 'scanWatches'
    | 'watchCount'
    | 'getTip'
    | 'getExpiry'
    | 'outboxStats'
    | 'memoryInfo'
  >
  /** getBlockCount is both the rpc liveness probe and the node height for chainLag */
  rpc: Pick<Rpc, 'getBlockCount'>
  /** the daemon's live state (owned by index.ts); read only here */
  runtime: Readonly<Runtime>
  isValidAddress(address: string): boolean
  config: {
    network: Network
    adminToken: string | null
    adminPort: number
    /** seconds; 0 = watch forever */
    watchDefaultTtl: number
    /** /ready is 503 once nodeHeight − tipHeight exceeds this */
    readyMaxLag: number
  }
}

/** `text/plain; version=0.0.4` is what Prometheus asks for; charset makes curl output sane. */
const METRICS_CONTENT_TYPE = 'text/plain; version=0.0.4; charset=utf-8'

function sendText(res: ServerResponse, status: number, body: string, contentType: string): void {
  res.writeHead(status, { 'content-type': contentType, 'content-length': Buffer.byteLength(body) })
  res.end(body)
}

/** whole seconds since `atMs`, floored at 0; null when there is no timestamp */
function ageSec(nowMs: number, atMs: number | null): number | null {
  return atMs === null ? null : Math.max(0, Math.floor((nowMs - atMs) / 1000))
}

function sendJson(res: ServerResponse, status: number, payload: unknown, extraHeaders: Record<string, string> = {}): void {
  const body = JSON.stringify(payload)
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(body),
    ...extraHeaders,
  })
  res.end(body)
}

function authorized(req: IncomingMessage, token: string): boolean {
  const header = req.headers.authorization
  if (typeof header !== 'string') return false
  const match = /^Bearer\s+(.+)$/i.exec(header.trim())
  const provided = match?.[1]
  if (provided === undefined) return false
  // Hash both sides so timingSafeEqual gets equal-length inputs regardless of token length.
  const a = createHash('sha256').update(provided).digest()
  const b = createHash('sha256').update(token).digest()
  return timingSafeEqual(a, b)
}

/**
 * Resolves to the body buffer, or null when the body exceeds maxBytes. On overflow the
 * socket is deliberately NOT destroyed here — that would reset the connection before the
 * caller's 413 is written. Instead buffering stops (later chunks are drained and dropped)
 * and the caller answers 413 with `connection: close`; Node then ends the socket right
 * after the response flushes, which also cuts off an endless chunked upload.
 */
function readBody(req: IncomingMessage, maxBytes: number): Promise<Buffer | null> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    let total = 0
    let done = false
    req.on('data', (chunk: Buffer) => {
      if (done) return // overflowed: drain without buffering
      total += chunk.length
      if (total > maxBytes) {
        done = true
        chunks.length = 0
        resolve(null)
        return
      }
      chunks.push(chunk)
    })
    req.on('end', () => {
      if (done) return
      done = true
      resolve(Buffer.concat(chunks))
    })
    req.on('error', (err) => {
      if (done) return
      done = true
      reject(err)
    })
  })
}

/** The admin server must not be constructed without a token — fatal by design. */
function requireAdminToken(token: string | null): string {
  if (token === null) {
    throw new Error('startAdminServer called without ADMIN_TOKEN — the admin server must not be constructed')
  }
  return token
}

export function startAdminServer(
  deps: AdminDeps,
  onFatal: (ctx: string, err: unknown) => void = fatal,
): { close(): Promise<void>; port: Promise<number> } {
  const token = requireAdminToken(deps.config.adminToken)

  /** `/live`: up and not shutting down. No dependency is consulted — on purpose. */
  function handleLive(res: ServerResponse): void {
    const ok = !deps.runtime.shuttingDown
    sendJson(res, ok ? 200 : 503, { ok })
  }

  interface RedisView {
    tipHeight: number | null
    watchCount: number
    outboxDepth: number
    outboxOldestAgeSec: number | null
    deadLetterCount: number
  }

  /** The redis-backed fields of /ready in one round of reads; null when any of them failed. */
  async function readRedis(probe: string): Promise<RedisView | null> {
    try {
      const [tip, watchCount, outbox] = await Promise.all([deps.store.getTip(), deps.store.watchCount(), deps.store.outboxStats()])
      return {
        tipHeight: tip?.height ?? null,
        watchCount,
        outboxDepth: outbox.depth,
        outboxOldestAgeSec: ageSec(Date.now(), outbox.oldestCreatedAt),
        deadLetterCount: outbox.dead,
      }
    } catch (err) {
      log.warn('admin', `${probe}: redis check failed: ${describeError(err)}`)
      return null
    }
  }

  /** getblockcount — the rpc liveness probe AND the node height; null when bitcoind does not answer. */
  async function readNodeHeight(probe: string): Promise<number | null> {
    try {
      return await deps.rpc.getBlockCount()
    } catch (err) {
      log.warn('admin', `${probe}: rpc check failed: ${describeError(err)}`)
      return null
    }
  }

  /**
   * `/ready` (and its alias `/health`): ok = redis && rpc && reconciled && chainLag ≤
   * READY_MAX_LAG && !shuttingDown. chainLag = nodeHeight − tipHeight, null when either is
   * unknown (then not ready: a lag that cannot be measured is not a lag within bound). The
   * runtime flags are read AFTER the async reads, so a shutdown that begins mid-probe still
   * answers 503. The outbox fields are reported for the operator and never affect `ok`.
   */
  async function handleReady(res: ServerResponse): Promise<void> {
    const [redis, nodeHeight] = await Promise.all([readRedis('ready'), readNodeHeight('ready')])
    const tipHeight = redis?.tipHeight ?? null
    const chainLag = nodeHeight !== null && tipHeight !== null ? nodeHeight - tipHeight : null
    const { reconciled, shuttingDown } = deps.runtime // after the awaits — see above
    const ok =
      redis !== null && nodeHeight !== null && reconciled && !shuttingDown && chainLag !== null && chainLag <= deps.config.readyMaxLag
    const now = Date.now()
    sendJson(res, ok ? 200 : 503, {
      ok,
      redis: redis !== null,
      rpc: nodeHeight !== null,
      reconciled,
      shuttingDown,
      tipHeight,
      nodeHeight,
      chainLag,
      watchCount: redis?.watchCount ?? 0,
      outboxDepth: redis?.outboxDepth ?? 0,
      outboxOldestAgeSec: redis?.outboxOldestAgeSec ?? null,
      deadLetterCount: redis?.deadLetterCount ?? 0,
      lastZmqTxAgeSec: ageSec(now, deps.runtime.lastZmqTxAt),
      lastZmqBlockAgeSec: ageSec(now, deps.runtime.lastZmqBlockAt),
    })
  }

  /**
   * `/metrics`: the registry (counters + the ZMQ timestamp gauges pushed by zmq.ts) plus
   * the gauges that need I/O, read now. Each read is independent: one that fails drops
   * only its own gauges (one warn line names them) — `weir_up` stays 1 and the scrape is
   * still a 200, because a redis hiccup is exactly what the remaining series should show.
   */
  async function handleMetrics(res: ServerResponse): Promise<void> {
    const extra: GaugeSample[] = [
      { name: 'weir_up', value: 1 },
      { name: 'weir_reconciled', value: deps.runtime.reconciled ? 1 : 0 },
    ]
    const failed: string[] = []
    const reads = await Promise.allSettled([
      deps.store.getTip(),
      deps.rpc.getBlockCount(),
      deps.store.watchCount(),
      deps.store.outboxStats(),
      deps.store.memoryInfo(),
    ])
    const [tipRead, nodeRead, watchRead, outboxRead, memRead] = reads
    const take = <T>(name: string, r: PromiseSettledResult<T>): T | null => {
      if (r.status === 'fulfilled') return r.value
      failed.push(`${name}: ${describeError(r.reason)}`)
      return null
    }
    const tip = take('getTip', tipRead)
    const nodeHeight = take('getBlockCount', nodeRead)
    const watchCount = take('watchCount', watchRead)
    const outbox = take('outboxStats', outboxRead)
    const mem = take('memoryInfo', memRead)

    const tipHeight = tip?.height ?? null // absent on a failed read AND before the first tip
    if (tipHeight !== null) extra.push({ name: 'weir_tip_height', value: tipHeight })
    if (nodeHeight !== null) extra.push({ name: 'weir_node_height', value: nodeHeight })
    if (tipHeight !== null && nodeHeight !== null) extra.push({ name: 'weir_chain_lag', value: nodeHeight - tipHeight })
    if (watchCount !== null) extra.push({ name: 'weir_watch_count', value: watchCount })
    if (outbox !== null) {
      extra.push({ name: 'weir_outbox_depth', value: outbox.depth })
      // 0 when empty (not absent): the alert `> 300` and the graph stay continuous.
      extra.push({ name: 'weir_outbox_oldest_age_seconds', value: ageSec(Date.now(), outbox.oldestCreatedAt) ?? 0 })
      extra.push({ name: 'weir_dead_letter_count', value: outbox.dead })
    }
    if (mem !== null) {
      extra.push({ name: 'weir_redis_memory_used_bytes', value: mem.usedBytes })
      if (mem.maxBytes !== null) extra.push({ name: 'weir_redis_memory_max_bytes', value: mem.maxBytes })
    }
    if (failed.length > 0) log.warn('admin', `metrics: scrape-time read(s) failed, gauges omitted — ${failed.join('; ')}`)
    sendText(res, 200, metrics.render(extra), METRICS_CONTENT_TYPE)
  }

  async function handleCreateWatch(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const body = await readBody(req, MAX_BODY_BYTES)
    if (body === null) {
      // `connection: close` makes Node end the socket once this response has flushed
      // (see readBody) — the client reads a real 413 instead of ECONNRESET.
      sendJson(res, 413, { error: `body exceeds ${MAX_BODY_BYTES} byte limit` }, { connection: 'close' })
      return
    }
    let parsed: unknown
    try {
      parsed = JSON.parse(body.toString('utf8'))
    } catch (err) {
      sendJson(res, 400, { error: `invalid JSON body: ${describeError(err)}` })
      return
    }
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      sendJson(res, 400, { error: 'body must be a JSON object' })
      return
    }
    const { address, ttl } = parsed as { address?: unknown; ttl?: unknown }
    if (typeof address !== 'string' || address.length === 0) {
      sendJson(res, 422, { error: 'address must be a non-empty string' })
      return
    }
    if (!deps.isValidAddress(address)) {
      sendJson(res, 422, { error: `invalid ${deps.config.network} address` })
      return
    }
    if (ttl !== undefined && (typeof ttl !== 'number' || !Number.isInteger(ttl) || ttl < 0 || ttl > MAX_TTL_SECONDS)) {
      sendJson(res, 422, { error: `ttl must be an integer number of seconds between 0 and ${MAX_TTL_SECONDS}` })
      return
    }
    const effectiveTtl = (ttl as number | undefined) ?? deps.config.watchDefaultTtl
    const expiresAt = effectiveTtl > 0 ? Date.now() + effectiveTtl * 1000 : null
    await deps.store.addWatch(address, expiresAt ?? undefined)
    sendJson(res, 201, { address, network: deps.config.network, expiresAt })
  }

  async function handleGetWatch(res: ServerResponse, address: string): Promise<void> {
    const watched = await deps.store.isWatched(address)
    if (!watched) {
      sendJson(res, 404, { error: 'not watched' })
      return
    }
    const expiresAt = await deps.store.getExpiry(address)
    sendJson(res, 200, { address, watched: true, expiresAt })
  }

  async function handleDeleteWatch(res: ServerResponse, address: string): Promise<void> {
    await deps.store.removeWatch(address) // idempotent: 204 even if absent
    res.writeHead(204)
    res.end()
  }

  async function handleListWatches(res: ServerResponse, url: URL): Promise<void> {
    const cursor = url.searchParams.get('cursor') ?? '0'
    if (!/^\d+$/.test(cursor)) {
      sendJson(res, 400, { error: 'cursor must be a non-negative integer string (from a previous page, or "0")' })
      return
    }
    const page = await deps.store.scanWatches(cursor)
    sendJson(res, 200, { addresses: page.addresses, cursor: page.cursor })
  }

  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const method = req.method ?? 'GET'
    const url = new URL(req.url ?? '/', 'http://admin.internal')

    // Probes: unauthenticated, GET only (a POST /live falls through to 401 like any other route).
    if (method === 'GET') {
      switch (url.pathname) {
        case '/live':
          handleLive(res)
          return
        case '/ready':
        case '/health':
          await handleReady(res)
          return
        case '/metrics':
          await handleMetrics(res)
          return
      }
    }

    if (!authorized(req, token)) {
      sendJson(res, 401, { error: 'unauthorized' })
      return
    }

    const rawSegments = url.pathname.split('/').filter((s) => s.length > 0)
    let segments: string[]
    try {
      segments = rawSegments.map((s) => decodeURIComponent(s))
    } catch (err) {
      sendJson(res, 400, { error: `malformed percent-encoding in path: ${describeError(err)}` })
      return
    }

    if (segments[0] === 'watches') {
      // Writes wait for boot reconciliation (see module doc); reads never do.
      if ((method === 'POST' || method === 'DELETE') && !deps.runtime.reconciled) {
        sendJson(res, 503, { error: 'not ready: reconciling' }, { 'retry-after': '1' })
        return
      }
      if (segments.length === 1) {
        if (method === 'POST') {
          await handleCreateWatch(req, res)
          return
        }
        if (method === 'GET') {
          await handleListWatches(res, url)
          return
        }
      } else if (segments.length === 2) {
        const address = segments[1] as string
        if (method === 'GET') {
          await handleGetWatch(res, address)
          return
        }
        if (method === 'DELETE') {
          await handleDeleteWatch(res, address)
          return
        }
      }
    }

    sendJson(res, 404, { error: 'not found' })
  }

  const server = createServer((req, res) => {
    handle(req, res).catch((err: unknown) => {
      log.error('admin', `unhandled error on ${req.method} ${req.url}: ${describeError(err)}`)
      if (!res.headersSent) {
        sendJson(res, 500, { error: 'internal error' })
      } else {
        res.destroy()
      }
    })
  })

  // Resolves with the BOUND port once listening (adminPort 0 = ephemeral, used by tests);
  // REJECTS with the listen error (EADDRINUSE, EACCES, ...) when the server errors before it
  // is listening, so a caller awaiting the port fails fast with the real cause instead of
  // hanging. `onFatal` is injectable for tests only; production callers use the default.
  let listening = false
  let rejectPort!: (err: Error) => void
  const port = new Promise<number>((resolve, reject) => {
    rejectPort = reject
    server.listen(deps.config.adminPort, () => {
      listening = true
      const addr = server.address()
      const bound = typeof addr === 'object' && addr !== null ? addr.port : deps.config.adminPort
      log.info('admin', `listening on :${bound}`)
      resolve(bound)
    })
  })

  // A server-level error (listen failure such as EADDRINUSE) has no request to answer with
  // a 500 — it is a background failure, so it takes the one fatal exit path (after the port
  // promise has been rejected, so tests and boot code see the error). Per-request failures
  // are answered 500 above and never crash the daemon.
  server.on('error', (err) => {
    if (!listening) rejectPort(err)
    onFatal('admin', err)
  })

  return {
    port,
    close(): Promise<void> {
      return new Promise((resolve, reject) => {
        server.closeAllConnections()
        server.close((err) => {
          if (err) {
            log.error('admin', `close failed: ${describeError(err)}`)
            reject(err)
            return
          }
          log.info('admin', 'closed')
          resolve()
        })
      })
    },
  }
}
