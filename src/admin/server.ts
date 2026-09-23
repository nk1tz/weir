import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { createHash, timingSafeEqual } from 'node:crypto'
import type { Network } from '../lib/types'
import type { Store } from '../store/redis'
import { describeError, fatal, log } from '../lib/log'

const MAX_BODY_BYTES = 4096
/** ttl upper bound: 10 years in seconds */
const MAX_TTL_SECONDS = 10 * 365 * 24 * 3600

export interface AdminDeps {
  store: Pick<
    Store,
    'addWatch' | 'removeWatch' | 'isWatched' | 'scanWatches' | 'watchCount' | 'getTip' | 'getExpiry' | 'outboxStats'
  >
  isValidAddress(address: string): boolean
  /** cheap RPC liveness probe (e.g. getBlockCount); must reject when bitcoind is unreachable */
  rpcPing(): Promise<void>
  /** unix ms of the last block weir processed, null before the first block */
  lastBlockAtMs(): number | null
  config: {
    network: Network
    adminToken: string | null
    adminPort: number
    /** seconds; 0 = watch forever */
    watchDefaultTtl: number
  }
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

export function startAdminServer(deps: AdminDeps): { close(): Promise<void>; port: Promise<number> } {
  const token = requireAdminToken(deps.config.adminToken)

  async function handleHealth(res: ServerResponse): Promise<void> {
    let redisOk = true
    let rpcOk = true
    let tipHeight: number | null = null
    let watchCount = 0
    let outboxDepth = 0
    let outboxOldestAgeSec: number | null = null
    let deadLetterCount = 0
    try {
      const tip = await deps.store.getTip()
      tipHeight = tip?.height ?? null
      watchCount = await deps.store.watchCount()
      const outbox = await deps.store.outboxStats()
      outboxDepth = outbox.depth
      outboxOldestAgeSec =
        outbox.oldestCreatedAt === null ? null : Math.max(0, Math.floor((Date.now() - outbox.oldestCreatedAt) / 1000))
      deadLetterCount = outbox.dead
    } catch (err) {
      redisOk = false
      log.warn('admin', `health: redis check failed: ${describeError(err)}`)
    }
    try {
      await deps.rpcPing()
    } catch (err) {
      rpcOk = false
      log.warn('admin', `health: rpc check failed: ${describeError(err)}`)
    }
    const lastBlockAtMs = deps.lastBlockAtMs()
    const secondsSinceLastBlock =
      lastBlockAtMs === null ? null : Math.max(0, Math.floor((Date.now() - lastBlockAtMs) / 1000))
    // `ok` is still redis && rpc: outbox depth is a signal for the operator, not a readiness
    // failure — the daemon is healthy when the consumer's endpoint is the thing that is down.
    const ok = redisOk && rpcOk
    sendJson(res, ok ? 200 : 503, {
      ok,
      redis: redisOk,
      rpc: rpcOk,
      tipHeight,
      secondsSinceLastBlock,
      watchCount,
      outboxDepth,
      outboxOldestAgeSec,
      deadLetterCount,
    })
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

    if (method === 'GET' && url.pathname === '/health') {
      await handleHealth(res)
      return
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

  // A server-level error (listen failure such as EADDRINUSE) has no request to answer with
  // a 500 — it is a background failure, so it takes the one fatal exit path. Per-request
  // failures are answered 500 above and never crash the daemon.
  server.on('error', (err) => fatal('admin', err))

  // Resolves with the BOUND port once listening (adminPort 0 = ephemeral, used by tests).
  // Never rejects: a listen failure surfaces through the server 'error' handler above.
  const port = new Promise<number>((resolve) => {
    server.listen(deps.config.adminPort, () => {
      const addr = server.address()
      const bound = typeof addr === 'object' && addr !== null ? addr.port : deps.config.adminPort
      log.info('admin', `listening on :${bound}`)
      resolve(bound)
    })
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
