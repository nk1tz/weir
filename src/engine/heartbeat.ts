/**
 * Periodic proof-of-life (dead-man's switch): every heartbeatInterval seconds, deliver a
 * heartbeat event built from tip height, watch count, and redis memory usage.
 *
 * Spec: docs/DESIGN.md "src/engine/heartbeat.ts".
 */
import { HeartbeatEvent, Network, Tip, WeirEvent } from '../lib/types'
import { idem } from '../store/keys'

interface Log {
  info(ctx: string, msg: string): void
  warn(ctx: string, msg: string): void
  error(ctx: string, msg: string): void
}

const consoleLog: Log = {
  info: (ctx, msg) => console.log(`[info] [${ctx}] ${msg}`),
  warn: (ctx, msg) => console.warn(`[warn] [${ctx}] ${msg}`),
  error: (ctx, msg) => console.error(`[error] [${ctx}] ${msg}`),
}

const CTX = 'heartbeat'

export interface HeartbeatDeps {
  cfg: { network: Network; heartbeatInterval: number }
  store: {
    getTip(): Promise<Tip | null>
    watchCount(): Promise<number>
    memoryInfo(): Promise<{ usedBytes: number; maxBytes: number | null }>
  }
  sink: { deliver(event: WeirEvent): Promise<boolean> }
  log?: Log
}

export function startHeartbeat(deps: HeartbeatDeps): { stop(): void } {
  const log = deps.log ?? consoleLog
  const intervalSec = deps.cfg.heartbeatInterval
  if (intervalSec === 0) {
    log.info(CTX, 'heartbeat disabled (HEARTBEAT_INTERVAL=0)')
    return { stop() {} }
  }

  async function tick(): Promise<void> {
    const [tip, watchCount, mem] = await Promise.all([
      deps.store.getTip(),
      deps.store.watchCount(),
      deps.store.memoryInfo(),
    ])
    const now = Date.now()
    const ev: HeartbeatEvent = {
      version: 1,
      event: 'heartbeat',
      network: deps.cfg.network,
      tipHeight: tip?.height ?? null,
      watchCount,
      memoryUsedPct: mem.maxBytes !== null && mem.maxBytes > 0 ? Math.round((mem.usedBytes / mem.maxBytes) * 100) : null,
      idempotencyKey: idem.heartbeat(deps.cfg.network, now),
      timestamp: now,
    }
    const ok = await deps.sink.deliver(ev)
    if (!ok) log.warn(CTX, 'heartbeat delivery failed — next interval retries')
  }

  const timer = setInterval(() => {
    tick().catch((err) => {
      // Unexpected internal error (redis down, etc): log and crash — never swallow.
      // Rethrowing here becomes an unhandled rejection; docker restarts us safely.
      log.error(CTX, `heartbeat tick failed: ${err instanceof Error ? err.message : String(err)}`)
      throw err
    })
  }, intervalSec * 1000)
  timer.unref?.()
  log.info(CTX, `heartbeat every ${intervalSec}s`)

  return {
    stop() {
      clearInterval(timer)
    },
  }
}
