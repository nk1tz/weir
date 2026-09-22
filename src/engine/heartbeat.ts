/**
 * Periodic proof-of-life (dead-man's switch): every heartbeatInterval seconds, deliver a
 * heartbeat event built from tip height, watch count, and redis memory usage.
 *
 * Spec: docs/DESIGN.md "src/engine/heartbeat.ts".
 */
import type { HeartbeatEvent, Network } from '../lib/types'
import type { Store } from '../store/redis'
import type { Sink } from '../delivery/webhook'
import { idem } from '../store/keys'
import { fatal, log } from '../lib/log'

const CTX = 'heartbeat'

export interface HeartbeatDeps {
  cfg: { network: Network; heartbeatInterval: number }
  store: Pick<Store, 'getTip' | 'watchCount' | 'memoryInfo'>
  sink: Sink
}

export function startHeartbeat(deps: HeartbeatDeps): { stop(): void } {
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
    // A failed delivery is a boolean (warned above); a rejected tick is an unexpected
    // internal error (redis down, etc) → fatal, never swallowed.
    tick().catch((err: unknown) => fatal(CTX, err))
  }, intervalSec * 1000)
  timer.unref()
  log.info(CTX, `heartbeat every ${intervalSec}s`)

  return {
    stop() {
      clearInterval(timer)
    },
  }
}
