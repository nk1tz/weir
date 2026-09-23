/**
 * Periodic proof-of-life (dead-man's switch): every heartbeatInterval seconds, send a
 * heartbeat event built from tip height, watch count, redis memory usage and the outbox.
 *
 * It bypasses the outbox on purpose: proof-of-life must reflect NOW, and a stale queued
 * heartbeat carries no information. One direct attempt per tick; a failed send is warned
 * and the next interval is the retry. It REPORTS the outbox (depth, oldest age, dead
 * count) so a consumer can tell "weir is alive but my endpoint has been rejecting events".
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
  store: Pick<Store, 'getTip' | 'watchCount' | 'memoryInfo' | 'outboxStats'>
  sink: Sink
}

export function startHeartbeat(deps: HeartbeatDeps): { stop(): void } {
  const intervalSec = deps.cfg.heartbeatInterval
  if (intervalSec === 0) {
    log.info(CTX, 'heartbeat disabled (HEARTBEAT_INTERVAL=0)')
    return { stop() {} }
  }

  async function tick(): Promise<void> {
    const [tip, watchCount, mem, outbox] = await Promise.all([
      deps.store.getTip(),
      deps.store.watchCount(),
      deps.store.memoryInfo(),
      deps.store.outboxStats(),
    ])
    const now = Date.now()
    const ev: HeartbeatEvent = {
      version: 1,
      event: 'heartbeat',
      network: deps.cfg.network,
      tipHeight: tip?.height ?? null,
      watchCount,
      memoryUsedPct: mem.maxBytes !== null && mem.maxBytes > 0 ? Math.round((mem.usedBytes / mem.maxBytes) * 100) : null,
      outboxDepth: outbox.depth,
      outboxOldestAgeSec: outbox.oldestCreatedAt === null ? null : Math.max(0, Math.floor((now - outbox.oldestCreatedAt) / 1000)),
      deadLetterCount: outbox.dead,
      idempotencyKey: idem.heartbeat(deps.cfg.network, now),
      timestamp: now,
    }
    const result = await deps.sink.send(ev)
    if (!result.ok) log.warn(CTX, `heartbeat delivery failed: ${result.error} — next interval retries`)
  }

  const timer = setInterval(() => {
    // A failed send is a result (warned above); a rejected tick is an unexpected internal
    // error (redis down, etc) → fatal, never swallowed.
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
