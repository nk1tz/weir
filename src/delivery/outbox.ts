/**
 * The outbox drainer — the ONLY place events leave the daemon (the heartbeat aside).
 * Spec: docs/DESIGN.md "Outbox (durable delivery)" and "src/delivery/outbox.ts".
 *
 * Every OUTBOX_POLL_MS it takes a batch of due ids, reads each hash and sends them
 * SERIALLY in score order: 2xx → ack; failure → attempts+1, lastError, then either a
 * rescheduled retry (exponential backoff + jitter, persisted as the ZSET score) or, once
 * the event is older than OUTBOX_MAX_AGE, the capped dead-letter set with an error log.
 * A dangling id (queue entry without a hash) is removed. The interval pass keeps taking
 * batches while they come back full (a backlog drains at line rate, not 50/s). EVERY pass
 * — interval or a direct drainOnce() — goes through one shared in-flight promise, so two
 * passes never run at once and never double-send. Store failures on the interval path are
 * fatal (background path); the sink never throws.
 */
import type { Store } from '../store/redis'
import type { Sink } from './webhook'
import { fatal, log } from '../lib/log'

const CTX = 'outbox'

/** Poll interval — a constant, not config: one second is the delivery latency floor. */
export const OUTBOX_POLL_MS = 1000
/** Due events taken per batch (ZRANGEBYSCORE … LIMIT 0 50). */
export const OUTBOX_BATCH = 50

const BACKOFF_BASE_MS = 1000
const BACKOFF_MAX_MS = 300_000
const JITTER_FRACTION = 0.25

export interface OutboxDrainerDeps {
  cfg: { outboxMaxAgeSec: number; outboxDeadMax: number }
  store: Pick<Store, 'outboxDue' | 'outboxRead' | 'outboxAck' | 'outboxRetry' | 'outboxDead'>
  sink: Sink
}

export interface OutboxDrainer {
  /**
   * One batch, serialized with every other pass (waits for an in-flight pass first).
   * Returns the number of events delivered. Store errors propagate (rejects).
   */
  drainOnce(): Promise<number>
  /** Clears the interval and waits for whatever pass is in flight (it stops between events). */
  stop(): Promise<void>
}

/** min(1000·2^(attempts−1), 300000) ms plus up to 25% jitter; `attempts` counts the failure just made (≥ 1). */
export function backoffMs(attempts: number, random: () => number = Math.random): number {
  const base = Math.min(BACKOFF_BASE_MS * 2 ** (Math.max(1, attempts) - 1), BACKOFF_MAX_MS)
  return base + Math.floor(random() * base * JITTER_FRACTION)
}

export function startOutboxDrainer(deps: OutboxDrainerDeps): OutboxDrainer {
  const { cfg, store, sink } = deps
  const maxAgeMs = cfg.outboxMaxAgeSec * 1000
  let inFlight: Promise<number> | null = null
  let stopping = false

  /** One batch of due ids, delivered serially. `full` = the batch hit OUTBOX_BATCH (more may be due). */
  async function batch(): Promise<{ delivered: number; full: boolean }> {
    const ids = await store.outboxDue(Date.now(), OUTBOX_BATCH)
    let delivered = 0
    for (const id of ids) {
      if (stopping) break
      const rec = await store.outboxRead(id)
      if (rec === null) {
        log.warn(CTX, `dangling outbox id ${id} (hash missing) — removing from the queue`)
        await store.outboxAck(id)
        continue
      }
      const result = await sink.send(rec.event)
      if (result.ok) {
        await store.outboxAck(id)
        delivered++
        continue
      }
      const attempts = rec.attempts + 1
      const now = Date.now()
      const ageMs = now - rec.createdAt
      if (ageMs >= maxAgeMs) {
        const applied = await store.outboxDead(id, now, attempts, result.error, cfg.outboxDeadMax)
        if (!applied) {
          log.warn(CTX, `outbox ${id} vanished before it could be dead-lettered (acked concurrently) — ignoring`)
          continue
        }
        log.error(
          CTX,
          `DEAD-LETTERED ${rec.event.event} ${rec.event.idempotencyKey} after ${attempts} attempt(s) over ` +
            `${Math.round(ageMs / 1000)}s (limit ${cfg.outboxMaxAgeSec}s): ${result.error}`,
        )
        continue
      }
      const delay = backoffMs(attempts)
      const applied = await store.outboxRetry(id, now + delay, attempts, result.error)
      if (!applied) {
        log.warn(CTX, `outbox ${id} vanished before it could be rescheduled (acked concurrently) — ignoring`)
        continue
      }
      log.warn(
        CTX,
        `delivery failed (attempt ${attempts}) for ${rec.event.event} ${rec.event.idempotencyKey}: ${result.error} — retry in ${delay}ms`,
      )
    }
    return { delivered, full: ids.length === OUTBOX_BATCH }
  }

  /** A pass: one batch, or (interval) batch after batch while they come back full. */
  async function pass(drainBacklog: boolean): Promise<number> {
    let delivered = 0
    for (;;) {
      const b = await batch()
      delivered += b.delivered
      if (!drainBacklog || !b.full || stopping) return delivered
    }
  }

  /**
   * Every pass goes through ONE in-flight promise: a caller that finds a pass running waits
   * for it to end (however it ended), then runs its own. Nothing ever overlaps.
   */
  function exclusive(drainBacklog: boolean): Promise<number> {
    const previous = inFlight ?? Promise.resolve(0)
    const run = () => pass(drainBacklog)
    const next = previous.then(run, run)
    inFlight = next
    const clear = (): void => {
      if (inFlight === next) inFlight = null
    }
    next.then(clear, clear)
    return next
  }

  const timer = setInterval(() => {
    if (inFlight !== null) return // a slow pass is still running — the next tick will see
    exclusive(true).catch((err: unknown) => fatal(CTX, err))
  }, OUTBOX_POLL_MS)

  log.info(
    CTX,
    `drainer started: poll ${OUTBOX_POLL_MS}ms, batch ${OUTBOX_BATCH}, max age ${cfg.outboxMaxAgeSec}s, dead cap ${cfg.outboxDeadMax}`,
  )

  return {
    drainOnce: () => exclusive(false),
    async stop(): Promise<void> {
      stopping = true
      clearInterval(timer)
      if (inFlight !== null) await inFlight.then(() => undefined, () => undefined)
      log.info(CTX, 'drainer stopped')
    },
  }
}
