import type { WeirEvent } from '../lib/types'
import { signBody } from '../lib/hmac'
import { describeError, log } from '../lib/log'

export interface WebhookSinkConfig {
  url: string
  secret: string
  /** total attempts (not extra retries); config field webhookMaxRetries / env WEBHOOK_MAX_RETRIES */
  maxAttempts: number
  /** per-request timeout, enforced via AbortController */
  timeoutMs: number
}

/** What every module needs from the sink — engine deps type `sink` as this. */
export type Sink = Pick<WebhookSink, 'deliver'>

const BACKOFF_BASE_MS = 500
const JITTER_MAX_MS = 250

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * POSTs signed events to the configured webhook URL.
 *
 * - Body is serialized exactly once; each attempt is re-signed with the current unix seconds.
 * - Retries up to `maxAttempts` attempts total, backoff 500ms * 2^n + jitter between attempts.
 * - `redirect: 'manual'` so a signed body is never re-POSTed to a redirect target.
 * - Returns true on any 2xx; false when attempts are exhausted. NEVER throws.
 */
export class WebhookSink {
  private readonly cfg: WebhookSinkConfig

  constructor(cfg: WebhookSinkConfig) {
    this.cfg = cfg
  }

  async deliver(event: WeirEvent): Promise<boolean> {
    const { url, secret, maxAttempts, timeoutMs } = this.cfg

    let body: string
    try {
      body = JSON.stringify(event)
    } catch (err) {
      log.error('webhook', `failed to serialize event ${event.event}: ${describeError(err)}`)
      return false
    }

    const attempts = Math.max(1, maxAttempts)
    for (let attempt = 1; attempt <= attempts; attempt++) {
      if (attempt > 1) {
        const backoff = BACKOFF_BASE_MS * 2 ** (attempt - 2) + Math.floor(Math.random() * JITTER_MAX_MS)
        await sleep(backoff)
      }

      const controller = new AbortController()
      const timer = setTimeout(() => controller.abort(), timeoutMs)
      try {
        const tSeconds = Math.floor(Date.now() / 1000)
        const res = await fetch(url, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-weir-signature': signBody(secret, body, tSeconds),
          },
          body,
          redirect: 'manual',
          signal: controller.signal,
        })
        // Cancel the unread response body so the socket is released; a cancel failure is
        // inconsequential to delivery status but is still logged.
        if (res.body) {
          void res.body.cancel().catch((err: unknown) => {
            log.warn('webhook', `response body cancel failed: ${describeError(err)}`)
          })
        }
        if (res.status >= 200 && res.status < 300) return true
        log.warn(
          'webhook',
          `attempt ${attempt}/${attempts}: HTTP ${res.status} (event=${event.event}, key=${event.idempotencyKey})`,
        )
      } catch (err) {
        log.warn(
          'webhook',
          `attempt ${attempt}/${attempts} failed: ${describeError(err)} (event=${event.event}, key=${event.idempotencyKey})`,
        )
      } finally {
        clearTimeout(timer)
      }
    }

    log.warn(
      'webhook',
      `delivery exhausted after ${attempts} attempts (event=${event.event}, key=${event.idempotencyKey})`,
    )
    return false
  }
}
