import type { WeirEvent } from '../lib/types'
import { signBody } from '../lib/hmac'
import { describeError, log } from '../lib/log'

const CTX = 'webhook'

export interface WebhookSinkConfig {
  url: string
  secret: string
  /** per-request timeout, enforced via AbortController; config field webhookTimeoutMs */
  timeoutMs: number
}

export type SendResult = { ok: true } | { ok: false; error: string }

/**
 * The sink surface: only the outbox drainer and the heartbeat hold one. Engine modules
 * never do — they enqueue through the Store and the drainer delivers.
 */
export type Sink = Pick<WebhookSink, 'send'>

/**
 * POSTs one signed event to the configured webhook URL — ONE attempt, no retry loop
 * (retry is the outbox drainer's job, src/delivery/outbox.ts).
 *
 * - Body serialized once, signed with the current unix seconds (hmac.ts).
 * - `redirect: 'manual'` so a signed body is never re-POSTed to a redirect target.
 * - The unread response body is cancelled so the socket is released.
 * - 2xx → `{ok: true}`; anything else → `{ok: false, error}` naming the HTTP status, the
 *   timeout, or the network error. NEVER throws.
 */
export class WebhookSink {
  private readonly cfg: WebhookSinkConfig

  constructor(cfg: WebhookSinkConfig) {
    this.cfg = cfg
  }

  async send(event: WeirEvent): Promise<SendResult> {
    const { url, secret, timeoutMs } = this.cfg

    let body: string
    try {
      body = JSON.stringify(event)
    } catch (err) {
      return { ok: false, error: `serialize failed: ${describeError(err)}` }
    }

    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-weir-signature': signBody(secret, body, Math.floor(Date.now() / 1000)),
        },
        body,
        redirect: 'manual',
        signal: controller.signal,
      })
      // A cancel failure is inconsequential to delivery status but is still logged.
      if (res.body) {
        void res.body.cancel().catch((err: unknown) => {
          log.warn(CTX, `response body cancel failed: ${describeError(err)}`)
        })
      }
      if (res.status >= 200 && res.status < 300) return { ok: true }
      return { ok: false, error: `HTTP ${res.status}` }
    } catch (err) {
      if (controller.signal.aborted) return { ok: false, error: `timeout after ${timeoutMs}ms` }
      return { ok: false, error: describeError(err) }
    } finally {
      clearTimeout(timer)
    }
  }
}
