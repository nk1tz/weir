import { createHmac, timingSafeEqual } from 'node:crypto'

/**
 * HMAC signing for webhook deliveries.
 *
 * Header format (the full VALUE of `x-weir-signature`):
 *   t=<unix seconds>, v1=<hex hmac_sha256(secret, "<t>." + rawBody)>
 */

const DEFAULT_TOLERANCE_SEC = 300

function hmacHex(secret: string, tSeconds: number, body: string): string {
  return createHmac('sha256', secret).update(`${tSeconds}.${body}`).digest('hex')
}

/** Returns the full `x-weir-signature` header value: `t=<t>, v1=<hex>`. */
export function signBody(secret: string, body: string, tSeconds: number): string {
  return `t=${tSeconds}, v1=${hmacHex(secret, tSeconds, body)}`
}

/**
 * Verify an `x-weir-signature` header value against a raw body.
 *
 * - Parses the header tolerantly (arbitrary whitespace around parts, any part order).
 * - Rejects timestamps outside |nowSeconds - t| <= toleranceSec (default 300s).
 * - Constant-time digest comparison via crypto.timingSafeEqual (length mismatch → false).
 */
export function verifySignature(
  secret: string,
  body: string,
  header: string,
  toleranceSec: number = DEFAULT_TOLERANCE_SEC,
  nowSeconds: number = Math.floor(Date.now() / 1000),
): boolean {
  if (typeof header !== 'string' || header.length === 0) return false

  let t: number | null = null
  let v1: string | null = null
  for (const part of header.split(',')) {
    const eq = part.indexOf('=')
    if (eq === -1) continue
    const key = part.slice(0, eq).trim()
    const value = part.slice(eq + 1).trim()
    if (key === 't') {
      const n = Number(value)
      if (value.length > 0 && Number.isInteger(n) && n >= 0) t = n
    } else if (key === 'v1') {
      v1 = value
    }
  }

  if (t === null || v1 === null) return false
  if (!/^[0-9a-fA-F]+$/.test(v1)) return false
  if (v1.length % 2 !== 0) return false
  if (Math.abs(nowSeconds - t) > toleranceSec) return false

  const expected = Buffer.from(hmacHex(secret, t, body), 'hex')
  const provided = Buffer.from(v1.toLowerCase(), 'hex')
  if (expected.length !== provided.length) return false
  return timingSafeEqual(expected, provided)
}
