#!/usr/bin/env node
'use strict'

/**
 * catch.js — zero-dependency webhook catcher for weir.
 *
 * Verifies the `x-weir-signature` header (t/v1 HMAC-SHA256 scheme, constant-time
 * compare, 300s timestamp tolerance) and pretty-prints verified events.
 *
 * Usage:
 *   WEBHOOK_SECRET=<same secret as weir's .env> node examples/catch.js
 *   PORT=9090 (default)
 */

const http = require('node:http')
const crypto = require('node:crypto')

const PORT = Number(process.env.PORT || 9090)
const SECRET = process.env.WEBHOOK_SECRET
const TOLERANCE_SEC = 300

if (!SECRET) {
  console.error('[catch] WEBHOOK_SECRET env var is required (must match weir\'s WEBHOOK_SECRET)')
  process.exit(1)
}

/**
 * Verify `x-weir-signature: t=<unix seconds>, v1=<hex hmac_sha256(secret, "<t>." + body)>`.
 * Reimplements weir's check inline: constant-time compare, reject stale timestamps.
 */
function verifySignature(secret, body, header, nowSeconds = Math.floor(Date.now() / 1000)) {
  if (typeof header !== 'string' || header.length === 0) return false

  let t = null
  let v1 = null
  for (const part of header.split(',')) {
    const p = part.trim()
    if (p.startsWith('t=')) t = p.slice(2)
    else if (p.startsWith('v1=')) v1 = p.slice(3)
  }
  if (!t || !v1) return false

  const tNum = Number(t)
  if (!Number.isInteger(tNum) || tNum <= 0) return false
  if (Math.abs(nowSeconds - tNum) > TOLERANCE_SEC) return false

  if (!/^[0-9a-f]{64}$/i.test(v1)) return false
  const received = Buffer.from(v1, 'hex')
  const expected = crypto.createHmac('sha256', secret).update(`${t}.${body}`).digest()
  if (received.length !== expected.length) return false
  return crypto.timingSafeEqual(received, expected)
}

/** One-line summary: event, txid/address, confs, sats. */
function summarize(event) {
  switch (event.event) {
    case 'seen':
    case 'confirmed':
    case 'dropped':
    case 'demoted':
    case 'conflicted': {
      const sats = (event.matched || []).reduce((sum, m) => sum + m.valueSats, 0)
      const addrs = (event.matched || []).map((m) => m.address).join(',')
      return `${event.event} txid=${event.txid} confs=${event.confs} sats=${sats} addr=${addrs}`
    }
    case 'expired':
      return `expired address=${event.address}`
    case 'heartbeat':
      return `heartbeat tip=${event.tipHeight} watches=${event.watchCount} mem=${event.memoryUsedPct === null ? 'n/a' : event.memoryUsedPct + '%'}`
    default:
      return `unknown event type "${event.event}"`
  }
}

function respond(res, status, obj) {
  const body = JSON.stringify(obj)
  res.writeHead(status, { 'content-type': 'application/json' })
  res.end(body)
}

const server = http.createServer((req, res) => {
  if (req.method !== 'POST') {
    respond(res, 405, { error: 'POST only' })
    return
  }

  // Cap the body BEFORE signature verification — an unauthenticated sender must not
  // be able to make us buffer unbounded bytes. Real weir events are a few KB.
  const MAX_BODY = 1024 * 1024
  const chunks = []
  let total = 0
  req.on('data', (c) => {
    total += c.length
    if (total > MAX_BODY) {
      console.error(`[catch] body exceeded ${MAX_BODY} bytes from ${req.socket.remoteAddress} — dropping connection`)
      req.destroy()
      return
    }
    chunks.push(c)
  })
  req.on('error', (err) => {
    console.error(`[catch] request stream error: ${err.message}`)
    res.destroy()
  })
  req.on('end', () => {
    const body = Buffer.concat(chunks).toString('utf8')
    const header = req.headers['x-weir-signature']

    if (!verifySignature(SECRET, body, header)) {
      console.error(`[catch] BAD SIGNATURE from ${req.socket.remoteAddress} (header: ${header ?? 'missing'})`)
      respond(res, 401, { error: 'bad signature' })
      return
    }

    let event
    try {
      event = JSON.parse(body)
    } catch (err) {
      console.error(`[catch] signature ok but body is not JSON: ${err.message}`)
      respond(res, 400, { error: 'invalid JSON' })
      return
    }

    console.log(`[catch] ${summarize(event)}`)
    console.log(JSON.stringify(event, null, 2))
    respond(res, 200, { ok: true })
  })
})

server.listen(PORT, () => {
  console.log(`[catch] listening on :${PORT} — verifying x-weir-signature with WEBHOOK_SECRET (tolerance ${TOLERANCE_SEC}s)`)
})
