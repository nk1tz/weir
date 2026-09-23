// receiver.js — verify weir webhooks with nothing but node.
//
// The smallest correct receiver: check the signature, parse the event, answer 2xx.
// Contract: docs/webhooks.md. A verbose version that pretty-prints every event is catch.js.
//
// Usage:
//   WEBHOOK_SECRET=<same secret as weir's .env> node examples/receiver.js
//   PORT=9090 (default)

const http = require('node:http')
const crypto = require('node:crypto')

const SECRET = process.env.WEBHOOK_SECRET
if (!SECRET) {
  console.error('WEBHOOK_SECRET is required (must match weir)')
  process.exit(1)
}

http.createServer((req, res) => {
  const chunks = []
  req.on('data', (c) => chunks.push(c))
  req.on('end', () => {
    const body = Buffer.concat(chunks).toString('utf8')
    const m = /^t=(\d+), v1=([0-9a-f]{64})$/.exec(req.headers['x-weir-signature'] ?? '')
    const stale = !m || Math.abs(Date.now() / 1000 - Number(m[1])) > 300 // reject replays
    const expect = m && crypto.createHmac('sha256', SECRET).update(`${m[1]}.${body}`).digest('hex')
    if (stale || !crypto.timingSafeEqual(Buffer.from(m[2], 'hex'), Buffer.from(expect, 'hex'))) {
      res.writeHead(401).end()
      return
    }
    const event = JSON.parse(body)
    // delivery is at-least-once: dedupe on event.idempotencyKey before acting
    console.log(event.event, event.txid ?? event.address ?? '')
    res.writeHead(200).end() // any 2xx = delivered; anything else and weir retries
  })
}).listen(Number(process.env.PORT || 9090))
