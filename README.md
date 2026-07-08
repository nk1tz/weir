<p align="center">
  <img src="assets/banner.png" alt="weir — watch every transaction flow by" width="100%">
</p>

# weir

Watch bitcoin addresses. Get signed webhooks. Runs on a pruned node.

weir is a single daemon that sits beside Bitcoin Core — pruned is fine, no `txindex`, no
address index — reads a watch set from redis, listens to the node's ZMQ stream, and POSTs
HMAC-signed events to one webhook URL whenever a transaction pays a watched address. A weir
is a fish trap in a river: the mempool and blocks flow through, watched payments get caught.

## What weir is not

Read this first. weir stays small by refusing to be these things:

- **Not a spend detector.** Receive-only: it matches transaction *outputs* paying watched
  addresses. Detecting spends *from* an address needs `txindex` and is out of scope.
- **Not a history service.** Forward-only: watching an address reports payments from that
  moment on. No backfill, no "what did this address receive last year".
- **Not a fanout.** One webhook URL per network. Routing events to per-customer endpoints
  is your layer.
- **Not multi-tenant.** No accounts, no API keys per user, no per-address webhook config.
- **Not reorg-proof beyond its window.** Reorgs deeper than your max confirmation milestone
  are invisible by design. Pick milestones that match your risk tolerance.

If you need any of the above, see [alternatives](#alternatives).

## 5-minute regtest quickstart

```sh
git clone https://github.com/nate/weir && cd weir
cp .env.example .env                          # defaults are regtest-ready

docker compose --profile regtest up -d --build   # weir + redis + a regtest bitcoind

# terminal 2 — a webhook receiver that verifies signatures and pretty-prints events
WEBHOOK_SECRET=change-me-openssl-rand-hex-32 node examples/catch.js

# terminal 3 — creates a wallet, watches an address, pays it, mines blocks
./examples/regtest-demo.sh
```

Watch terminal 2: a `seen` event when the payment hits the mempool, then `confirmed` at
1 and 3 confirmations. The full transcript with expected output for every command is in
[docs/quickstart-regtest.md](docs/quickstart-regtest.md).

## Integration touchpoints

There are exactly two.

### 1. Add watches: one redis command

The set `weir:{network}:addresses` is the public input API. Write to it from anything that
can speak redis:

```sh
redis-cli SADD weir:mainnet:addresses bc1qyouraddresshere
```

For a watch that expires (e.g. an invoice), also add a deadline to the `expiries` ZSET,
score = expiry as unix milliseconds; weir emits `expired` and removes the watch if it
passes unpaid:

```sh
redis-cli SADD weir:mainnet:addresses bc1q...
redis-cli ZADD weir:mainnet:expiries 1767225600000 bc1q...
```

Unwatch with `SREM` (and `ZREM`). Every other key under `weir:{network}:*` is the daemon's
memory — read if curious, never write.

If you prefer HTTP with address validation, set `ADMIN_TOKEN` to enable a small
bearer-token API (`POST /watches`, `DELETE /watches/:address`, `GET /health`). Unset, the
daemon listens on nothing at all.

### 2. Receive events: verify the HMAC

Events arrive as JSON POSTs with an `x-weir-signature` header:

```
x-weir-signature: t=<unix seconds>, v1=<hex hmac_sha256(secret, "<t>." + rawBody)>
```

A complete framework-free receiver:

```js
// receiver.js — verify weir webhooks with nothing but node
const http = require('node:http')
const crypto = require('node:crypto')

const SECRET = process.env.WEBHOOK_SECRET

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
}).listen(9090)
```

Respond 2xx fast and do real work async. Failed deliveries retry with exponential backoff
(`WEBHOOK_MAX_RETRIES` attempts); `confirmed` events that never get a 2xx are retried again
on every subsequent block until delivered.

## Events

Seven event types, one payload shape for the five transaction events
(`{version, event, network, txid, confs, matched: [{address, vout, valueSats}],
idempotencyKey, timestamp, blockHeight, blockHash, hex}`):

| event | when it fires | handler action |
|---|---|---|
| `seen` | a tx paying a watched address enters the mempool (needs milestone `0`) | show "payment detected", start waiting for confirmations |
| `confirmed` | the tx reaches a configured milestone depth (fires once per milestone, e.g. at 1 and at 3) | credit at whichever depth matches your risk tolerance |
| `dropped` | a seen tx vanishes from the mempool without being mined (evicted, replaced) | roll back "payment detected"; a rebroadcast will fire a fresh `seen` |
| `demoted` | a reorg orphans the tx's block and the tx returns to the mempool | revert to unconfirmed; new `confirmed` events follow if it's re-mined |
| `conflicted` | a reorg orphans the tx's block and the tx is gone (double-spend won) | reverse any credit, alert a human — terminal |
| `expired` | a TTL'd watch passed its deadline unpaid (address-scoped payload) | close the invoice for that address |
| `heartbeat` | every `HEARTBEAT_INTERVAL` seconds (`{tipHeight, watchCount, memoryUsedPct}`) | reset a dead-man's switch; page if heartbeats stop |

Every event carries an `idempotencyKey` unique to the logical occurrence — a re-mined tx's
second `confirmed:1` has a *different* key (it embeds the block hash). Dedupe on the key,
not on `(txid, event)`.

## Configuration

All configuration is environment variables. See [.env.example](.env.example).

| variable | required | default | notes |
|---|---|---|---|
| `NETWORK` | yes | — | `mainnet` \| `testnet` \| `signet` \| `regtest`; prefixes all redis keys |
| `BITCOIN_RPC_URL` | yes | — | credentials in the URL: `http://user:pass@host:8332` |
| `BITCOIN_ZMQ_URL` | yes | — | node must have `zmqpubrawtx` + `zmqpubrawblock` (same port is fine) |
| `REDIS_URL` | yes | — | `maxmemory-policy` must be `noeviction` — weir refuses to start otherwise |
| `WEBHOOK_URL` | yes | — | the one endpoint for this network's events |
| `WEBHOOK_SECRET` | yes | — | HMAC key; generate with `openssl rand -hex 32` |
| `CONFIRMATION_MILESTONES` | no | `0,1,3` | depths that fire events; `0` enables `seen`; the max value is the tracking window and reorg shield |
| `WATCH_DEFAULT_TTL` | no | `0` | seconds; default lifetime for admin-API watches; `0` = forever |
| `HEARTBEAT_INTERVAL` | no | `0` | seconds between heartbeats; `0` = off |
| `ADMIN_TOKEN` | no | unset | setting it creates the HTTP admin API and is its bearer token; unset = no listening socket exists |
| `ADMIN_PORT` | no | `8787` | admin API port |
| `WEBHOOK_MAX_RETRIES` | no | `3` | delivery attempts per event |
| `WEBHOOK_TIMEOUT_MS` | no | `10000` | per-attempt timeout |
| `REDIS_MAXMEMORY` | no | `256mb` | read by docker-compose for the redis container, not by the daemon |

**Capacity:** watches live in redis, ~330 bytes each with overhead. Rough formula:
`watches ≈ (maxmemory ÷ 1.5 − 40MB) ÷ 330B` — the ÷1.5 leaves headroom for the working
set, the 40MB is redis baseline. Defaults (`256mb`) hold roughly 400k watches; `1gb`
roughly 2M. weir logs the estimate for your configured maxmemory at boot.

## Limitations

Honest edges, beyond the [is-not list](#what-weir-is-not):

- **At-least-once delivery.** Crashes and retries can duplicate events. Dedupe on
  `idempotencyKey` — this is not optional.
- **`seen` is best-effort.** A tx can be mined without weir ever seeing it unconfirmed
  (daemon restart, ZMQ gap, direct-to-block). It still gets its `confirmed` events; don't
  build logic that requires a `seen` first.
- **`dropped`/`demoted`/`conflicted`/`expired` are one-shot.** If delivery fails after all
  retries, the event is logged and lost. Only `confirmed` re-fires until delivered.
- **Ordering isn't guaranteed** across event types under retries. Use `confs` and the
  idempotency keys, not arrival order.
- **Script coverage:** p2pkh, p2sh, p2wpkh, p2wsh, p2tr. Exotic output scripts decode with
  `address: null` and can't be watched.
- **Redis is the source of truth for watches.** Persistence is on (AOF) in the shipped
  compose file; if you run your own redis, losing it loses the watch set.
- **Downtime is safe for the chain, not the mempool.** Boot reconciliation replays missed
  blocks, so confirmations are never lost — but mempool-only activity during downtime
  (a tx seen and dropped) is unobservable.

## Alternatives

| | weir | BlockCypher | electrs | Cyphernode |
|---|---|---|---|---|
| model | self-hosted daemon | hosted API | self-hosted index | self-hosted suite |
| node requirements | pruned is fine | none (theirs) | unpruned + ~100GB index | full node stack |
| push webhooks | yes, HMAC-signed | yes | no — Electrum protocol subscriptions over a socket | yes |
| address history | no | yes | yes | some |
| who sees your addresses | you | them | you | you |
| moving parts | 1 daemon + redis | 0 (their uptime, their rate limits, their pricing) | electrs + clients | many containers |

Use BlockCypher if you don't run a node and don't mind a third party learning your
addresses. Use electrs if you need history or arbitrary-address queries and can afford a
full index. Use Cyphernode if you want a whole self-hosted bitcoin backend. Use weir if
you run a (pruned) node and just want to know when watched addresses get paid.

## License

MIT — see [LICENSE](LICENSE).
