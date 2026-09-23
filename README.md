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
bearer-token API (`POST /watches`, `DELETE /watches/:address`) — the same port also serves
the unauthenticated probes `GET /live`, `/ready` and `/metrics` (see
[Monitoring](#monitoring)). Unset, the daemon listens on nothing at all. While weir is
still reconciling after a (re)start, the write routes answer `503 {error: 'not ready:
reconciling'}` with `retry-after: 1` — retry, or wait for `GET /ready` to report
`reconciled: true`. (A watch added before reconciliation could be paid in a block weir then
skips past.) The `SADD` path has no such window: redis accepts the watch immediately and
the next block weir processes sees it.

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

Respond 2xx fast and do real work async. Every event is written to a durable outbox in redis
in the same transaction as the state change that produced it, and a drainer POSTs it: a
non-2xx, a network error or a timeout is retried with exponential backoff (1s doubling, capped
at 5 min, with jitter) for up to `OUTBOX_MAX_AGE` (3 days by default). After that the event is
dead-lettered — kept in redis (`weir:{network}:outbox:dead`, capped at `OUTBOX_DEAD_MAX`) and
logged at error level with its `idempotencyKey`. The guarantee is per queued EVENT, by age:
an event is retried until it is older than `OUTBOX_MAX_AGE`, then dead-lettered on its next
failed attempt — regardless of how long the current outage has lasted. Only the last
`OUTBOX_DEAD_MAX` dead-lettered events are retained for inspection. Block processing never
waits on your endpoint.

Because retries reorder things, **dedupe on `idempotencyKey` in the same database transaction
as the credit or reversal you perform, and never assume arrival order**: every event carries
absolute state (event type, `confs`, block hash in the key), never a delta.

## Events

Seven event types, one payload shape for the five transaction events
(`{version, event, network, txid, confs, matched: [{address, vout, valueSats}],
idempotencyKey, timestamp, blockHeight, blockHash, hex}`):

| event | when it fires | handler action |
|---|---|---|
| `seen` | a tx paying a watched address enters the mempool (needs milestone `0`) | show "payment detected", start waiting for confirmations |
| `confirmed` | the tx reaches a configured milestone depth (fires once per milestone, e.g. at 1 and at 3) | credit at whichever depth matches your risk tolerance |
| `dropped` | a seen tx vanishes from the mempool without being mined; `reason` is `replaced` (another tx spent one of its inputs — RBF fee-bump or redirect — detected the moment weir sees it, with `replacedBy: <txid>`) or `evicted` (residual: gone from the mempool at the next block) | roll back "payment detected"; a rebroadcast will fire a fresh `seen` |
| `demoted` | a reorg orphans the tx's block and the tx returns to the mempool | revert to unconfirmed; new `confirmed` events follow if it's re-mined |
| `conflicted` | a reorg orphans the tx's block and the tx is gone (double-spend won); when the new chain provably spent one of its inputs the event carries `reason: 'double-spend'` and `conflictingTxid` | reverse any credit, alert a human — terminal |
| `expired` | a TTL'd watch passed its deadline unpaid (address-scoped payload) | close the invoice for that address |
| `heartbeat` | every `HEARTBEAT_INTERVAL` seconds (`{tipHeight, nodeHeight, chainLag, watchCount, memoryUsedPct, outboxDepth, outboxOldestAgeSec, deadLetterCount}`) | reset a dead-man's switch; page if heartbeats stop, `chainLag` stays above `READY_MAX_LAG`, or `outboxDepth` keeps growing |

Every event carries an `idempotencyKey` unique to the logical occurrence — a re-mined tx's
second `confirmed:1` has a *different* key (it embeds the block hash). Dedupe on the key,
not on `(txid, event)`.

`heartbeat` is the one event that skips the outbox: it is proof-of-life, sent directly once
per interval, and it *reports* the outbox instead (`outboxDepth`, `outboxOldestAgeSec`,
`deadLetterCount`) plus the chain lag (`nodeHeight − tipHeight`, so a consumer can tell "alive
but stuck" without its own chain source). `GET /ready` reports the same fields.

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
| `WEBHOOK_TIMEOUT_MS` | no | `10000` | per-attempt timeout; a failed attempt is retried by the outbox |
| `OUTBOX_MAX_AGE` | no | `259200` | seconds an undelivered event is retried before it is dead-lettered (3 days) |
| `OUTBOX_DEAD_MAX` | no | `1000` | dead-letter cap; the oldest dead events beyond it are dropped |
| `READY_MAX_LAG` | no | `2` | `GET /ready` is 503 once `chainLag` (node height − weir tip) exceeds this; see [Monitoring](#monitoring) |
| `REDIS_MAXMEMORY` | no | `256mb` | read by docker-compose for the redis container, not by the daemon |

**Capacity:** watches live in redis, ~330 bytes each with overhead. Rough formula:
`watches ≈ (maxmemory ÷ 1.5 − 45MB) ÷ 330B` — the ÷1.5 leaves headroom for the working
set, the 45MB is redis baseline. Defaults (`256mb`) hold roughly 400k watches; `1gb`
roughly 2M. weir logs the estimate for your configured maxmemory at boot.

## Limitations

Honest edges, beyond the [is-not list](#what-weir-is-not):

- **At-least-once delivery.** A crash between a 2xx and the outbox ack re-sends the event.
  Dedupe on `idempotencyKey` — this is not optional.
- **`seen` is best-effort.** A tx can be mined without weir ever seeing it unconfirmed
  (daemon restart, ZMQ gap, direct-to-block). It still gets its `confirmed` events; don't
  build logic that requires a `seen` first.
- **Delivery is bounded by age, not by attempts.** An event that never gets a 2xx within
  `OUTBOX_MAX_AGE` is dead-lettered (kept in redis, logged loudly) — it will not be retried
  by itself. Watch `deadLetterCount` in heartbeats or `/ready`, or alert on any increase of
  `weir_events_dead_lettered_total` in `/metrics`.
- **Ordering is best-effort across failures.** Events enqueued together arrive in order when
  your endpoint is healthy; once a delivery fails, its retry can land after younger events.
  Use `confs` and the idempotency keys, not arrival order.
- **Script coverage:** p2pkh, p2sh, p2wpkh, p2wsh, p2tr. Exotic output scripts decode with
  `address: null` and can't be watched.
- **Redis is the source of truth for watches.** Persistence is on (AOF) in the shipped
  compose file; if you run your own redis, losing it loses the watch set.
- **Downtime is safe for the chain, not the mempool.** Boot reconciliation replays missed
  blocks, so confirmations are never lost — but mempool-only activity during downtime
  (a tx seen and dropped) is unobservable.

## Monitoring

With `ADMIN_TOKEN` set, the admin port serves three unauthenticated probes (they expose
counts, never addresses or txids). Without it there is no listening socket: use the
`heartbeat` event as your only signal.

| endpoint | answers | use it for |
|---|---|---|
| `GET /live` | `200 {ok:true}` while the process is up; `503` once shutdown has begun | liveness probe. It consults nothing — restarting weir because redis or bitcoind is down fixes nothing |
| `GET /ready` | `200`/`503` `{ok, redis, rpc, reconciled, shuttingDown, tipHeight, nodeHeight, chainLag, watchCount, outboxDepth, outboxOldestAgeSec, deadLetterCount, lastZmqTxAgeSec, lastZmqBlockAgeSec}` | readiness probe and a one-shot status page. `ok` = redis reachable AND bitcoind reachable AND boot reconciliation finished AND not shutting down AND `chainLag ≤ READY_MAX_LAG` |
| `GET /health` | alias of `/ready` | kept for compatibility (`secondsSinceLastBlock` is gone — block intervals are Poisson, so it never meant anything) |
| `GET /metrics` | Prometheus text (`text/plain; version=0.0.4`) | scraping |

**The signal is chain lag**: `chainLag = nodeHeight − tipHeight`, the node's best height
(one `getblockcount` per probe) minus the last block weir processed. Bitcoin Core knows about
blocks weir has not handled, or it does not — a lag of 1 is normal for a moment after every
block; a lag that stays above `READY_MAX_LAG` (default 2) means the pipeline is stuck or ZMQ
is dead. A slow endpoint, a growing outbox or dead-lettered events NEVER fail `/ready`: weir
is healthy when your consumer is down, and the outbox is retrying for you.

`/metrics` carries weir-native series only (host metrics are your platform's job):

- gauges: `weir_up`, `weir_reconciled`, `weir_tip_height`, `weir_node_height`,
  `weir_chain_lag`, `weir_watch_count`, `weir_outbox_depth`,
  `weir_outbox_oldest_age_seconds`, `weir_dead_letter_count`,
  `weir_redis_memory_used_bytes`, `weir_redis_memory_max_bytes` (absent when redis has no
  `maxmemory`), `weir_last_zmq_tx_timestamp_seconds`, `weir_last_zmq_block_timestamp_seconds`
  (absent until the first message after boot);
- counters (reset on restart): `weir_events_enqueued_total{event}`,
  `weir_webhook_deliveries_total{result="ok|fail"}`, `weir_events_dead_lettered_total`,
  `weir_blocks_processed_total`, `weir_reorgs_total`.

A gauge whose read failed during the scrape (redis hiccup) is simply omitted; `weir_up` stays
1 and the scrape never 500s.

Alert on:

- no `heartbeat` for 2× `HEARTBEAT_INTERVAL` — the daemon is down or cannot reach you;
- `weir_chain_lag > READY_MAX_LAG` (or `chainLag` in heartbeats) persisting for more than
  3 minutes — weir is behind the node;
- `weir_outbox_oldest_age_seconds > 300` — your endpoint has been rejecting an event for
  5 minutes;
- any increase in `weir_events_dead_lettered_total` — an event was given up on; it is in
  `weir:{network}:outbox:dead` for inspection;
- `weir_redis_memory_used_bytes / weir_redis_memory_max_bytes > 0.8` — the watch set is
  approaching `maxmemory` (`noeviction` means writes start failing, not that watches vanish).

Do NOT page on: a single missed block interval (20-minute gaps are normal), `outboxDepth`
briefly above zero (a retry in progress), `lastZmqTxAgeSec` alone on a quiet regtest/signet
(no transactions means no messages), or `/ready` 503 during boot while `reconciled` is
false (a long catch-up after downtime is working as designed).

Example scrape config:

```yaml
scrape_configs:
  - job_name: weir
    static_configs:
      - targets: ['weir:8787']
```

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

## Development

```sh
pnpm test          # unit suite: engine + store contracts against in-memory fakes, no services needed
pnpm test:redis    # executes every Store Lua script against a real redis (see below)
pnpm typecheck && pnpm build
```

The Store's state transitions are Redis Lua scripts (atomic guards + enqueue). The unit
suite pins their text and mirrors their semantics in a fake, but only a real redis executes
them — run the integration file before touching `src/store/redis.ts`:

```sh
docker run -d --name weir-test-redis -p 6390:6379 redis:7-alpine
pnpm test:redis
```

End to end against a real node: `examples/regtest-demo.sh` (see the quickstart).

## License

MIT — see [LICENSE](LICENSE).
