# Configuration

All configuration is environment variables. See [.env.example](../.env.example) for a
regtest-ready starting point.

| variable | required | default | notes |
|---|---|---|---|
| `NETWORK` | yes | — | `mainnet` \| `testnet` \| `signet` \| `regtest`; prefixes all redis keys |
| `BITCOIN_RPC_URL` | yes | — | credentials in the URL: `http://user:pass@host:8332` |
| `BITCOIN_ZMQ_URL` | yes | — | node must have `zmqpubrawtx` + `zmqpubrawblock` (same port is fine) |
| `REDIS_URL` | yes | — | `maxmemory-policy` must be `noeviction`: a detected eviction policy is fatal at boot; when redis blocks `CONFIG`, weir warns and continues, so enforce it yourself |
| `WEBHOOK_URL` | yes | — | the one endpoint for this network's events; plain `http(s)`, no embedded `user:pass@` (authenticity is the HMAC) |
| `WEBHOOK_SECRET` | yes | — | HMAC key; generate with `openssl rand -hex 32` |
| `CONFIRMATION_MILESTONES` | no | `0,1,3` | depths that fire events, integers 0–100; `0` enables `seen`; the max value is the tracking window and reorg shield |
| `WATCH_DEFAULT_TTL` | no | `0` | seconds; default lifetime for admin-API watches; `0` = forever |
| `HEARTBEAT_INTERVAL` | no | `0` | seconds between heartbeats; `0` = off |
| `ADMIN_TOKEN` | no | unset | setting it creates the HTTP admin API and is its bearer token; unset = no listening socket exists |
| `ADMIN_PORT` | no | `8787` | admin API port |
| `WEBHOOK_TIMEOUT_MS` | no | `10000` | per-attempt timeout; a failed attempt is retried by the outbox |
| `OUTBOX_MAX_AGE` | no | `259200` | seconds an undelivered event is retried before it is dead-lettered (3 days) |
| `OUTBOX_DEAD_MAX` | no | `1000` | dead-letter cap; the oldest dead events beyond it are dropped |
| `READY_MAX_LAG` | no | `2` | `GET /ready` is 503 once `chainLag` (node height − weir tip) exceeds this; see [operations.md](operations.md#monitoring) |
| `REDIS_MAXMEMORY` | no | `256mb` | read by docker-compose for the redis container, not by the daemon |

## Parsing

Every value is trimmed. A blank optional setting takes its default; a whitespace-only
`ADMIN_TOKEN` counts as unset (no HTTP server). Integers must be non-negative;
`OUTBOX_MAX_AGE`, `OUTBOX_DEAD_MAX` and `READY_MAX_LAG` must be positive.
`CONFIRMATION_MILESTONES` drops empty segments, dedupes and sorts; each entry must be an
integer 0–100, and a non-blank list with no entries is rejected. An invalid value stops the
daemon at boot with a message naming the variable.

Retry, delivery and dead-letter behaviour: [webhooks.md](webhooks.md#delivery-and-retries).
The production compose file also reads `WEIR_VERSION`, `BITCOIND_VERSION` and
`BITCOIND_CONF`: [DEPLOY.md](DEPLOY.md).

## Capacity

Watches live in redis, ~330 bytes each with overhead. Rough formula:
`watches ≈ (maxmemory ÷ 1.5 − 45MB) ÷ 330B` — the ÷1.5 leaves headroom for the working
set, the 45MB is redis baseline. Defaults (`256mb`) hold roughly 400k watches; `1gb`
roughly 2M. weir logs the estimate for your configured maxmemory at boot.
