# Admin API

The HTTP admin API exists only when `ADMIN_TOKEN` is set; unset, the daemon listens on
nothing at all. It serves on `ADMIN_PORT` (default `8787`). The four probes (`GET /live`,
`/ready`, `/health`, `/metrics`) are unauthenticated and expose counts only, never addresses
or txids. Every other request needs `authorization: Bearer <ADMIN_TOKEN>` (scheme
case-insensitive, compared in constant time) and answers `401 {"error":"unauthorized"}`
without it; an unknown path answers `404 {"error":"not found"}` after the token check, so a
wrong path without a token is a 401. A path segment with malformed percent-encoding answers
`400 {"error":"malformed percent-encoding in path: …"}`. Until boot reconciliation finishes,
the write routes (`POST /watches`, `DELETE /watches/:address`) answer
`503 {"error":"not ready: reconciling"}` with `retry-after: 1`; reads and probes never do.
An unhandled error answers `500 {"error":"internal error"}` and never crashes the daemon.
All bodies are `application/json` except `/metrics`. The webhook side of the surface is
[webhooks.md](webhooks.md); the redis input path that needs no HTTP is
[redis-api.md](redis-api.md).

## GET /live

Liveness probe: the process is up and not shutting down. It consults nothing (restarting
weir because redis or bitcoind is down fixes nothing).

```sh
curl -s localhost:8787/live
```

```json
{"ok":true}
```

| status | body |
|---|---|
| `200` | `{"ok":true}` |
| `503` | `{"ok":false}` once shutdown has begun |

## GET /ready

Readiness probe and one-shot status page. `ok` is true when redis is reachable AND
bitcoind is reachable AND boot reconciliation finished AND not shutting down AND
`chainLag ≤ READY_MAX_LAG`. Webhook and outbox state never affect `ok`: weir is healthy
when your consumer is down. Status is `200` when `ok`, else `503`; the body is the same
shape either way.

```sh
curl -s localhost:8787/ready
```

```json
{
  "ok": true,
  "redis": true,
  "rpc": true,
  "reconciled": true,
  "shuttingDown": false,
  "tipHeight": 935412,
  "nodeHeight": 935412,
  "chainLag": 0,
  "watchCount": 1842,
  "outboxDepth": 0,
  "outboxOldestAgeSec": null,
  "deadLetterCount": 0,
  "lastZmqTxAgeSec": 4,
  "lastZmqBlockAgeSec": 512
}
```

| field | type | meaning |
|---|---|---|
| `ok` | boolean | the readiness verdict |
| `redis` | boolean | the redis reads (tip, watch count, outbox stats) succeeded |
| `rpc` | boolean | `getblockcount` answered |
| `reconciled` | boolean | boot reconciliation finished |
| `shuttingDown` | boolean | shutdown has begun |
| `tipHeight` | number \| null | last block weir processed; `null` before the first tip or when redis failed |
| `nodeHeight` | number \| null | the node's best height; `null` when the RPC failed |
| `chainLag` | number \| null | `nodeHeight − tipHeight`; `null` when either is unknown (then not ready) |
| `watchCount` | number | size of the watch set; `0` when redis failed |
| `outboxDepth` | number | events queued for delivery; `0` when redis failed |
| `outboxOldestAgeSec` | number \| null | age of the oldest queued event; `null` when empty or redis failed |
| `deadLetterCount` | number | dead-lettered events retained; `0` when redis failed |
| `lastZmqTxAgeSec` | number \| null | seconds since the last ZMQ `rawtx`; `null` until the first after boot |
| `lastZmqBlockAgeSec` | number \| null | seconds since the last ZMQ `rawblock`; `null` until the first after boot |

## GET /health

Alias of `GET /ready`, kept for compatibility. Same status codes and body.
(`secondsSinceLastBlock` is gone: block intervals are Poisson, so it never meant anything.)

## GET /metrics

Prometheus text exposition (`text/plain; version=0.0.4; charset=utf-8`), weir-native series
only. A gauge whose read failed during the scrape is omitted (warned); `weir_up` stays `1`
and the scrape is always `200`. Counters reset on restart.

```sh
curl -s localhost:8787/metrics
```

```
# TYPE weir_blocks_processed_total counter
weir_blocks_processed_total 17
# TYPE weir_chain_lag gauge
weir_chain_lag 0
# TYPE weir_events_enqueued_total counter
weir_events_enqueued_total{event="seen"} 3
weir_events_enqueued_total{event="confirmed"} 6
weir_events_enqueued_total{event="dropped"} 0
weir_events_enqueued_total{event="demoted"} 0
weir_events_enqueued_total{event="conflicted"} 0
weir_events_enqueued_total{event="expired"} 0
# TYPE weir_webhook_deliveries_total counter
weir_webhook_deliveries_total{result="ok"} 9
weir_webhook_deliveries_total{result="fail"} 0
…
```

| series | type | labels | meaning |
|---|---|---|---|
| `weir_up` | gauge | — | always `1` on a served scrape |
| `weir_reconciled` | gauge | — | `1` once boot reconciliation finished |
| `weir_tip_height` | gauge | — | last block weir processed; absent before the first tip |
| `weir_node_height` | gauge | — | the node's best height |
| `weir_chain_lag` | gauge | — | `weir_node_height − weir_tip_height`; absent when either is missing |
| `weir_watch_count` | gauge | — | size of the watch set |
| `weir_outbox_depth` | gauge | — | events queued for delivery |
| `weir_outbox_oldest_age_seconds` | gauge | — | age of the oldest queued event; `0` when empty |
| `weir_dead_letter_count` | gauge | — | dead-lettered events retained |
| `weir_redis_memory_used_bytes` | gauge | — | redis `used_memory` |
| `weir_redis_memory_max_bytes` | gauge | — | redis `maxmemory`; absent when unlimited |
| `weir_last_zmq_tx_timestamp_seconds` | gauge | — | unix time of the last ZMQ `rawtx`; absent until the first after boot |
| `weir_last_zmq_block_timestamp_seconds` | gauge | — | unix time of the last ZMQ `rawblock`; absent until the first after boot |
| `weir_events_enqueued_total` | counter | `event="seen\|confirmed\|dropped\|demoted\|conflicted\|expired"` | events written to the outbox (`heartbeat` bypasses it) |
| `weir_webhook_deliveries_total` | counter | `result="ok\|fail"` | delivery attempts by outcome |
| `weir_events_dead_lettered_total` | counter | — | events given up on |
| `weir_blocks_processed_total` | counter | — | blocks applied |
| `weir_reorgs_total` | counter | — | reorgs handled |

## POST /watches

Add a watch, with address validation for the configured network. Idempotent: watching an
address again answers `201` and replaces its expiry (or clears it when the new watch has
no TTL).

```sh
curl -s -X POST localhost:8787/watches \
  -H 'authorization: Bearer 3f9c…' \
  -H 'content-type: application/json' \
  -d '{"address":"bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq","ttl":86400}'
```

```json
{"address":"bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq","network":"mainnet","expiresAt":1758672000123}
```

Request body (JSON object, at most 4096 bytes):

| field | type | meaning |
|---|---|---|
| `address` | string | required; must be a valid address for `NETWORK` |
| `ttl` | integer | optional; lifetime in seconds, `0`–`315360000` (10 years). Omitted: `WATCH_DEFAULT_TTL` applies. `0` (or a default of `0`): watch forever |

Response `201`:

| field | type | meaning |
|---|---|---|
| `address` | string | as sent |
| `network` | string | the daemon's `NETWORK` |
| `expiresAt` | number \| null | unix ms when the watch expires; `null` for forever |

Errors:

| status | body |
|---|---|
| `400` | `{"error":"invalid JSON body: …"}` |
| `400` | `{"error":"body must be a JSON object"}` |
| `413` | `{"error":"body exceeds 4096 byte limit"}` (with `connection: close`) |
| `422` | `{"error":"address must be a non-empty string"}` |
| `422` | `{"error":"invalid mainnet address"}` (names the configured network) |
| `422` | `{"error":"ttl must be an integer number of seconds between 0 and 315360000"}` |
| `503` | `{"error":"not ready: reconciling"}` + `retry-after: 1` until boot reconciliation finishes |

## GET /watches

List the watch set one page at a time. Pagination is a redis `SSCAN` cursor: start at
`0` (the default), pass back the `cursor` you received, stop when it comes back as `"0"`.
Page size is a hint (`COUNT 1000`), not a guarantee; an address can appear on more than
one page if the set changes during iteration.

```sh
curl -s 'localhost:8787/watches?cursor=0' -H 'authorization: Bearer 3f9c…'
```

```json
{"addresses":["bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq","bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"],"cursor":"0"}
```

| field | type | meaning |
|---|---|---|
| `addresses` | string[] | the addresses on this page |
| `cursor` | string | pass it back as `?cursor=`; `"0"` means iteration complete |

Errors: `400 {"error":"cursor must be a non-negative integer string (from a previous page, or \"0\")"}`.

## GET /watches/:address

Is this address watched, and until when. The path segment is percent-decoded; the address
is not validated (an invalid one is simply not watched).

```sh
curl -s localhost:8787/watches/bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq -H 'authorization: Bearer 3f9c…'
```

```json
{"address":"bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq","watched":true,"expiresAt":1758672000123}
```

| status | body |
|---|---|
| `200` | `{"address", "watched": true, "expiresAt"}` where `expiresAt` is unix ms or `null` for a watch without TTL |
| `404` | `{"error":"not watched"}` |

## DELETE /watches/:address

Remove a watch and its expiry. Idempotent: `204` whether or not the address was watched.
A tx already confirmed keeps firing its remaining milestones; one still unconfirmed stops
being tracked once mined.

```sh
curl -s -X DELETE localhost:8787/watches/bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq -H 'authorization: Bearer 3f9c…'
```

| status | body |
|---|---|
| `204` | empty |
| `503` | `{"error":"not ready: reconciling"}` + `retry-after: 1` until boot reconciliation finishes |
