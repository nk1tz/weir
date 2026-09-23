# Operations

How to know weir is healthy, what to page on, and the edges it does not cover. Field-level
detail for the probes and metrics is in [admin-api.md](admin-api.md); the heartbeat payload
is in [webhooks.md](webhooks.md#heartbeat).

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

## Limitations

Honest edges, beyond the [is-not list](../README.md#what-weir-is-not):

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
  blocks, so confirmations survive downtime as long as the node still holds those blocks:
  past a pruned node's prune window weir resets tracking, logs it, and skips the
  unavailable history. Mempool-only activity during downtime (a tx seen and dropped) is
  unobservable.
