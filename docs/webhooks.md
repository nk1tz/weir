# Webhooks

weir delivers every event as one HTTP `POST` to `WEBHOOK_URL` with a JSON body and an
`x-weir-signature` header. Any `2xx` response means delivered; anything else (non-2xx, a
network error, or no response within `WEBHOOK_TIMEOUT_MS`) is a failed attempt that weir
retries from a durable outbox. Delivery is at-least-once and order is best-effort, so
consumers dedupe on `idempotencyKey` and never assume arrival order. The admin API is the
other half of the surface: [admin-api.md](admin-api.md). Internals: [DESIGN.md](DESIGN.md)
"Events" and "Outbox".

## Signature

```
x-weir-signature: t=<unix seconds>, v1=<hex hmac_sha256(secret, "<t>." + rawBody)>
```

- `t` is the unix time (seconds) at which weir signed the request.
- `v1` is HMAC-SHA256 over the string `<t>.<rawBody>`, keyed with `WEBHOOK_SECRET`,
  as lowercase hex.
- Verify over the raw bytes you received, not a re-serialized JSON object.
- Reject the request when `|now − t| > 300` seconds: this bounds replays.
- Compare digests in constant time.

The lines that matter, from [examples/receiver.js](../examples/receiver.js):

```js
const m = /^t=(\d+), v1=([0-9a-f]{64})$/.exec(req.headers['x-weir-signature'] ?? '')
const stale = !m || Math.abs(Math.floor(Date.now() / 1000) - Number(m[1])) > 300 // reject replays
const expect = m && crypto.createHmac('sha256', SECRET).update(`${m[1]}.${body}`).digest('hex')
if (stale || !crypto.timingSafeEqual(Buffer.from(m[2], 'hex'), Buffer.from(expect, 'hex'))) {
  res.writeHead(401).end()
  return
}
```

Every request also carries `content-type: application/json`. weir follows no redirects: a
`3xx` is a failed attempt.

## Delivery and retries

Every event (except `heartbeat`) is written to the outbox in redis in the same transaction
as the state change that produced it. A drainer POSTs queued events once per second, in
enqueue order, one at a time.

| setting | default | meaning |
|---|---|---|
| `WEBHOOK_TIMEOUT_MS` | `10000` | per-attempt timeout |
| backoff | — | after failure number *n*: `min(1000 × 2^(n−1), 300000)` ms, plus up to 25 % jitter (1 s, 2 s, 4 s, … capped at 5 min) |
| `OUTBOX_MAX_AGE` | `259200` (3 days) | once an event is older than this, its next failed attempt dead-letters it |
| `OUTBOX_DEAD_MAX` | `1000` | dead-letter cap; the oldest dead events beyond it are deleted |

A dead-lettered event is moved to `weir:{network}:outbox:dead`, logged at error level with
its `idempotencyKey`, and never retried on its own. The guarantee is per event, by age:
the length of the current outage does not matter.

Respond 2xx fast and do real work asynchronously. Block processing never waits on your
endpoint.

## Ordering and idempotency

Retries reorder events: once a delivery fails, its retry can land after younger events. A
crash between your 2xx and the outbox ack re-sends the event. Therefore:

- dedupe on `idempotencyKey` in the same database transaction as the credit or reversal
  you perform;
- never assume arrival order: every event carries absolute state (event type, `confs`,
  block hash in the key), never a delta;
- do not require a `seen` before a `confirmed`: `seen` is best-effort.

`idempotencyKey` is unique per logical occurrence. Shapes:

| event | key |
|---|---|
| `seen` | `{network}:{txid}:seen` |
| `confirmed` | `{network}:{txid}:confirmed:{milestone}:{blockHash}` |
| `dropped` (`evicted`) | `{network}:{txid}:dropped:{tipHeight}` |
| `dropped` (`replaced`) | `{network}:{txid}:dropped:replaced:{replacedBy}` |
| `demoted` | `{network}:{txid}:demoted:{blockHash}` |
| `conflicted` | `{network}:{txid}:conflicted` |
| `expired` | `{network}:{address}:expired:{expiresAtMs}` |
| `heartbeat` | `{network}:heartbeat:{timestampMs}` |

A re-mined tx's second `confirmed:1` has a different key because it embeds the block hash.
Dedupe on the key, not on `(txid, event)`. The one key that repeats: a tx that was
`dropped` and then rebroadcast fires a second `seen` with the ORIGINAL
`{network}:{txid}:seen` key. A consumer that dedupes on the key forever will discard that
second `seen`; forget the key when you process the `dropped`, or treat `seen` as advisory.

## Transaction event payload

`seen`, `confirmed`, `dropped`, `demoted` and `conflicted` share one shape.

| field | type | meaning |
|---|---|---|
| `version` | `1` | payload version |
| `event` | string | one of the five names above |
| `network` | string | `mainnet` \| `testnet` \| `signet` \| `regtest` |
| `txid` | string | transaction id, display-order hex |
| `confs` | number | `0` for `seen`/`dropped`/`demoted`; the milestone for `confirmed`; the last milestone fired for `conflicted` (`0` if none) |
| `matched` | `{address: string, vout: number, valueSats: number}[]` | every output paying a watched address; `[]` on an `evicted` drop whose stored record is missing |
| `blockHeight` | number \| null | `null` while unconfirmed; for `demoted`/`conflicted`, the height of the block that was orphaned |
| `blockHash` | string \| null | same rule as `blockHeight` |
| `hex` | string | the raw transaction; `""` on an `evicted` drop whose stored record is missing |
| `idempotencyKey` | string | see above |
| `timestamp` | number | unix ms; `confirmed` and proven `conflicted` use the block time, the others use wall-clock time |
| `reason` | string | only on `dropped` (`replaced` \| `evicted`) and proven `conflicted` (`double-spend`) |
| `replacedBy` | string | only on `dropped` with `reason: 'replaced'` |
| `conflictingTxid` | string | only on `conflicted` with `reason: 'double-spend'` |

One tx paying two watched addresses yields one event with two `matched` entries.

## seen

A tx paying a watched address entered the mempool. Requires milestone `0` in
`CONFIRMATION_MILESTONES`. Best-effort: a tx can be mined without weir ever seeing it
unconfirmed (daemon restart, ZMQ gap, direct-to-block). Handler action: show "payment
detected", start waiting for confirmations.

```json
{
  "version": 1,
  "event": "seen",
  "network": "mainnet",
  "txid": "d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a",
  "confs": 0,
  "matched": [
    { "address": "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq", "vout": 0, "valueSats": 50000000 }
  ],
  "blockHeight": null,
  "blockHash": null,
  "hex": "02000000000101…",
  "idempotencyKey": "mainnet:d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a:seen",
  "timestamp": 1758585600123
}
```

## confirmed

The tx reached a configured milestone depth. Fires once per milestone above `0` (with the
default `0,1,3`: at 1 and at 3; nothing at 2). The max milestone ends tracking. Handler
action: credit at whichever depth matches your risk tolerance.

```json
{
  "version": 1,
  "event": "confirmed",
  "network": "mainnet",
  "txid": "d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a",
  "confs": 1,
  "matched": [
    { "address": "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq", "vout": 0, "valueSats": 50000000 }
  ],
  "blockHeight": 935412,
  "blockHash": "00000000000000000001b7c4c1e48d76c5a37902165a270156b7a8d72728a054",
  "hex": "02000000000101…",
  "idempotencyKey": "mainnet:d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a:confirmed:1:00000000000000000001b7c4c1e48d76c5a37902165a270156b7a8d72728a054",
  "timestamp": 1758586010000
}
```

## dropped

A tracked unconfirmed tx left the mempool without being mined. It does not require a prior
`seen`: with milestone `0` disabled weir still tracks pending txs and can emit `dropped`.
Handler action: roll back "payment detected". With milestone `0` enabled, a rebroadcast
fires a fresh `seen` (same key as the first, see above).

| `reason` | when | extra field |
|---|---|---|
| `replaced` | another tx spent one of its inputs (RBF fee-bump or redirect), detected in the mempool or in a block | `replacedBy`: the spending txid |
| `evicted` | residual: the tx was gone from the mempool at the next block and no replacement was seen | — |

```json
{
  "version": 1,
  "event": "dropped",
  "network": "mainnet",
  "txid": "d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a",
  "confs": 0,
  "matched": [
    { "address": "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq", "vout": 0, "valueSats": 50000000 }
  ],
  "blockHeight": null,
  "blockHash": null,
  "hex": "02000000000101…",
  "reason": "replaced",
  "replacedBy": "7c1d9e0f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d",
  "idempotencyKey": "mainnet:d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a:dropped:replaced:7c1d9e0f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d",
  "timestamp": 1758586100456
}
```

## demoted

A reorg orphaned the tx's block and the tx is back in the mempool. `blockHeight` and
`blockHash` name the orphaned block. Handler action: revert to unconfirmed; new
`confirmed` events (with new keys) follow if it is re-mined.

```json
{
  "version": 1,
  "event": "demoted",
  "network": "mainnet",
  "txid": "d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a",
  "confs": 0,
  "matched": [
    { "address": "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq", "vout": 0, "valueSats": 50000000 }
  ],
  "blockHeight": 935412,
  "blockHash": "00000000000000000001b7c4c1e48d76c5a37902165a270156b7a8d72728a054",
  "hex": "02000000000101…",
  "idempotencyKey": "mainnet:d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a:demoted:00000000000000000001b7c4c1e48d76c5a37902165a270156b7a8d72728a054",
  "timestamp": 1758586700789
}
```

## conflicted

A reorg orphaned the tx's block and the tx is gone: a competing spend won. Terminal.
`confs` is the last milestone that fired; `blockHeight`/`blockHash` name the orphaned
block. When the new chain provably spent one of the tx's inputs, the event carries
`reason: 'double-spend'` and `conflictingTxid`; otherwise the fields are absent (conflicted
by elimination). Handler action: reverse any credit, alert a human.

```json
{
  "version": 1,
  "event": "conflicted",
  "network": "mainnet",
  "txid": "d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a",
  "confs": 1,
  "matched": [
    { "address": "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq", "vout": 0, "valueSats": 50000000 }
  ],
  "blockHeight": 935412,
  "blockHash": "00000000000000000001b7c4c1e48d76c5a37902165a270156b7a8d72728a054",
  "hex": "02000000000101…",
  "reason": "double-spend",
  "conflictingTxid": "7c1d9e0f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d",
  "idempotencyKey": "mainnet:d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a:conflicted",
  "timestamp": 1758586900000
}
```

## expired

A TTL'd watch reached the end of its lifetime, paid or not. weir removes the address from
the watch set and stops matching new payments to it. A tx already confirmed keeps firing
its remaining milestones. One still unconfirmed is re-matched when mined: if no output
pays a remaining watch, tracking ends quietly; if another output does, it confirms with
`matched` recomputed. The sweep runs on every successful tip settlement (each processed
block, and boot reconciliation), so the event fires at the next settlement after the
deadline. Handler action: stop expecting new payments on that address.

```json
{
  "version": 1,
  "event": "expired",
  "network": "mainnet",
  "address": "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
  "idempotencyKey": "mainnet:bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq:expired:1758672000000",
  "timestamp": 1758672345678
}
```

| field | type | meaning |
|---|---|---|
| `version` | `1` | payload version |
| `event` | `"expired"` | |
| `network` | string | network name |
| `address` | string | the watch that ended |
| `idempotencyKey` | string | embeds the expiry deadline (unix ms) |
| `timestamp` | number | unix ms |

## heartbeat

Proof-of-life, every `HEARTBEAT_INTERVAL` seconds (`0` = off). It bypasses the outbox: one
direct attempt per tick, a failed send is warned and the next tick is the retry. It reports
the outbox and the chain lag instead, so a consumer can tell "alive but stuck" without its
own chain source. `GET /ready` reports the same fields. Handler action: reset a dead-man's
switch; page if heartbeats stop, `chainLag` stays above `READY_MAX_LAG`, or `outboxDepth`
keeps growing.

```json
{
  "version": 1,
  "event": "heartbeat",
  "network": "mainnet",
  "tipHeight": 935412,
  "nodeHeight": 935412,
  "chainLag": 0,
  "watchCount": 1842,
  "memoryUsedPct": 3,
  "outboxDepth": 0,
  "outboxOldestAgeSec": null,
  "deadLetterCount": 0,
  "idempotencyKey": "mainnet:heartbeat:1758586800000",
  "timestamp": 1758586800000
}
```

| field | type | meaning |
|---|---|---|
| `tipHeight` | number \| null | last block weir processed; `null` before the first tip |
| `nodeHeight` | number \| null | the node's best height (one `getblockcount` per tick); `null` when the RPC failed |
| `chainLag` | number \| null | `nodeHeight − tipHeight`; `null` when either is unknown |
| `watchCount` | number | size of the watch set |
| `memoryUsedPct` | number \| null | `round(used ÷ maxmemory × 100)`, not clamped (can exceed 100); `null` when redis has no positive `maxmemory` |
| `outboxDepth` | number | events queued for delivery (still retrying) |
| `outboxOldestAgeSec` | number \| null | age of the oldest queued event; `null` when the outbox is empty |
| `deadLetterCount` | number | events given up on (capped at `OUTBOX_DEAD_MAX`) |
| `idempotencyKey` | string | embeds the tick timestamp |
| `timestamp` | number | unix ms |
