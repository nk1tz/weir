# Redis API

The watch set is a redis SET, and writing to it is the primary way to add watches: no HTTP,
no restart, no notify call. weir picks a new member up on the very next transaction it
evaluates. Two keys are public; every other `weir:{network}:*` key is the daemon's memory.
The HTTP alternative, with address validation, is [admin-api.md](admin-api.md).

## Watch an address

`weir:{network}:addresses` is a SET of address strings. `{network}` is the daemon's
`NETWORK` (`mainnet` | `testnet` | `signet` | `regtest`).

```sh
redis-cli SADD weir:mainnet:addresses bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq
```

There is no validation on this path: an address of the wrong network or an unsupported
script type is accepted and never matches. Supported scripts: p2pkh, p2sh, p2wpkh, p2wsh,
p2tr.

## Watch with an expiry

`weir:{network}:expiries` is a ZSET, member = address, score = expiry as unix
milliseconds. Add the address to both keys:

```sh
redis-cli SADD weir:mainnet:addresses bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq
redis-cli ZADD weir:mainnet:expiries 1767225600000 bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq
```

When the deadline passes, weir removes the address from both keys and emits an `expired`
event (paid or not): the watch's lifetime ends, whatever happened to it. A tx already
confirmed keeps firing its remaining milestones; one still unconfirmed stops being tracked
once mined. The sweep runs when weir processes a tip block, so the event fires at the first
block after the deadline. Payload: [webhooks.md](webhooks.md#expired).

To renew, `ZADD` a later score. To make a watch permanent, `ZREM` it from `expiries`.

## Unwatch

```sh
redis-cli SREM weir:mainnet:addresses bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq
redis-cli ZREM weir:mainnet:expiries bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq
```

A tx already confirmed keeps firing its remaining milestones; one still unconfirmed stops
being tracked once mined.

## Rules

- Redis is the source of truth for watches. Lose redis, lose the watch set (the shipped
  compose file enables AOF persistence).
- `maxmemory-policy` must be `noeviction`; weir refuses to start otherwise. Under memory
  pressure writes fail loudly instead of watches vanishing.
- The `SADD` path has no boot window: redis accepts the watch immediately and the next
  block weir processes sees it. (The admin API's write routes wait for boot reconciliation
  instead.)
- Every other `weir:{network}:*` key is the daemon's memory: read if curious, never write.

## The daemon's keys

For orientation only. Shapes are internal and change between versions; the authoritative
list is [DESIGN.md](DESIGN.md) "Redis schema".

| key | type | meaning |
|---|---|---|
| `tip` | HASH | `{hash, height}`: the chain tip as weir last saw it |
| `blocks` | ZSET | recent block hashes scored by height: the reorg-detection ring |
| `maturing` | ZSET | tracked txids scored by inclusion height |
| `maturing:{txid}` | HASH | the per-tx record (height, block hash, matched outputs, fired milestones, hex, inputs) |
| `pending` | SET | txids seen in the mempool, awaiting first confirmation |
| `limbo` | SET | txids whose block a reorg disconnected, awaiting re-resolution |
| `evaluated` | SET | mempool txids already evaluated since the last tip block (reparse dedupe) |
| `outpoint:{txid}:{vout}` | SET | claimant txids per spent prevout (replacement and double-spend detection) |
| `outbox` | ZSET | queued event ids scored by next attempt time: the delivery queue |
| `outbox:{eventId}` | HASH | one queued event: payload, attempts, createdAt, lastError |
| `outbox:created` | ZSET | queued event ids scored by enqueue time (the "oldest queued" index) |
| `outbox:dead` | ZSET | dead-lettered event ids scored by give-up time, capped at `OUTBOX_DEAD_MAX` |

All keys are prefixed `weir:{network}:`.
