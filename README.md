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

## 5-minute regtest quickstart

```sh
git clone https://github.com/nk1tz/weir && cd weir
cp .env.example .env                          # defaults are regtest-ready

docker compose --profile regtest up -d --build   # weir + redis + a regtest bitcoind

# terminal 2 — a webhook receiver that verifies signatures and pretty-prints events
WEBHOOK_SECRET=change-me-openssl-rand-hex-32 node examples/catch.js

# terminal 3 — creates a wallet, watches an address, pays it, mines blocks
./examples/regtest-demo.sh
```

Watch terminal 2: a `seen` event when the payment hits the mempool, then `confirmed` at
1 and 3 confirmations. The full transcript with expected output for every command is in
[docs/quickstart-regtest.md](docs/quickstart-regtest.md). Production (one VM, prebuilt image,
mainnet node bootstrapped from a UTXO snapshot): [docs/DEPLOY.md](docs/DEPLOY.md).

## Add a watch

The redis set `weir:{network}:addresses` is the input API; write to it from anything that
speaks redis. With `ADMIN_TOKEN` set, a bearer-token HTTP API does the same with address
validation and TTLs. Details: [docs/redis-api.md](docs/redis-api.md) and
[docs/admin-api.md](docs/admin-api.md).

```sh
redis-cli SADD weir:mainnet:addresses bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq

curl -X POST localhost:8787/watches -H 'authorization: Bearer <ADMIN_TOKEN>' \
  -H 'content-type: application/json' -d '{"address":"bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"}'
```

## Receive events

Events arrive as JSON POSTs; every POST carries an `x-weir-signature` HMAC header.

Delivery is at-least-once from a durable outbox, and retries reorder events. Dedupe on
`idempotencyKey` in the same database transaction as the credit or reversal you perform,
and never assume arrival order: every event carries absolute state, never a delta.
[examples/receiver.js](examples/receiver.js) is a complete framework-free receiver; every
event and the retry policy are in [docs/webhooks.md](docs/webhooks.md).

## Docs

Using weir:

- [docs/webhooks.md](docs/webhooks.md) — the webhook contract: signature, retries, idempotency, every event
- [docs/admin-api.md](docs/admin-api.md) — the HTTP admin API: probes, metrics, watch routes
- [docs/redis-api.md](docs/redis-api.md) — adding watches straight in redis; the key namespace
- [docs/configuration.md](docs/configuration.md) — every environment variable and the capacity formula
- [docs/operations.md](docs/operations.md) — monitoring, what to alert on, known limitations
- [docs/quickstart-regtest.md](docs/quickstart-regtest.md) — the regtest walkthrough with expected output
- [docs/DEPLOY.md](docs/DEPLOY.md) — production deployment on one VM

Internals:

- [docs/DESIGN.md](docs/DESIGN.md) — the design specification: single writer, redis schema, lifecycle, outbox
- [docs/ROADMAP.md](docs/ROADMAP.md) — what is planned next
- [docs/development.md](docs/development.md) — tests, the real-redis suite, the E2E gate

## License

MIT — see [LICENSE](LICENSE).
