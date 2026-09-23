# regtest quickstart

A full transcript: every command, and what you should see. Regtest is a private bitcoin
network where you mine your own blocks instantly — the whole loop (watch → pay → mempool
event → mine → confirmation events) takes about five minutes.

Hashes, addresses and timestamps below will differ on your machine, and log wording may
drift between versions — the shape is what matters.

Prerequisites: docker with compose v2, node ≥ 20, git.

## 1. Clone and configure

```console
$ git clone https://github.com/nk1tz/weir && cd weir
$ cp .env.example .env
```

The defaults in `.env.example` are already pointed at the compose stack's regtest bitcoind
and redis. Nothing to edit.

## 2. Start the stack

The `regtest` profile adds a throwaway bitcoind to the usual weir + redis pair. First run
builds the image (the zeromq native module compiles — give it a couple of minutes).

```console
$ docker compose --profile regtest up -d --build
...
[+] Running 4/4
 ✔ Network weir_default       Created
 ✔ Container weir-redis-1     Started
 ✔ Container weir-bitcoind-1  Started
 ✔ Container weir-weir-1      Started
```

Check the daemon came up clean:

```console
$ docker compose logs weir
weir-1  | [info] [index] weir v0.1.0 starting
weir-1  | [info] [index] network=regtest milestones=[0,1,3] webhook=host.docker.internal admin=off
weir-1  | [info] [index] redis connected
weir-1  | [info] [preflight] redis reachable (used 1.1 MB)
weir-1  | [info] [preflight] redis maxmemory-policy=noeviction — OK
weir-1  | [info] [preflight] bitcoind reachable: chain=regtest blocks=0 pruned=false
weir-1  | [info] [preflight] zmq publishers OK (pubrawtx=tcp://0.0.0.0:28332, pubrawblock=tcp://0.0.0.0:28332)
weir-1  | [info] [preflight] redis maxmemory 256 MB — estimated watch capacity ~399000 addresses
weir-1  | [info] [reconcile] first run — initialized tip to 0f9188f1…@0 (forward-only, no backfill)
weir-1  | [info] [zmq] subscribed to rawtx+rawblock at tcp://bitcoind:28332
weir-1  | [info] [index] weir is running
```

Two lines matter: `maxmemory-policy=noeviction — OK` (weir refuses to start under any
eviction policy — evicted keys would be silently forgotten watches) and `zmq publishers OK`.

If you set `ADMIN_TOKEN` in `.env` (and publish the port), `GET /ready` is the one-shot
health check — `ok` turns true once boot reconciliation is done and weir is within
`READY_MAX_LAG` blocks of the node:

```console
$ curl -s localhost:8787/ready
{"ok":true,"redis":true,"rpc":true,"reconciled":true,"tipHeight":0,"nodeHeight":0,"chainLag":0,"watchCount":0,"outboxDepth":0,"outboxOldestAgeSec":null,"deadLetterCount":0,"lastZmqTxAgeSec":null,"lastZmqBlockAgeSec":null}
```

No token needed for `/ready`, `/live` or `/metrics`. See the README's "Monitoring" section.

## 3. Start the event catcher

In a second terminal, run the bundled receiver. It verifies `x-weir-signature` and
pretty-prints every event:

```console
$ WEBHOOK_SECRET=change-me-openssl-rand-hex-32 node examples/catch.js
[catch] listening on :9090, verifying signatures
```

Leave it running. (The secret matches the `WEBHOOK_SECRET` in `.env` — if you changed it
there, change it here.)

## 4. Fund a wallet and watch an address

Back in the first terminal. An alias saves typing:

```console
$ alias btc='docker compose exec bitcoind bitcoin-cli -conf=/etc/bitcoin/bitcoin.conf -regtest -rpcuser=weir -rpcpassword=weir'
$ btc createwallet demo
{
  "name": "demo"
}
$ btc -generate 101
{
  "address": "bcrt1qm7yp3nqz3urrnw5wm2f4y08weyxjrhkm5c5nl4",
  "blocks": [
    "4e9c0b8f4a4a2b8f0d1e6c7a9b3f5d2e8c1a0f9b7d6e5c4a3b2f1e0d9c8b7a6f",
    ...100 more...
  ]
}
```

(101 blocks because coinbase outputs need 100 confirmations before they're spendable.)

Now the address we care about — get a fresh one and add it to the watch set. The redis set
`weir:regtest:addresses` **is** the API:

```console
$ ADDR=$(btc getnewaddress)
$ echo $ADDR
bcrt1q9h7qmc3dl5xg2p8w4zf0k6ejt2s5v8n3u7yale
$ docker compose exec redis redis-cli SADD weir:regtest:addresses $ADDR
(integer) 1
```

That's it. weir picks it up on the very next transaction it evaluates — no restart, no
notify call.

## 5. Pay it — the seen event

```console
$ btc sendtoaddress $ADDR 0.5
d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a
```

The tx hits the mempool, bitcoind publishes it over ZMQ, weir matches the output. Within a
moment, the catcher terminal shows:

```console
[catch] seen d4a3f0c9… signature ok
{
  "version": 1,
  "event": "seen",
  "network": "regtest",
  "txid": "d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a",
  "confs": 0,
  "matched": [
    {
      "address": "bcrt1q9h7qmc3dl5xg2p8w4zf0k6ejt2s5v8n3u7yale",
      "vout": 0,
      "valueSats": 50000000
    }
  ],
  "blockHeight": null,
  "blockHash": null,
  "hex": "02000000000101…",
  "idempotencyKey": "regtest:d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a:seen",
  "timestamp": 1751545678901
}
```

`vout` may be 0 or 1 — the wallet shuffles the change position. `matched` lists every
output paying a watched address, so one tx paying two watched addresses yields one event
with two entries.

## 6. Mine — the confirmed events

```console
$ btc -generate 1
```

The catcher prints the 1-confirmation milestone:

```console
[catch] confirmed d4a3f0c9… confs=1 signature ok
{
  "version": 1,
  "event": "confirmed",
  "network": "regtest",
  "txid": "d4a3f0c9b8e2715a6f0e9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a3f2e1d0c9b8a",
  "confs": 1,
  "matched": [
    {
      "address": "bcrt1q9h7qmc3dl5xg2p8w4zf0k6ejt2s5v8n3u7yale",
      "vout": 0,
      "valueSats": 50000000
    }
  ],
  "blockHeight": 102,
  "blockHash": "1f2e3d4c5b6a7f8e9d0c1b2a3f4e5d6c7b8a9f0e1d2c3b4a5f6e7d8c9b0a1f2e",
  "hex": "02000000000101…",
  "idempotencyKey": "regtest:d4a3f0c9…:confirmed:1:1f2e3d4c…",
  "timestamp": 1751545712000
}
```

Note the idempotency key embeds the milestone *and* the block hash — if a reorg re-mines
this tx into a different block, the re-fired milestone gets a new key.

Two more blocks reach the `3` milestone, after which weir stops tracking the tx (the max
milestone is the end of the tracking window):

```console
$ btc -generate 2
```

```console
[catch] confirmed d4a3f0c9… confs=3 signature ok
```

There is no event at 2 confirmations — only configured milestones (`0,1,3`) fire.

The whole sequence above is also scripted, narrated, as `./examples/regtest-demo.sh`.

## 7. Try breaking it

### Kill the daemon, mine behind its back

weir's crash story is boot reconciliation: it stores its last-seen tip in redis and replays
whatever the chain did while it was gone. Watch it work.

Stop the daemon, then create a payment *and* confirm it while nobody's listening:

```console
$ docker compose stop weir
$ btc sendtoaddress $ADDR 0.25
7c1d9e0f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d
$ btc -generate 1
```

The catcher stays silent — there's no daemon to deliver anything. Now bring it back:

```console
$ docker compose start weir
$ docker compose logs -f weir
weir-1  | [info] [preflight] redis maxmemory-policy=noeviction — OK
...
weir-1  | [info] [reconcile] tip 3c1e…@104 behind/diverged from node best 5a7d… — processing catch-up
weir-1  | [info] [blockPipeline] first run — … (or: promoted pending … / never-seen 7c1d9e0f… mined paying a watched address — maturing at 5a7d…@105)
weir-1  | [info] [blockPipeline] processed block 5a7d…@105 (2 txs)
weir-1  | [info] [reconcile] reconcile complete
```

And the catcher receives:

```console
[catch] confirmed 7c1d9e0f… confs=1 signature ok
```

Two things to notice:

- **No `seen` event, and there never will be one.** weir was down for the tx's entire
  mempool life; it caught it at the block level during reconciliation. This is why `seen`
  is documented as best-effort — never gate your `confirmed` handling on a prior `seen`.
- **No confirmations were lost.** The block mined during downtime was processed exactly as
  if weir had been running. Mine two more (`btc -generate 2`) and `confirmed` at 3 arrives
  on schedule.

This is also the crash-handling model in general: on any unexpected internal error the
daemon logs and exits, docker restarts it, and reconciliation makes that safe.

### Bonus: force a reorg

Pay and confirm once more, then invalidate the tip so the chain rewinds and rebuilds:

```console
$ btc sendtoaddress $ADDR 0.1        # → seen, then:
$ btc -generate 1                    # → confirmed confs=1 in block 108
$ btc invalidateblock $(btc getbestblockhash)
$ btc -generate 2
```

The new block at height 108 has a different hash and a different parent than the tip weir
stored, so the reorg path runs: find the fork point, resolve the transactions from the
disconnected block. Our tx went back to the mempool and was immediately re-mined into the
replacement block, so its milestone state resets and re-fires:

```console
[catch] confirmed <txid>… confs=1 signature ok    # same txid, NEW blockHash, NEW idempotencyKey
```

Same tx, same milestone, different block — and a different idempotency key. This is the
concrete reason consumers dedupe on `idempotencyKey` rather than on `(txid, milestone)`:
a re-fire after a reorg is *new information* (the confirmation now lives in a different
block), not a duplicate.

Had the replacement chain excluded the tx instead, you'd have seen `demoted` (back to
mempool) or `conflicted` (gone for good — a double-spend won).

## End-to-end suite

Everything above, plus the paths you cannot hit by hand in five minutes (RBF replacement,
redirect, reorg → `demoted`, reorg + double-spend → proven `conflicted`, TTL expiry, webhook
down, daemon restart mid-flight, `/ready` during catch-up), is scripted as the v0.2 gate:

```console
$ ./examples/regtest-e2e.sh
== 1. happy path
   ✓ seen → confirmed:1 → confirmed:3
...
== ALL SCENARIOS PASSED
```

It recreates the stack from scratch (`down -v`), starts `examples/catch.js` itself on :9090,
drives bitcoind through nine scenarios, asserts the exact `[catch]` lines (event, txid,
`reason`, `replacedBy`, `conflictingTxid`, block hash), checks that every `idempotencyKey`
arrived exactly once and that weir logged no `[error]` line, then tears the stack down.
Needs: docker compose v2, node ≥ 20 on the host, `.env` with `WEBHOOK_URL` pointing at
`host.docker.internal:9090`, `WEBHOOK_SECRET` and `ADMIN_TOKEN` set. About two minutes.
`KEEP_STACK=1` leaves the stack up for inspection; the catcher log is
`/tmp/weir-e2e-catch.log` (`CATCH_LOG` overrides it).

## 8. Teardown

```console
$ docker compose --profile regtest down -v
```

`-v` deletes the redis and regtest chain volumes — full reset.

## Troubleshooting

- **No events arrive, weir logs webhook delivery failures.** The default `WEBHOOK_URL`
  uses `host.docker.internal`, which Docker Desktop (mac/windows) provides automatically.
  On Linux, add `extra_hosts: ["host.docker.internal:host-gateway"]` to the weir service,
  or point `WEBHOOK_URL` at an address reachable from the container.
- **`[catch] … signature MISMATCH`.** The `WEBHOOK_SECRET` passed to `catch.js` doesn't
  match the one in `.env`.
- **weir exits at preflight with a maxmemory-policy error.** You pointed `REDIS_URL` at a
  redis configured with an eviction policy. weir requires `noeviction` — anything else
  can silently delete watches under memory pressure.
- **weir exits at preflight with a ZMQ error.** Your bitcoind is missing `zmqpubrawtx` /
  `zmqpubrawblock`; the error message prints the exact `bitcoin.conf` lines to add. (The
  compose regtest bitcoind ships with them.)
