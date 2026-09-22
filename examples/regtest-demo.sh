#!/usr/bin/env bash
#
# regtest-demo.sh — end-to-end weir demo on a throwaway regtest chain.
#
# What it does:
#   1. brings up weir + redis + bitcoind (compose profile 'regtest')
#   2. creates a wallet, mines 101 blocks (matures a coinbase)
#   3. watches a fresh address by SADD-ing it into redis
#   4. sends coins to it            -> expect a 'seen' event in catch.js
#   5. mines 1 block                -> expect 'confirmed' (confs=1)
#   6. mines 2 more                 -> expect 'confirmed' (confs=3, default milestones 0,1,3)
#
# Prerequisites:
#   - .env in the repo root (cp .env.example .env), with WEBHOOK_URL pointing at
#     catch.js, e.g. http://host.docker.internal:9090/webhook
#   - the catcher running on the host, in another terminal:
#       WEBHOOK_SECRET=<same as .env> node examples/catch.js

set -euo pipefail

cd "$(dirname "$0")/.."

step() { printf '\n== %s\n' "$*"; }
note() { printf '   %s\n' "$*"; }

bcli() { docker compose exec -T bitcoind bitcoin-cli -conf=/etc/bitcoin/bitcoin.conf "$@"; }
rcli() { docker compose exec -T redis redis-cli "$@"; }

if [[ ! -f .env ]]; then
  echo "error: .env not found. Run: cp .env.example .env  (then set WEBHOOK_SECRET)" >&2
  exit 1
fi

step "docker compose --profile regtest up -d --build"
note "starting weir + redis + bitcoind. weir may restart a few times until bitcoind is ready — that's fine."
docker compose --profile regtest up -d --build

step "waiting for bitcoind RPC"
for i in $(seq 1 60); do
  if bcli getblockchaininfo >/dev/null 2>&1; then
    break
  fi
  if [[ "$i" == 60 ]]; then
    echo "error: bitcoind did not become ready in 60s; check: docker compose logs bitcoind" >&2
    exit 1
  fi
  sleep 1
done
note "bitcoind is up (chain: $(bcli getblockchaininfo | grep -o '"chain": *"[a-z]*"'))"

step "creating wallet 'demo'"
if bcli createwallet demo >/dev/null 2>&1; then
  note "wallet 'demo' created"
elif bcli loadwallet demo >/dev/null 2>&1; then
  note "wallet 'demo' already existed — loaded"
else
  note "wallet 'demo' already loaded"
fi

step "mining 101 blocks (coinbase needs 100 confirmations to become spendable)"
MINER_ADDR="$(bcli getnewaddress miner)"
bcli generatetoaddress 101 "$MINER_ADDR" >/dev/null
note "height is now $(bcli getblockcount)"

step "getting a fresh address to watch"
WATCH_ADDR="$(bcli getnewaddress watched)"
note "address: $WATCH_ADDR"

step "watching it: redis-cli SADD weir:regtest:addresses $WATCH_ADDR"
rcli SADD "weir:regtest:addresses" "$WATCH_ADDR" >/dev/null
note "watch set size: $(rcli SCARD weir:regtest:addresses | tr -d '[:space:]')"

step "sending 0.5 BTC to the watched address"
TXID="$(bcli sendtoaddress "$WATCH_ADDR" 0.5)"
note "txid: $TXID"
note ">>> catch.js should now print:  seen txid=$TXID confs=0 sats=50000000 addr=$WATCH_ADDR"

sleep 2

step "mining 1 block to confirm it"
bcli generatetoaddress 1 "$MINER_ADDR" >/dev/null
note ">>> catch.js should now print:  confirmed txid=$TXID confs=1 ..."

sleep 2

step "mining 2 more blocks for the 3-conf milestone"
bcli generatetoaddress 2 "$MINER_ADDR" >/dev/null
note ">>> catch.js should now print:  confirmed txid=$TXID confs=3 ..."
note "(after confs=3 — the max default milestone — weir stops tracking this tx)"

step "done"
note "tear down with: docker compose --profile regtest down    (add -v to wipe chain + redis)"
