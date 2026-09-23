#!/usr/bin/env bash
# One-shot: generate every weir secret on the production box. Prints nothing secret.
# Layout and rationale: docs/DEPLOY.md. Run once as root after cloning into ~/weir-mainnet
# and ~/weir-regtest; re-running rotates everything.
set -euo pipefail

# --- mainnet: bitcoind rpcauth -> bitcoin.conf (outside the repo copy) ---
cd ~/weir-mainnet; mkdir -p snapshots
curl -fsSL https://raw.githubusercontent.com/bitcoin/bitcoin/v31.0/share/rpcauth/rpcauth.py -o /root/rpcauth.py
python3 /root/rpcauth.py weir > /root/rpcauth.out
RPCAUTH_LINE=$(grep '^rpcauth=' /root/rpcauth.out)
RPCPW=$(awk '/Your password/{getline; print}' /root/rpcauth.out)
rm -f /root/rpcauth.out /root/rpcauth.py
sed "s|^#rpcauth=.*|$RPCAUTH_LINE|" docker/bitcoin-mainnet.conf > bitcoin.conf
chown 101:101 bitcoin.conf && chmod 600 bitcoin.conf   # uid 101 = the image's bitcoin user

# --- mainnet .env ---
sed -i \
  -e "s|^BITCOIN_RPC_URL=.*|BITCOIN_RPC_URL=http://weir:$RPCPW@bitcoind:8332|" \
  -e "s|^BITCOIND_CONF=.*|BITCOIND_CONF=/root/weir-mainnet/bitcoin.conf|" \
  -e "s|^WEBHOOK_SECRET=.*|WEBHOOK_SECRET=$(openssl rand -hex 32)|" \
  -e "s|^ADMIN_TOKEN=.*|ADMIN_TOKEN=$(openssl rand -hex 32)|" \
  .env
chmod 600 .env
# bitcoind now needs the real conf (rpcauth): recreate it on the new bind
docker compose -f docker-compose.prod.yml up -d bitcoind >/dev/null 2>&1

# --- regtest .env ---
cd ~/weir-regtest
[ -f .env ] || cp .env.example .env
sed -i \
  -e "s|^WEBHOOK_SECRET=.*|WEBHOOK_SECRET=$(openssl rand -hex 32)|" \
  -e "s|^ADMIN_TOKEN=.*|ADMIN_TOKEN=$(openssl rand -hex 32)|" \
  -e "s|^HEARTBEAT_INTERVAL=.*|HEARTBEAT_INTERVAL=60|" \
  .env
chmod 600 .env

echo "mainnet rpcauth lines in bitcoin.conf: $(grep -c '^rpcauth=weir:' ~/weir-mainnet/bitcoin.conf)"
echo "mainnet secrets set: $(grep -cE '^(WEBHOOK_SECRET|ADMIN_TOKEN)=[0-9a-f]{64}$' ~/weir-mainnet/.env)/2"
echo "regtest secrets set: $(grep -cE '^(WEBHOOK_SECRET|ADMIN_TOKEN)=[0-9a-f]{64}$' ~/weir-regtest/.env)/2"
echo "done"
