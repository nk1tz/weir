# Production deployment

One VM, Docker, two compose projects (mainnet and regtest), no inbound ports. This is the
exact procedure used for the reference deployment; every command was run as written.

## Machine

- Ubuntu 24.04, 2 vCPU, 8 GB RAM, 160 GB local NVMe. Shared CPU is fine: weir and the node
  are idle between blocks. Local disk, not network block storage: chainstate is random-read
  heavy.
- 4 GB works with `dbcache=300` and `maxmempool=150` uncommented in `bitcoin.conf`.
- Disk budget, mainnet: chainstate ~13 GB, `prune=10000` ~10 GB of blocks, the UTXO snapshot
  file ~10 GB until deleted, two chainstates during background validation. 80 GB is the
  floor.

## Layout

```
/root/weir-mainnet/   docker-compose.prod.yml  docker/  .env  bitcoin.conf  snapshots/
/root/weir-regtest/   docker-compose.prod.yml  docker/  .env
```

One directory per stack. Compose names networks and volumes after the directory, so the
two projects cannot see each other. `docker compose down -v` deletes only the project in
the current directory — and for mainnet that is the watch set. Check `pwd` first.

`docker-compose.prod.yml` is standalone (not an override). Always pass it with `-f`.

## 1. Box

```bash
ufw allow OpenSSH && ufw default deny incoming && ufw default allow outgoing && ufw --force enable
apt-get update && apt-get install -y ca-certificates curl git python3 unattended-upgrades
# docker from docker.com's apt repo (docker-ce, docker-compose-plugin), then:
systemctl enable --now docker
```

Nothing publishes a port, so ufw's SSH-only rule is the whole firewall story.

## 2. Files

```bash
for d in weir-mainnet weir-regtest; do
  git clone https://github.com/nk1tz/weir.git ~/$d && git -C ~/$d checkout v0.2.1
done
```

Only `docker-compose.prod.yml`, `docker/`, and `.env.example` are used from the checkout.

## 3. Secrets

Run [examples/prod-secrets.sh](../examples/prod-secrets.sh) once on the box. It:

- generates the mainnet RPC credentials with Core's `rpcauth.py`, writes the hash line into
  `~/weir-mainnet/bitcoin.conf` (a copy of `docker/bitcoin-mainnet.conf`, outside the git
  checkout) and the password into `BITCOIN_RPC_URL`;
- generates `WEBHOOK_SECRET` and `ADMIN_TOKEN` for both stacks (`openssl rand -hex 32`);
- writes both `.env` files, mode 600, and prints only counts.

Two details it gets right that are easy to get wrong:

- `bitcoin.conf` must be readable by the container's `bitcoin` user, which is **uid 101** in
  `bitcoin/bitcoin:31`, not 1000. The script does `chown 101:101` and keeps mode 600.
  A root-only file makes bitcoind crash-loop with "config file could not be opened".
- `mkdir -p snapshots` happens before the first `up`; otherwise docker creates the bind
  directory root-owned.

Copy `ADMIN_TOKEN` and `WEBHOOK_SECRET` from each `.env` into your password manager and
your app's config. Rotate by editing `.env` and `docker compose -f docker-compose.prod.yml
up -d weir`. Watches and the outbox live in redis and survive.

The mainnet `.env` also needs `NETWORK=mainnet`, the real `WEBHOOK_URL`, and
`HEARTBEAT_INTERVAL=60` (see [Monitoring](../README.md#monitoring)).

## 4. Mainnet node from a UTXO snapshot

A fresh node replays every block since 2009: days. Core hard-codes the hash of the UTXO set
at one height per release (Core 31: 935,000), so a node can start from a file holding that
set, verify it against the built-in hash, and sync only the blocks since. Any mirror works;
`loadtxoutset` refuses a file whose hash does not match.

```bash
cd ~/weir-mainnet
docker compose -f docker-compose.prod.yml up -d bitcoind
# ~10 GB; https://bitcoin-snapshots.jaonoctus.dev/ lists a torrent and an HTTP mirror per height
curl -fsSL --retry 10 -C - -o snapshots/utxo-935000.dat https://files-vps02.jaonoctus.dev/utxo-935000.dat
```

Wait until `getblockchaininfo` shows `headers` past the snapshot height, then:

```bash
C="docker compose -f docker-compose.prod.yml exec -T --user bitcoin bitcoind \
   bitcoin-cli -conf=/etc/bitcoin/bitcoin.conf -datadir=/home/bitcoin/.bitcoin"
$C getblockchaininfo | grep -E '"(blocks|headers)"'
nohup $C -rpcclienttimeout=0 loadtxoutset /snapshots/utxo-935000.dat > snapshots/loadtxoutset.log 2>&1 &
```

`exec` bypasses the image entrypoint, hence the explicit user and datadir (the RPC cookie
lives in the datadir). Progress shows in `docker compose logs bitcoind` as
`[snapshot] N coins loaded`. Loading takes about an hour; syncing from the snapshot height
to the tip takes a few more. Background validation from genesis then runs for days and does
not block anything. When `$C getchainstates` shows the snapshot chainstate as active and
`blocks == headers`, delete the `.dat`.

## 5. Bring up weir

```bash
cd ~/weir-mainnet && docker compose -f docker-compose.prod.yml up -d
docker compose -f docker-compose.prod.yml logs -f weir      # until "weir is running"
```

Regtest is the same in `~/weir-regtest`; its node needs no snapshot.

`GET /ready` (on `ADMIN_PORT`, only reachable inside the compose network unless you put a
reverse proxy in front) answers 200 once reconciled and within `READY_MAX_LAG` of the node.

## Operating

- **Upgrade weir:** set `WEIR_VERSION` in `.env`, then
  `docker compose -f docker-compose.prod.yml pull weir && docker compose -f docker-compose.prod.yml up -d weir`.
  weir reconciles on boot; nothing is lost across a restart.
- **Upgrade the node:** bump `BITCOIND_VERSION` deliberately, after reading the release
  notes. The datadir volume persists.
- **Change the webhook URL:** edit `.env`, `up -d weir`. Queued outbox events go to the
  new URL.
- **Crashes:** weir exits on any unrecoverable error and `restart: unless-stopped` brings
  it back. A rising restart count in `docker compose ps` means the node or redis is
  unhealthy.
- **Downtime tolerance:** weir catches up every block it missed on boot. Longer than the
  node's prune window (`prune=10000`, roughly a week) and it resets tracking and logs it.
- **Logs:** `docker compose -f docker-compose.prod.yml logs --tail 100 weir`. json-file
  rotation is set on every service; bitcoind runs with `-nodebuglogfile`.
- **Backups:** redis is the only state you cannot rebuild. A weekly droplet snapshot covers
  it (and the chainstate, and `.env`, so treat snapshots as secret).
- **Monitoring:** `HEARTBEAT_INTERVAL=60` and alert in the receiver on a missing
  heartbeat, `chainLag > 2`, `deadLetterCount > 0`, or `outboxDepth` growing. See
  [README Monitoring](../README.md#monitoring). No inbound port is needed for any of it.
