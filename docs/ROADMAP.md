# Roadmap

## v0.2 execution sequence (decided 2026-09-22)

Five independent reviews (two fresh-eyes, Codex, Kimi K3, driver) agreed: the core
design earns its keep; the plumbing is duplicated; state transitions and the edges are
under-tested, with real bugs in that gap. Order of work:

1. **Consolidation** — zero behavior change. One `FakeStore`, one matcher with one
   `SMISMEMBER` per block, one logger, one error-describer, `Pick<>` deps, delete dead
   store methods and double cleanup, trim spec-narrating docblocks.
2. **Standalone bug fixes**, each with a test: reparse retry (`current − evaluated`,
   drop `previous`), 413 must actually respond, `?cursor=abc` → 400, `ringAbove(0)`
   exclusive at height 0, drop the `evaluated` gate in block promotion and make
   promotion one MULTI, one fatal-error policy.
3. **Regtest smoke** — DONE 2026-09-22: real bitcoind + redis, one payment: `seen` →
   `confirmed:1` → `confirmed:3`, all HMAC-verified by examples/catch.js; boot-time gap walk
   caught up 101 blocks. Found and fixed two compose/script bugs (conf mount, bitcoin-cli -conf).
4. **Outbox** — DONE 2026-09-22 (f69364e). Spec: DESIGN.md "Outbox (durable delivery)". Every state transition is one Store MULTI that also enqueues its event;
   nothing awaits the network inside a transition (kills the `seen`/block race). One
   drainer, persisted backoff, capped dead-letter after OUTBOX_MAX_AGE. Heartbeat bypasses
   the outbox and reports it.
5. **Outpoint tracking** — DONE 2026-09-22 (claimant-SET model; 6 review rounds).
   **Chain-lag health `/live` `/ready` `/metrics`** — IN PROGRESS (branch v0.2/health; spec:
   DESIGN.md "Health from chain lag").
6. **Full regtest E2E** — reorg via `invalidateblock`, restart mid-flight, webhook down
   during a reorg. This is the v0.2 gate.

Known bugs (from the reviews) are tracked against steps 2 and 4 above.

## Gate before any of this: regtest end-to-end

Run weir against a real bitcoind + redis on regtest. Watch an address, send, mine,
force a reorg with `invalidateblock`, restart the daemon mid-flight. Confirm every
event fires as `docs/DESIGN.md` says. The unit tests prove the logic; only a real
node proves the wire.

## v0.2 — production readiness

Each item ships in its smallest correct form. weir stays a small primitive.

### 1. Durable outbox (correctness)

Problem: `dropped`, `demoted`, `conflicted`, `expired` are fire-and-forget. If the
webhook is down past the retry budget, the event is lost and the state transition
completes anyway. These are the "reverse the money" events — the worst ones to lose.

Fix:
- Enqueue the event in the SAME Redis MULTI/Lua as the state mutation. This is the
  non-negotiable part.
- A drainer delivers due entries (`nextAttemptAt` ZSET + per-event hash), backoff,
  delete on 2xx.
- Bounded: `OUTBOX_MAX_AGE`, then a capped dead-letter state with a loud log.
- Side effect: delivery decouples from block processing (a slow endpoint no longer
  stalls the pipeline).
- Contract change to document: delivery order becomes best-effort. Consumers must
  be order-independent (they already can be — events carry absolute state).
- Docs: the receiver MUST dedupe on `idempotencyKey` in the same DB transaction as
  the credit/reversal.

### 2. Outpoint tracking (detection)

Problem: weir discards the inputs of tracked txs. So an RBF replacement is only
noticed as `dropped` at the next block, and `conflicted` is inferred from mempool
absence rather than proven.

Fix:
- `weir:{net}:outpoints` — `txid:vout` → txid, for pending + maturing watched txs
  only. Added at `seen`, removed on every terminal transition. Bounded.
- On every incoming rawtx: if any input hits the set, the tracked tx it conflicts
  with is `dropped` immediately, reason `replaced`.
- During reorg resolution: compare new-chain inputs against limbo txs' inputs. A hit
  is a proven `conflicted`.
- Optional `reason` field on `dropped`: `replaced` | `evicted`.
- No txindex needed. Outpoints are literal bytes in the tx; weir only checks them
  against its own memory, never resolves what they fund.

### 3. Health from chain lag (detection)

Problem: `/health` returns 200 while ZMQ is disconnected or weir is behind the node.
`lastBlockAt` is null after restart and time-since-block is unreliable (block
intervals are Poisson).

Fix:
- `/live`: process/event loop alive. Never fails on webhook downtime.
- `/ready`: redis ok, rpc ok, reconciliation done, chain lag within bound.
- Chain lag = `node best height − weir tip height`, persisting. This is the signal.
- `/metrics` with weir-native signals only: chain lag, last zmq tx/block timestamps,
  webhook success/fail counts, outbox depth + oldest age, watch count, tip height.
  Host metrics (disk, restarts) are the platform's job.
- Heartbeat docs: the receiver must check `tipHeight` advances, not just that a
  heartbeat arrived.

### 4. Pinned, prebuilt images (repeatability)

Problem: `build: .` + `FROM node:22-alpine` is not reproducible and is a
supply-chain exposure.

Fix:
- Keep `build: .` for the getting-started path.
- Add `compose.prod.yml`: weir, redis, bitcoind pinned by digest.
- CI: test → typecheck → build image → push → deploy by digest.
- Bitcoin Core upgrades are deliberate: read release notes, test on regtest/signet,
  back up redis, upgrade, verify RPC + ZMQ + block processing + delivery.
- Secrets stay in an env file / secret manager, never in the image.
