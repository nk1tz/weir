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
   **Chain-lag health `/live` `/ready` `/metrics`** — DONE 2026-09-22 (spec: DESIGN.md
   "Health from chain lag"; 1 review round: write gating until reconciled, RPC deadlines,
   `!shuttingDown` in readiness, metrics registry type guard).
6. **Full regtest E2E** — DONE 2026-09-22: `examples/regtest-e2e.sh`, nine scripted scenarios
   against a real bitcoind 28 + redis (happy path, RBF bump, redirect, reorg → demoted →
   re-confirmed, reorg + double-spend → proven `conflicted`, TTL expiry, webhook down, restart
   mid-flight, `/ready` 503 → 200), asserting the exact events the catcher receives. Found no
   weir defect: every event fired as DESIGN.md says, no duplicate idempotency keys, no stray
   redis state, zero error lines. This is the v0.2 gate.

7. **v0.2 single-writer refactor** — DONE 2026-09-22 (branch `v0.2/single-writer`, version
   0.2.0). A requirements-pruning pass found that every guard in the engine existed only
   because it had been wired as three concurrent writers (fire-and-forget rawtx evaluations,
   a self-mutexed reparse timer, a serial block queue) and six review rounds had defended
   that accident. Decision (product owner): weir is ONE writer — every engine action is one
   item on one queue (src/engine/queue.ts), every state change is one plain MULTI, and there
   are no guards, fences, tombstones, watermarks or conditional scripts anywhere. A block is
   one MULTI (tip included: crash before exec = nothing happened), and the tip-only work
   (eviction, TTL, `evaluated` prune, limbo resolution) runs only when the block is the
   node's current tip. Product behaviour unchanged; the E2E gate passes twice from scratch.
   Removed, in counts: 5 key families (`tombstones`, `retired`, `mempool:current`,
   `mempool:postBlock`, `block:txids`), 2 constants (`MAX_EVALUATION_AGE_MS`,
   `TOMBSTONE_TTL_MS`), 10 Lua scripts → 0, 21 Store methods (60 → 39), 70 test cases net
   (355 → 285, guard/race/Lua-text pins deleted, behavioural tests kept or rewritten), 644
   source lines net (2505 → 1893 across the touched src files). Codex review then found
   one class the rule did not cover — bitcoind moving between a read and a write INSIDE a
   queue item — fixed without any Redis condition: limbo resolves from the tip path's
   validated mempool snapshot (never a later probe); boot reconciles until the stored tip
   is the node's best and settles through that same path (an off-chain stored tip is
   rewound first); an evaluation confirms the tx is in the node's mempool before it writes
   a `seen` or a replacement. E2E scenario 10 (restart mid-reorg) added. DESIGN.md gained "Single writer"
   and "Transaction lifecycle" (the state machine every code path is a transition of).

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
- Enqueue the event in the SAME Redis MULTI as the state mutation. This is the
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

**Prebuilt: DONE 2026-09-23** — `release.yml` pushes `ghcr.io/nk1tz/weir:<version>`, `:<major.minor>`, `:latest` on every semver `v*` tag (prereleases get only their own tag); `docker-compose.prod.yml` is the standalone prod stack (weir by tag, `redis:7.4-alpine`, `bitcoin/bitcoin:31`), one directory + `.env` per network. **Digest pinning: open** — tags are still mutable.

Problem: `build: .` + `FROM node:22-alpine` is not reproducible and is a
supply-chain exposure.

Fix:
- Keep `build: .` for the getting-started path.
- Add `compose.prod.yml`: weir, redis, bitcoind pinned by digest.
- CI: test → typecheck → build image → push → deploy by digest.
- Bitcoin Core upgrades are deliberate: read release notes, test on regtest/signet,
  back up redis, upgrade, verify RPC + ZMQ + block processing + delivery.
- Secrets stay in an env file / secret manager, never in the image.
