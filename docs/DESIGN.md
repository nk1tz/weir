# weir — design specification

Watch bitcoin addresses, get signed webhooks. Runs beside a **pruned** Bitcoin Core node —
no txindex, no address index. Matching is set-membership: the mempool/block stream flows
through, weir catches transactions paying watched addresses (a weir is a fish trap in a river).

This document is the authoritative spec. Code must conform to it; deviations require editing
this file in the same change.

## What weir is / is not

- IS: a single daemon that reads a watch set from redis, listens to bitcoind over ZMQ,
  and POSTs HMAC-signed events to ONE webhook URL per network.
- IS NOT: multi-tenant. No accounts, no per-address webhook routing, no fanout — that is
  the consumer's layer. Receive-only (transaction OUTPUTS paying watched addresses; spend
  detection would require txindex and is out of scope). Forward-only (no history backfill).
  Reorgs deeper than the max milestone are invisible by design.

## Topology

Three containers per network: `weir` (this daemon), `redis`, `bitcoind` (bring-your-own or
compose profile). The daemon has ZERO listening ports unless `ADMIN_TOKEN` is set (then
exactly one). All its connections are outbound: RPC + ZMQ to bitcoind, redis, HTTPS to the
webhook. Multiple networks = parallel stacks; a shared redis is safe because every key is
prefixed `weir:{network}:`. weir targets a SINGLE redis instance: its transitions are
multi-key MULTI/EVAL without hash tags, so Redis Cluster is not a target.

## Config (src/config.ts — WRITTEN, do not change signatures)

Required: `NETWORK` (mainnet|testnet|signet|regtest), `BITCOIN_RPC_URL` (creds in URL),
`BITCOIN_ZMQ_URL`, `REDIS_URL`, `WEBHOOK_URL`, `WEBHOOK_SECRET`.
`WEBHOOK_URL` must be plain http(s) with NO embedded credentials (fail at load): webhook
authenticity is the HMAC's job, and a credentialed URL is rejected by fetch at delivery time
with an error message that would leak the password into logs.
Optional: `CONFIRMATION_MILESTONES` (default `0,1,3`; 0 = seen events enabled; max value =
tracking window = reorg shield), `WATCH_DEFAULT_TTL` (0 = forever), `HEARTBEAT_INTERVAL`
(0 = off), `ADMIN_TOKEN` (unset = no HTTP server *exists*), `ADMIN_PORT` (8787),
`WEBHOOK_TIMEOUT_MS` (10000), `OUTBOX_MAX_AGE` (seconds an undelivered event is retried
before it is dead-lettered; default 259200 = 3 days), `OUTBOX_DEAD_MAX` (dead-letter cap,
default 1000), `READY_MAX_LAG` (chain-lag bound for /ready, default 2). There is no `WEBHOOK_MAX_RETRIES`: retry is the outbox's job, bounded by age.

## Redis schema (src/store/keys.ts — WRITTEN)

See `keysFor()`. Public: `addresses` SET (+ `expiries` ZSET member=address score=expiresAt-ms).
Durable chain view: `tip` HASH {hash,height}; `blocks` ZSET (member=hash, score=height,
pruned to `ringSize`); `maturing` ZSET (member=txid, score=inclusion height);
`maturing:{txid}` HASH {height, blockHash, matched(JSON), fired(JSON array), hex, inputs(JSON
Outpoint[]; absent on records written before outpoint tracking — read as `[]`)}.
Working set (reconstructible): `pending`, `evaluated`, `mempool:current`, `mempool:postBlock`,
`blockTxids` — all SETs of txids.
Reorg state (durable): `limbo` SET — txids whose inclusion block was disconnected by a reorg,
awaiting re-resolution (re-included by the new chain / demoted to mempool / conflicted). Kept
in redis so a crash mid-reorg finishes resolving on the next block or boot.
Outpoints: `outpoint:{txid}:{vout}` SET of claimant txids (inputs of pending + maturing
txs; see Outpoint tracking).
Outbox (durable): `outbox` ZSET member=eventId score=nextAttemptAt-ms (the delivery queue);
`outbox:{eventId}` HASH {payload(JSON WeirEvent), event, idempotencyKey, attempts, createdAt,
lastError}; `outbox:created` ZSET member=eventId score=createdAt-ms (mirrors `outbox`
membership — the exact "oldest queued event" index); `outbox:dead` ZSET member=eventId
score=deadAt-ms (same hash keys; capped at `OUTBOX_DEAD_MAX`, oldest dropped with their
hashes). eventId = `<nowMs padded 15>-<seq within that ms padded 8>-<8 hex random>`
(`makeEventId`; the sequence restarts at 0 whenever the millisecond changes): monotonic, so
redis' tie order for equal scores (by member) is enqueue order.
Tombstones (durable, self-pruning): `tombstones` ZSET member=txid score=doneAt-ms — txids
whose tracking ENDED (final milestone, conflicted); a stale mempool evaluation must not
resurrect them (see `recordSeen`), and the block path's never-seen promotion skips them.
ABSOLUTE: any evaluation of a tombstoned txid is refused. Pruned on every tip block below
now − TOMBSTONE_TTL_MS (3,600,000; constant). The tombstone bounds the STORE side; the
EVALUATION FENCE (`MAX_EVALUATION_AGE_MS` = 600,000, far below the TTL) bounds the evaluator
side, so no evaluation can outlive the tombstone that guards it. `conflicted` needs no
permanent set: an invalid tx cannot re-enter the mempool, and the only other path back, a
stale evaluation, is fenced.
Retirement watermark (durable, self-pruning): `retired` ZSET member=txid score=exitAt-ms —
txids that LEFT pending without ending (`dropPending`, `replacePending`; same Lua). NOT a
tombstone: a drop is not terminal (a rebroadcast may re-fire `seen`, and a dropped-then-mined
tx must still confirm through the block path), so the watermark is RELATIVE: `recordSeen`
refuses (stale) an evaluation whose `startedAtMs <= exitAt` — a duplicate evaluation (ZMQ
rawtx + reparse fetch) that was in flight when the tx left would otherwise resurrect it as a
fresh pending record and earn a second `dropped` under a different key — while an evaluation
started after the exit (a real rebroadcast) records normally and leaves the watermark in
place (the next exit overwrites it). Pruned with the tombstones (same TTL; only the window
inside MAX_EVALUATION_AGE_MS matters).

## Events (src/lib/types.ts — WRITTEN)

`seen | confirmed | dropped | demoted | conflicted | expired | heartbeat`.
TxEvent payload: `{version:1, event, network, txid, confs, matched:[{address,vout,valueSats}],
idempotencyKey, timestamp, blockHeight, blockHash, hex}` plus, per Outpoint tracking rule 6,
`reason`/`replacedBy` on `dropped` and `reason`/`conflictingTxid` on proven `conflicted`.
`expired` is address-scoped: `{version:1, event:'expired', network, address, idempotencyKey, timestamp}`.
`heartbeat`: `{version:1, event:'heartbeat', network, tipHeight, nodeHeight, chainLag, watchCount,
memoryUsedPct, outboxDepth, outboxOldestAgeSec, deadLetterCount, idempotencyKey, timestamp}`.
Idempotency key shapes are in `src/store/keys.ts` (`idem`). Timestamps: unix ms. For
`confirmed`/`demoted`/`conflicted` use blockTime*1000 where a block drives the event, else `Date.now()`.

Delivery is at-least-once and ORDER IS BEST-EFFORT (see Outbox). HMAC-SHA256 header:
`x-weir-signature: t=<unix seconds>, v1=<hex hmac_sha256(secret, "<t>." + rawBody)>`.
2xx = delivered; anything else (non-2xx, network error, `WEBHOOK_TIMEOUT_MS` abort) is a failed
attempt that the outbox retries. Consumers MUST dedupe on `idempotencyKey` in the same
database transaction as the credit/reversal they perform, and must not assume arrival order:
every event carries absolute state (event type, confs, block hash in the key), never a delta.

## Outbox (durable delivery)

Why: the events that REVERSE money (`dropped`, `demoted`, `conflicted`, `expired`) must not
be lost when the webhook is down, and no state transition may await the network (a slow
delivery inside a transition let a block race the `seen` write and corrupt the record).

Rules:
1. A state transition and its event are ONE atomic Redis step. Every Store transition method
   that produces an event takes the event and appends `HSET outbox:{id} …` + `ZADD outbox now id`
   to the same MULTI as its state mutation. Either both persist or neither does. The
   exceptions in MECHANISM (not in atomicity) are Lua scripts (EVAL) rather than MULTIs:
   `recordSeen`, `replacePending` and `dropPending`, because they race other paths (a tx can
   be mined, or replaced from the mempool, while a caller is between its read and its write)
   and need guards a MULTI cannot express, and every transition that releases outpoints
   (`conflict`, `finishMaturing`, `endTracking` too), whose release reads the record's
   `inputs` and SREMs only the releaser's own txid from each claimant SET.
   GUARDS: a transition re-validates live state INSIDE its Lua wherever two writers can
   race. The mempool evaluator races the block pipeline, so `recordSeen`, `replacePending`
   and the tip-check `dropPending` are guarded (evaluated / mined / tombstoned / pendingUnmined
   / fence) and return 0 to a stale caller — never a second verdict. The block + reorg path
   is ONE serialized writer (makeBlockHandler's queue; reconcile runs before ZMQ starts) and
   the evaluator never mutates maturing/limbo state, so `conflict`, `finishMaturing`,
   `endTracking`, `demoteToPending`, `promoteToMaturing`, `markFired` read-then-write within
   one block's processing and carry no guard by design.
2. Nothing in the engine awaits delivery. Engine code never calls the sink.
3. One drainer (src/delivery/outbox.ts) delivers: every `OUTBOX_POLL_MS` (1000, constant) it
   takes due events (`ZRANGEBYSCORE outbox -inf now LIMIT 0 50`), reads each hash, and sends
   them SERIALLY in score order (ties by the monotonic id = enqueue order, so events enqueued
   together arrive in order when the endpoint is healthy; across failures order is
   best-effort). The interval pass keeps taking batches while they come back full (a backlog
   drains at line rate). Per event: 2xx → `outboxAck` (MULTI DEL hash, ZREM outbox, ZREM
   created); failure → attempts+1, `lastError`, and either `outboxRetry` (ZADD score =
   now + backoff, backoff = min(1000·2^(attempts−1), 300000) ms + up to 25% jitter) or,
   once `now − createdAt ≥ OUTBOX_MAX_AGE`, `outboxDead` (ZREM outbox + created, ZADD dead,
   then cap: drop the oldest dead entries and their hashes beyond `OUTBOX_DEAD_MAX` — ONE Lua)
   with an error-level log naming the idempotencyKey. Both are Lua scripts that are NO-OPS
   when the hash no longer exists (a concurrent ack won), so a partial hash can never be
   created. A dangling id (hash missing) is ZREM'd.
4. The drainer's own failures (redis) are fatal (background path). The sink never throws.
5. Crash after a 2xx but before the ack → the event is sent again: this is the at-least-once
   duplicate consumers dedupe on `idempotencyKey`.
6. `heartbeat` does NOT use the outbox: it is proof-of-life, delivered directly with one
   attempt; a stale queued heartbeat carries no information. It reports the outbox instead.
7. Boot: the drainer starts right after preflight and ONE `drainOnce()` is awaited before
   reconcile (pending events from before a crash go out first). Under a FULL redis
   (maxmemory reached, `noeviction`): each ack is a DEL/ZREM and frees memory; a failed
   delivery's retry write is a deny-oom command and fails → fatal → docker restarts → the next
   boot drain tries again — a restart loop that ends as soon as deliveries succeed, never a
   silent stall. If the endpoint is merely down the pass just reschedules. Shutdown stops
   the poll loop and waits for the
   in-flight drain pass. Every pass — interval or a direct `drainOnce()` — runs through ONE
   shared in-flight promise: a caller that finds a pass running waits for it, then runs; two
   passes never overlap and never double-send.
8. EVALUATION FENCE: every evaluation carries `startedAtMs` — captured at ZMQ rawtx receipt
   (makeRawTxHandler) and, in the reparser, immediately BEFORE getrawtransaction is issued.
   `recordSeen(rec, event, startedAtMs)` refuses (`stale`, warns with the age) when
   `now − startedAtMs > MAX_EVALUATION_AGE_MS` (600,000, exported from src/store/redis.ts;
   MUST stay far below TOMBSTONE_TTL_MS). The check runs inside the Lua, atomically with the
   other guards. A refused evaluation leaves the txid un-evaluated: the next reparse redoes
   it with a fresh view. Without it a response parked past the tombstone TTL would land after
   the prune: confirmed:1 → seen → false dropped.

Store surface (all methods live on Store, mirrored exactly in tests/fakes.ts):
`outboxDue(nowMs, limit): Promise<string[]>` (ids, ascending score), `outboxRead(id)` →
`{event: WeirEvent, attempts, createdAt, lastError} | null`, `outboxAck(id)`,
`outboxRetry(id, nextAtMs, attempts, lastError): Promise<boolean>` and
`outboxDead(id, nowMs, attempts, lastError, deadMax): Promise<boolean>` (false = no-op, the
hash was gone), `outboxStats(): Promise<{depth: number; oldestCreatedAt: number | null; dead: number}>`
(`oldestCreatedAt` = the lowest score in `outbox:created`, `ZRANGE 0 0 WITHSCORES` — exact
and O(1)). Transition methods that enqueue are listed under src/store/redis.ts.

## Outpoint tracking (replacement + proven conflicts)

Why: weir discards the inputs of the transactions it tracks, so an RBF replacement is only
noticed as `dropped` at the NEXT block (up to ~10 min), and `conflicted` is inferred from
mempool absence rather than proven. Inputs are literal bytes in every tx weir already
decodes — no txindex, no prevout resolution: weir only checks them against its OWN memory.

Model: an outpoint can legitimately have SEVERAL concurrent tracked spenders (an RBF chain,
competing double-spends), so ownership is a SET of claimants, never a single owner. Claims
never conflict; there is no force, no waitlist, no handover, no adjudication rounds.

Rules:
1. `DecodedTx` carries `inputs: Outpoint[]` (`{txid, vout}`, prev-txid display-order hex);
   the coinbase input (txid all zeros, vout 0xffffffff) is omitted. `MaturingRecord` carries
   the same `inputs` so terminal cleanup can release them.
2. `weir:{net}:outpoint:{txid}:{vout}` SET of claimant txids (pending or maturing).
   - CLAIM = SADD, inside `recordSeen` (its Lua) and `promoteToMaturing` (its MULTI). Always
     succeeds. A tx's claims are complete or absent (same atomic op as its record).
   - RELEASE = SREM of ONLY the releaser's own txid, inside every transition that deletes the
     record (`dropPending`, `replacePending`, `conflict`, `finishMaturing`, `endTracking` —
     each reads `inputs` from the record it deletes). The key vanishes when empty. Nobody
     else's claim is ever touched. `demoteToPending` keeps claims (the tx stays tracked).
   - `clearTracking` DELs every `outpoint:*` key (SCAN + DEL BEFORE its main Lua — the bulk
     pass); its main Lua and the orphan sweep also release each record's OWN claims before
     deleting the record, so a `recordSeen` landing between the bulk pass and the Lua leaves
     no orphan claim. A claimant without a record is skipped by every reader anyway, so a
     stray claim would be harmless, just unbounded.
3. REPLACEMENT (mempool path, src/engine/txPipeline.ts) — ONE pass, no rounds:
   `isEvaluated` → evaluation fence (a stale view is refused before anything is mutated) →
   `outpointOwners(inputs)` (pipelined SMEMBERS, self excluded) → for EVERY distinct owner:
   read its record only to build the payload (gone → nothing to do); record unmined →
   `replacePending(owner, ev, startedAtMs, spender)` — ONE Lua that atomically checks the
   SPENDER's own state first (the spender retired at an exit ≥ `startedAt`, or tombstoned →
   `stale`, nothing written: an evaluation that predates its own tx's exit may not act on
   anyone — an old evaluation of A would otherwise replace the B that replaced A), then
   applies the fence (-1), requires `pendingUnmined` (in `pending` AND record exists with height 0, else 0:
   nothing written, nothing enqueued — a second caller, or one the block pipeline overtook,
   finds it already handled), then SREM pending, SREM evaluated (the original may return if
   the replacement is itself dropped), release its claims, DEL record, + enqueue `dropped`
   with `reason: 'replaced'`, `replacedBy: <this txid>`, key
   `{net}:{owner}:dropped:replaced:{replacedBy}` → 1, and ZADD the retirement watermark
   (`retired`, score = now); record MINED (maturing/limbo) → a mempool tx cannot displace a
   confirmed tx (bitcoind would not have relayed it; during reorg lag the block path
   adjudicates it — rule 4): warn, skip. `replacePending` resolves `'replaced' | 'skipped' |
   'stale'`; if ANY adjudication of the pass came back `stale`, the evaluator stops WITHOUT
   `markEvaluated` (an unwatched spender marked evaluated would never be re-fetched while the
   tx it replaced stayed pending): the next reparse redoes the whole evaluation with a fresh
   `startedAt`. Then match outputs; no match → `markEvaluated`, but only after the same
   TS-side age check (`now − startedAtMs > MAX_EVALUATION_AGE_MS` → return without marking);
   a watched tx is recorded with `recordSeen` (outcome `'recorded' | 'skipped' | 'stale'` —
   `stale` also when the evaluation predates the txid's retirement watermark), which claims
   its inputs. Documented imprecision: two spenders of one outpoint arriving CONCURRENTLY can both
   see no prior owner and both be recorded; bitcoind keeps only one, and the loser is
   reported `dropped` with `reason: 'evicted'` at the next tip check rather than `replaced`
   immediately — a labelling difference, never a lost verdict.
4. BLOCK PATH (src/engine/blockPipeline.ts, step 2, before promotion): collect every input of
   every block tx (excluding coinbase), `outpointOwners` once, then for each owner ≠ its
   spender (the owner's record read only for the payload; gone → skip):
   - owner in LIMBO → PROVEN conflict: `conflict(owner, ev)` with `conflictingTxid` = the
     spender, `reason: 'double-spend'`, timestamp = blockTime*1000;
   - owner's record MINED (height > 0) and not in limbo → impossible on a valid chain; log
     error, skip;
   - otherwise → `replacePending(owner, ev, scanStartMs, spender)` with `replacedBy` = the
     confirmed spender (a double-spend confirmed while ours sat in the mempool); the Lua
     decides (a `stale` here means the mined spender is tombstoned within the TTL — warned,
     the owner is left to the tip-block dropped check).
   Every claimant of a spent outpoint is adjudicated (a field may have several). Running
   before promotion means a limbo tx cannot be both re-included and conflicted by one block.
5. `resolveLimbo` keeps its by-elimination fallback (`getmempoolentry` → demoted, else
   conflicted) for txs the new chain neither re-included nor provably conflicted (e.g. an
   input spent by a tx weir never decoded because it was below the ring floor).
6. Payload additions (all OPTIONAL, additive — consumers ignoring them are unaffected):
   `dropped` gains `reason: 'replaced' | 'evicted'` (`evicted` is the residual verdict of the
   tip-block dropped check) and `replacedBy?: string`; `conflicted` gains
   `reason?: 'double-spend'` and `conflictingTxid?: string` when proven.
7. RETRY SAFETY NET: an evaluation the fence refused, or a tx missed by ZMQ without a
   sequence gap, is un-evaluated and is picked up by the next mempool reparse — which runs
   PERIODICALLY (`MEMPOOL_REPARSE_INTERVAL_MS` = 300 000, constant; the reparser's mutex skips
   overlap), not only at boot and on ZMQ gaps. Cost: one getrawmempool plus fetches of
   un-evaluated txids only.

Store surface: `outpointOwners(outpoints: Outpoint[]): Promise<Map<string, string[]>>`
(field → claimant txids, empty fields omitted, chunked pipelining), `replacePending(txid,
event, startedAtMs, spenderTxid): Promise<'replaced' | 'skipped' | 'stale'>` (rule 3; only
`replaced` wrote anything), `recordSeen(rec, event, startedAtMs): Promise<'recorded' | 'skipped' |
'stale'>`, `promoteToMaturing` (claims via SADD), and the releasing Luas `dropPending(txid,
event): Promise<boolean>` (guarded like replacePending, unfenced; both ZADD `retired`),
`conflict(txid, event)`, `finishMaturing(txid, nowMs)`, `endTracking(txid)` — they read the
record's own `inputs`, callers pass nothing. `pruneRetired(beforeMs)` (tip blocks, same TTL
as `pruneTombstones`). `clearTracking` (SCAN-DEL of `outpoint:*` FIRST, then one Lua that
releases each collected record's own claims and DELs records + tracking keys, then
`sweepOrphanRecords` — a `recordSeen` landing after the Lua keeps its record, memberships
and claims; one landing before it leaves no orphan claim) and `sweepOrphanRecords`
(per-record conditional Lua that also releases the record's own claims). Idempotency: `idem.replaced(net, txid,
replacedBy)`; `conflicted` keeps its single key (one terminal verdict per txid).

## Health from chain lag (`/live`, `/ready`, `/metrics`)

Why: `/health` returned 200 while ZMQ was disconnected or weir was blocks behind the node,
and `secondsSinceLastBlock` was null after every restart and unreliable anyway (block
intervals are Poisson — a 20-minute gap is normal). The signal that matters is CHAIN LAG:
`nodeHeight − tipHeight`. Bitcoin knows about blocks weir has not processed, or it does not.

Endpoints (all on the admin port, all UNAUTHENTICATED — they are for the platform's probes;
they expose counts, never addresses or txids):
- `GET /live` → 200 `{ok:true}` while the process is up and not shutting down; 503 during
  shutdown. Never depends on redis, rpc, or the webhook: restarting weir because a
  dependency is down fixes nothing.
- `GET /ready` → 200/503 `{ok, redis, rpc, reconciled, shuttingDown, tipHeight, nodeHeight,
  chainLag, watchCount, outboxDepth, outboxOldestAgeSec, deadLetterCount, lastZmqTxAgeSec,
  lastZmqBlockAgeSec}`. `ok` = redis ok AND rpc ok AND reconciled (boot finished reconcile +
  resolveLimbo) AND NOT shuttingDown AND chainLag ≤ `READY_MAX_LAG`. The runtime flags are
  read AFTER the async reads, so a shutdown that begins mid-probe still answers 503.
  Webhook/outbox state NEVER fails readiness: the daemon is healthy when the consumer is
  down. `nodeHeight` is one `getblockcount` per probe (short deadline, see src/bitcoin/rpc.ts);
  `chainLag` is null when either height is unknown.
- `GET /health` → alias of `/ready` (kept for compatibility; `secondsSinceLastBlock` removed).
- `GET /metrics` → Prometheus text exposition (text/plain; version=0.0.4), weir-native signals
  only — host metrics are the platform's job:
  gauges `weir_up 1`, `weir_tip_height`, `weir_node_height`, `weir_chain_lag`,
  `weir_watch_count`, `weir_outbox_depth`, `weir_outbox_oldest_age_seconds`,
  `weir_dead_letter_count`, `weir_redis_memory_used_bytes`, `weir_redis_memory_max_bytes`
  (absent when unlimited), `weir_last_zmq_tx_timestamp_seconds`,
  `weir_last_zmq_block_timestamp_seconds`, `weir_reconciled`;
  counters `weir_events_enqueued_total{event="seen|confirmed|dropped|demoted|conflicted|expired"}`,
  `weir_webhook_deliveries_total{result="ok|fail"}`, `weir_events_dead_lettered_total`,
  `weir_blocks_processed_total`, `weir_reorgs_total`.
  Hand-rolled formatter (no dependency); one `# TYPE` line per family.

src/lib/metrics.ts: an in-process registry — `counters.inc(name, labels?)`,
`gauges.set(name, value, labels?)`, `render(): string`. Process-local (counters reset on
restart; that is what `_total` means). Incremented by: every enqueuing Store transition
AFTER its write succeeded (events_enqueued_total by event — MULTI and Lua paths alike;
`Store.enqueue` itself is only the MULTI helper and runs before the write, and a guarded Lua
may write nothing), the outbox drainer (deliveries_total, dead_lettered_total), the block pipeline
(blocks_processed_total, reorgs_total), zmq.ts (last-message timestamps on every rawtx /
rawblock). Gauges that need I/O (heights, watch count, outbox, memory) are computed at
scrape time by the admin handler, not pushed.

Heartbeat gains `nodeHeight` and `chainLag` (one getblockcount per tick) so a consumer can
apply the stuck-pipeline rule without an independent chain source. Ticks never overlap: a
tick still in flight makes the interval skip (warned once per stall) — a stalled node or
endpoint must not pile up heartbeats.

Admin writes during the boot window: until `runtime.reconciled`, `POST /watches` and
`DELETE /watches/:address` answer 503 `{error: 'not ready: reconciling'}` with
`retry-after: 1` (after auth). A watch added before first-run reconcile could be paid and
mined in a block that reconcile then initialises the tip PAST — a payment weir would never
process. Reads (`GET /watches…`) and the probes are always served.

Config: `READY_MAX_LAG` (default 2 — lag 1 is normal for a moment after every block; 2 means
weir is genuinely behind). Docs (README "Monitoring"): alert on no heartbeat for 2× the
interval; `chainLag > READY_MAX_LAG` persisting > 3 min; `weir_outbox_oldest_age_seconds >
300`; any `weir_events_dead_lettered_total` increase; redis memory > 80% of max.

src/index.ts: a `Runtime` object `{reconciled, shuttingDown, lastZmqTxAt, lastZmqBlockAt}`
replaces the `lastBlockAt` closure; reconciled is set after resolveLimbo; shuttingDown is
set first thing in shutdown() so `/live` flips before anything closes. The admin server
starts right after preflight — BEFORE the boot drain and reconcile — so the probes answer
during a long catch-up (`/live` 200, `/ready` 503 `{reconciled: false}`) instead of refusing
connections; started after reconcile, `reconciled` could never be observed false.

## Module map and contracts

All engine modules take a `deps` object (structural typing) so tests can pass in-memory fakes.
NEVER swallow errors silently. ONE fatal-error policy for BACKGROUND paths: an UNEXPECTED
error on a path where nobody is waiting for an answer — where a swallowed error would be a
silent stall — crashes the process through `fatal()` (src/lib/log.ts — logs message + stack,
`process.exit(1)`); docker restarts the daemon and boot reconciliation (reconcile →
resolveLimbo → mempool reparse) heals. Fatal sites: the ZMQ subscriber loop failing or ANY
ZMQ handler rejecting (rawtx, rawblock, gap-triggered reparse), a heartbeat tick rejecting,
the initial mempool reparse rejecting, a block that fails to process (makeBlockHandler — no
"skip the failed block and keep the queue alive"), the redis client giving up reconnecting
or ending unasked (src/store/redis.ts), the admin server's `'error'` event (listen failure),
and boot/shutdown failures. Every abnormal exit goes through `fatal`; a clean shutdown exits 0.
REQUEST/RESPONSE paths are different: a failed admin request is answered `500 {error}` and
the daemon keeps running — the request has a natural error channel, and a failed request
does not imply corrupted engine state. Webhook delivery failures are NEVER fatal either:
`send` returns `{ok: false, error}` and the outbox drainer retries (heartbeats: the next
interval retries).

Deps are typed by picking from the real classes, never by hand-copying signatures:
`store: Pick<Store, …>` / `rpc: Pick<Rpc, …>` (type-only imports) and
`sink: Sink` (= `Pick<WebhookSink, 'send'>`, declared in src/delivery/webhook.ts) — only the
outbox drainer and the heartbeat take a sink; engine modules enqueue through the Store. Logging
is the single shared `log` from src/lib/log.ts — no module takes a logger dep. Tests
substitute tests/fakes.ts (`FakeStore`, `FakeSink`, `FakeChain`), which MUST match the real
classes' semantics exactly (e.g. `removeMaturing` deletes index AND record; the ring is a
ZSET member→score map, so two hashes can coexist at one height); a signature mismatch is
fixed in the fake, never in the real class. `FakeStore` also models the outbox (id → record,
queue as id → score, dead set) and exposes `outboxEvents(): WeirEvent[]` (queued events in
score-then-insertion order) so engine tests assert on what was ENQUEUED; delivery itself is
tested through the drainer with `FakeSink`.

### src/lib/log.ts
`export const log: { info(ctx: string, msg: string): void; warn(...): void; error(...): void }`
— single-line output `[level] [ctx] msg`, no colors dependency.
`export function describeError(err: unknown): string` — the one error describer for log
lines: joins AggregateError inner messages, follows `.cause` chains.
`export function fatal(ctx: string, err: unknown): never` — the one fatal-error exit: logs
`describeError(err)` plus the stack when present at error level, then `process.exit(1)`.

### src/lib/hmac.ts
`export function signBody(secret: string, body: string, tSeconds: number): string` → full header value.
`export function verifySignature(secret: string, body: string, header: string, toleranceSec?: number, nowSeconds?: number): boolean`
— constant-time compare (crypto.timingSafeEqual), reject stale t beyond tolerance (default 300s).

### src/bitcoin/decoder.ts
Pure, no I/O. Port the decode approach from
`/Users/nate/Developer/blockhooksV2/packages/zmqapp/src/lib/decodeRawTransaction/index.ts`
(same repo owner) with fixes:
- `decodeRawTx(raw: Buffer | string, network: Network): DecodedTx` — txid via bitcoinjs
  Transaction.getId(); `inputs` = each `ins[i]` as `{txid: reversed-hex(hash), vout: index}`,
  coinbase input omitted; EVERY output present in `outputs` with real `vout` index and `valueSats`;
  `address:null, scriptType:null` for unrecognized script types (never dropped from the array).
- Script types: p2wpkh, p2wsh (bech32, witness v0 — use `bech32.toWords`/`bech32.encode`),
  p2tr (bech32m, witness v1), p2pkh, p2sh (base58check). FIX the upstream bug that mixed
  `bech32m.toWords` into v0 encoding.
- Template matching is by EXACT BYTE PATTERN (Bitcoin Core Solver style), NOT decompiled
  chunks: decompiling erases push minimality, so a non-minimal lookalike (`OP_0 PUSHDATA1
  0x14 <20B>` etc.) would classify as p2wpkh/p2sh/p2tr while NOT actually paying that
  address (segwit lookalikes are anyone-can-spend) — a payment-forgery vector for a
  notifier. Lookalikes must yield address:null.
- Network prefixes: mainnet bc/00/05; testnet AND signet tb/6f/c4; regtest bcrt/6f/c4.
- `decodeBlock(raw: Buffer, network: Network): DecodedBlock` — bitcoinjs Block.fromBuffer;
  `hash` = block.getId(), `prevHash` = hex of block.prevHash reversed, `time` = header timestamp.
- `isValidAddress(address: string, network: Network): boolean` — via bitcoinjs
  address.toOutputScript against the right network object (signet uses testnet params).
  NOTE: bitcoinjs-lib v6 toOutputScript rejects ALL taproot addresses unless an ECC lib is
  initialized (weir has none by design), so on throw the implementation falls back to
  address.fromBech32 accepting only version 1 / matching HRP / 32-byte program — BIP350
  address validity (checksum + variant still enforced; point validity is not address validity).
Tests: round-trip fixtures. Reuse/adapt fixtures from
`/Users/nate/Developer/blockhooksV2/packages/zmqapp/src/lib/decodeRawTransaction/rawTransactionExamples.json`
plus at least one known-good mainnet tx per script type with hardcoded expected addresses.

### src/bitcoin/rpc.ts
`export class Rpc { constructor(url: string) }` — creds parsed from URL, Basic auth, global
`fetch`, JSON-RPC 1.0, small retry (3x, backoff) on network errors, NO retry on 401/403.
DEADLINE: every attempt runs under one AbortController covering the request AND reading the
body — `RPC_TIMEOUT_MS` (30 000; the block path fetches whole blocks) or, for
`getBlockCount` (the probe/heartbeat call), `RPC_PROBE_TIMEOUT_MS` (5 000). An abort is a
network failure for retry purposes, so a call's total budget is bounded at attempts ×
timeout + backoff. A stalled node is a rejection, never a hang.
Methods: `getBlockCount(): Promise<number>`, `getBestBlockHash(): Promise<string>`,
`getBlockHash(height): Promise<string>`,
`getBlockHeader(hash): Promise<{height: number; previousblockhash?: string; time: number}>`,
`getBlockRaw(hash): Promise<Buffer>` (verbosity 0),
`getRawMempool(): Promise<string[]>`,
`getRawTransactionVerbose(txid): Promise<{blockhash?: string; hex: string} | null>` (null on "not found"),
`getMempoolEntry(txid): Promise<object | null>` (null on "not in mempool"),
`getBlockchainInfo(): Promise<{chain: string; blocks: number; pruned: boolean; pruneheight?: number}>`,
`getZmqNotifications(): Promise<Array<{type: string; address: string}>>`.

### src/bitcoin/zmq.ts
`export async function startZmq(opts: { url: string; onRawTx: (buf: Buffer) => void;
onRawBlock: (buf: Buffer) => void; onTxGap: () => void }): Promise<{ close(): Promise<void> }>`
— zeromq v6 Subscriber, topics `rawtx` + `rawblock`, sequence tracking per topic; a rawtx
sequence gap calls `onTxGap()` (fires once per gap, not per message). Handlers are invoked
without await (fire-and-forget) but MUST be wrapped so a rejection is `fatal` — never
unhandled, never swallowed. A subscriber-loop failure (other than close()) is fatal too.

### src/store/redis.ts
`export class Store` wrapping `redis` v4 client. Constructor `(url: string, network: Network)`.
`connect()/quit()`. Methods (all promise-returning; keys via `keysFor`):
- watches: `isWatched(addr)`, `watchedSubset(addrs: string[]): Promise<string[]>` (one
  SMISMEMBER — return ALL matches, do not bail on first), `addWatch(addr, expiresAtMs?: number)`,
  `removeWatch(addr): Promise<boolean>` (also ZREM expiries), `watchCount()`,
  `scanWatches(cursor: string): Promise<{cursor: string; addresses: string[]}>` (SSCAN, COUNT 1000),
  `dueExpiries(nowMs): Promise<Array<{address: string; expiresAtMs: number}>>`,
  `clearExpiry(addr)`, `getExpiry(addr): Promise<number | null>` (ZSCORE expiries — the admin
  server's `GET /watches/:address` needs the expiresAt readback).
- evaluated/pending: `isEvaluated(txid)`, `markEvaluated(txid)` (the no-match path),
  `pendingTxids(): Promise<string[]>`. Every other pending/evaluated mutation is part of a
  transition below (there is no standalone `addPending`/`removePending`/`unmarkEvaluated`).
- block/mempool bookkeeping: `setBlockTxids(txids)`, `pendingInBlock(): Promise<string[]>`
  (SINTER pending ∩ blockTxids), `replaceCurrentMempool(txids)`,
  `newMempoolTxids(): Promise<string[]>` (SDIFF current − evaluated — no "previous" snapshot:
  `evaluated` alone decides; a dropped tx is un-evaluated again so a rebroadcast re-fires `seen`),
  `clearCurrentMempool()` (DEL current), `replacePostBlockMempool(txids)`,
  `droppedPending(): Promise<string[]>` (SDIFF pending − postBlock − blockTxids),
  `pruneEvaluated()` (SINTERSTORE evaluated = evaluated ∩ postBlock).
- tip/ring: `getTip(): Promise<Tip | null>`, `setTip(tip)`, `ringPut(height, hash)`,
  `ringHashAt(height): Promise<string | null>`, `ringAll(): Promise<Array<{height; hash}>>`
  (ZRANGE 0 -1 WITHSCORES, ascending — INCLUDES height 0, which a `(0 +inf` range would
  miss on a fresh regtest ring holding only genesis), `ringPrune(keep: number)`,
  `ringRemoveAbove(height)` (ZREMRANGEBYSCORE — reorg rewind, keeps the one-hash-per-height
  invariant).
- transitions that ENQUEUE (each one MULTI = state mutation + outbox HSET/ZADD; see Outbox):
  `recordSeen(rec: MaturingRecord, event: TxEvent | null, startedAtMs: number): Promise<'recorded' | 'skipped' | 'stale'>`
  — HSET record (height 0), SADD pending, SADD evaluated, claim each of `rec.inputs` (SADD
  txid into `outpoint:{txid}:{vout}`; ARGV[14] = inputs JSON, ARGV[15] = the `{txid}:{vout}`
  fields joined by ',', ARGV[16] = the key prefix the Lua builds the SET names from —
  single-instance redis, see Topology), + enqueue when event is non-null (null when seen is
  disabled). ONE Lua script, FENCED (refused when now − startedAtMs >
  MAX_EVALUATION_AGE_MS, see Outbox rule 8) and GUARDED: a no-op (resolves false) when `evaluated` already
  holds the txid (a duplicate concurrent evaluation), or the record already has height > 0
  (the block pipeline mined + promoted it between this evaluation's read and its write —
  without the guard a late `seen` write would put a mined record back to height 0), or the
  txid is TOMBSTONED (tracking already ended and the record is GONE, so the first two guards
  cannot see it: a mempool RPC that read the tx before the block and returned after the
  final-milestone cleanup would otherwise recreate a height-0 pending record and the next
  block would emit a false `dropped` for a confirmed payment); and `stale` (code -3, warned)
  when the txid is RETIRED at an exit time ≥ `startedAtMs` (a duplicate evaluation that
  predates a drop/replacement — see Redis schema, retirement watermark);
  `markFired(txid, fired: number[], event: TxEvent)` — HSET fired + enqueue `confirmed`
  (fired is recorded at ENQUEUE time; delivery retry is the outbox's job);
  `dropPending(txid, event: TxEvent): Promise<boolean>` — ONE GUARDED Lua: not
  pending-unmined → 0 (nothing written, false); else SREM pending, SREM evaluated, release its
  claims (SREM its own txid from each prevout SET in the record's `inputs`), DEL record, ZADD
  `retired` score=now (the retirement watermark), + enqueue (`reason: 'evicted'`) → true;
  `replacePending(txid, event: TxEvent, startedAtMs, spenderTxid): Promise<'replaced' | 'skipped' | 'stale'>`
  — ONE Lua: the spender retired at an exit ≥ startedAtMs, or tombstoned → -3 (`stale`,
  warned); fence → -1 (`stale`, warned); not pending-unmined (not pending, record missing,
  or mined) → 0 (`skipped`, nothing written); else the dropPending mutation (watermark
  included) + enqueue `dropped` with `reason: 'replaced'` → 1 (`replaced`) (see Outpoint
  tracking rule 3; no tombstone — not a terminal verdict);
  `demoteToPending(rec, event: TxEvent)` — HSET record back to height 0/blockHash ''/fired [],
  SADD pending, SADD evaluated, SREM limbo, + enqueue `demoted` (`evaluated` is part of it
  because the tip prune forgot the txid when it was mined; without it the next reparse would
  emit a second `seen`);
  `conflict(txid, event: TxEvent)` — ONE Lua: SREM pending, ZREM maturing, release its
  claims, DEL record, SREM limbo, ZADD tombstones, + enqueue;
  `expireWatch(addr, event: ExpiredEvent)` — SREM addresses, ZREM expiries, + enqueue;
  `endTracking(txid)` — ONE Lua: SREM pending, SREM limbo, release its claims, DEL record
  (no event: watch removed mid-flight);
  `finishMaturing(txid, nowMs)` — ONE Lua: release its claims, DEL record, ZREM maturing,
  ZADD tombstones score=nowMs (no event: the final-milestone cleanup — tracking ENDED). `dropPending` deliberately does NOT
  tombstone: a rebroadcast may legitimately re-fire `seen`.
- tombstones: `isTombstoned(txid)` (ZSCORE non-nil), `pruneTombstones(beforeMs)`
  (ZREMRANGEBYSCORE -inf beforeMs; the tip block calls it with now − TOMBSTONE_TTL_MS);
  `pruneRetired(beforeMs)` — the same prune for the retirement watermark.
- maturing: `promoteToMaturing(rec: MaturingRecord)` — THE mined-tx promotion, one MULTI:
  HSET record, ZADD maturing, SREM pending, SREM limbo, SADD evaluated, SADD into each prevout SET (every promotion
  branch uses it, so a crash cannot leave a half-promoted tx; no event — confirmations come
  from the milestone sweep), `maturingEntries(): Promise<Array<{txid: string; height: number}>>`,
  `removeMaturing(txid)` (DEL record + ZREM — dangling-index cleanup only; the final-milestone
  cleanup is `finishMaturing`, which also tombstones),
  `unindexMaturing(txids: string[])` (ONE ZREM of the index ONLY, records kept — the
  maturing→limbo transition; no-op on empty). Reading a maturing record is `readRecord`.
- records (hash-only, exist from seen-time): `readRecord(txid)`. Records are written only by
  transitions (`recordSeen`, `promoteToMaturing`, `demoteToPending`, `markFired`) and deleted
  only by transitions (`dropPending`, `conflict`, `endTracking`, `removeMaturing`).
- limbo: `addLimbo(txids: string[])`, `limboTxids(): Promise<string[]>`, `removeLimbo(txid)`
  (demotion is `demoteToPending` above).
- outpoints: `outpointOwners(outpoints): Promise<Map<string, string[]>>` (one SMEMBERS per
  prevout, pipelined in chunks of 1000; prevouts with claimants only; contract under Outpoint
  tracking).
- `sweepOrphanRecords(): Promise<number>` — SCAN `maturing:*`; each record's own claims are
  released and the record deleted by its own Lua ONLY while its txid has no live membership
  (not pending, not in limbo, not in the maturing index) — a record a concurrent
  `recordSeen`/promotion just created is left alone.
- `clearTracking(): Promise<string[]>` — prune-window guard: SCAN + DEL every `outpoint:*`
  SET (the bulk pass), collect the txids of limbo + pending + maturing, then ONE Lua releases
  each of their records' own claims, DELs the records AND the tracking keys (maturing index,
  pending, limbo, evaluated, mempool scratch, block txids) atomically, then
  `sweepOrphanRecords()` — so a `recordSeen` landing after the Lua keeps its record,
  memberships and claims, and one landing before it leaves no orphan claim. PRESERVING watches/expiries/tip/ring AND the
  outbox (queued events are still owed); returns the txids whose tracking was lost.
- outbox: `outboxDue`, `outboxRead`, `outboxAck`, `outboxRetry`, `outboxDead`, `outboxStats`
  (contracts under Outbox).
- meta: `memoryInfo(): Promise<{usedBytes: number; maxBytes: number | null}>` (INFO memory),
  `maxmemoryPolicy(): Promise<string | null>` (CONFIG GET; null ONLY when the error is a
  command-access refusal — `/unknown command|not allowed|NOPERM|disabled|CONFIG/i`, e.g.
  managed redis — anything else, such as connection loss, is rethrown).
- connection: `connect()` rejects when the bounded reconnect (5 attempts, 250ms x2 capped 2s)
  is exhausted at boot. At RUNTIME exhaustion is fatal from inside the reconnect strategy
  (node-redis emits no terminal event when it gives up — a closed client with no command in
  flight would be a zombie process), and so is an `'end'` event not preceded by `quit()`.

### src/delivery/webhook.ts
`export class WebhookSink { constructor(cfg: {url; secret; timeoutMs}); send(event: WeirEvent):
Promise<{ok: true} | {ok: false; error: string}> }` plus `export type Sink = Pick<WebhookSink, 'send'>`
— ONE attempt: serialize, sign via hmac.ts with the current unix seconds, POST with
`content-type: application/json` + `x-weir-signature`, `redirect: 'manual'`, AbortController
timeout `WEBHOOK_TIMEOUT_MS`, cancel the response body. 2xx → `{ok: true}`; anything else →
`{ok: false, error}` where error names the HTTP status or the network/abort error. Never
throws. No retry loop here — retry is the outbox drainer's job (src/delivery/outbox.ts).

### src/delivery/outbox.ts
`export function startOutboxDrainer(deps: {cfg; store; sink}): {stop(): Promise<void>; drainOnce(): Promise<number>}`
— the loop described under Outbox. `drainOnce` processes one batch and returns the number of
events delivered (index.ts awaits it once at boot; tests call it directly — a store error
then REJECTS). The interval pass takes batch after batch while they come back full
(`ids.length === OUTBOX_BATCH`); a store error there is fatal. Every pass, interval or direct,
is serialized through one shared in-flight promise (a direct call during a pass waits for it,
then runs). `stop()` clears the interval and awaits the in-flight pass, which finishes its
current send and then exits between events (a batch of 50 against a timing-out endpoint would
otherwise hold shutdown for 50 × WEBHOOK_TIMEOUT_MS); the events it did not reach stay queued.

### src/engine/matcher.ts
`export function matchAgainst(tx: DecodedTx, watched: ReadonlySet<string>): MatchedOutput[]`
— pure: return ALL outputs paying an address in `watched` (an address matched by 2 outputs
yields 2 entries).
`export async function matchTx(tx: DecodedTx, store: Pick<Store, 'watchedSubset'>): Promise<MatchedOutput[]>`
— thin wrapper: collect addresses from outputs, `watchedSubset`, `matchAgainst`.

### src/engine/txPipeline.ts
`export function makeTxEvaluator(deps): (tx: DecodedTx, startedAtMs: number) => Promise<void>` and
`export function makeRawTxHandler(deps): (raw: Buffer) => Promise<void>` (startedAtMs =
receipt time, then decode → evaluate).
Evaluate — ONE pass (Outpoint tracking rule 3): if `isEvaluated` → return. Fence (refuse a
stale evaluation, warn, leave un-evaluated). Replacement check: `outpointOwners(inputs)`,
every distinct claimant ≠ this txid → its record (gone → skip; mined → warn, skip; unmined →
the guarded `replacePending`); any `stale` outcome → warn, return WITHOUT markEvaluated.
Match. If no match → the TS-side age check, then markEvaluated, done. If match:
build the `seen` TxEvent (confs 0, block fields null, timestamp now) when seenEnabled, then
`recordSeen(rec, event | null, startedAtMs)` — one atomic fenced + guarded step that also
claims the inputs, nothing awaited from the network; `skipped` (a concurrent path already
tracks or mined the tx) / `stale` is logged and is the end of the evaluation.
The old "delivery failed → leave un-evaluated" path no longer exists: the outbox owns retry.

### src/engine/mempool.ts
`export function makeMempoolReparser(deps): () => Promise<void>` — module-level mutex (skip
if running). getRawMempool → replaceCurrentMempool → newMempoolTxids → for each (bounded
concurrency 32): startedAtMs = now → getRawTransactionVerbose → decode → evaluate(tx,
startedAtMs) (reuse txPipeline evaluator; the fence clock starts BEFORE the RPC);
tx that vanished (null) is skipped; tx that was mined meanwhile (`blockhash` set) is skipped
too — the block pipeline owns mined txs and a `seen` for it would be false. Then
clearCurrentMempool. Exports `MEMPOOL_REPARSE_INTERVAL_MS` (300 000, constant): index.ts runs
the reparser on that interval as the retry safety net (Outpoint tracking rule 7).

### src/engine/reorg.ts
`export async function findForkPoint(deps, incomingPrevHash: string, incomingHeight: number):
Promise<{ancestorHeight: number; disconnected: Array<{height: number; hash: string}>}>`
— walk back from incomingPrevHash via getBlockHeader until a header's hash matches
`ringHashAt(height)`; entries in the ring above the ancestor are the disconnected blocks.
If the walk exits the ring (deeper than tracked), log loudly and treat ancestor = lowest ring
entry (documented bound).
Displaced-tx resolution is the LIMBO MODEL — on a pruned/no-txindex node there is no way to
ask "which block is txid X in now?" at reorg time (getrawtransaction without a blockhash only
answers for mempool txs), so weir never adjudicates at detection time:
`export async function enterLimboAndRewind(deps, ancestorHeight): Promise<void>` — every
maturing tx with inclusion height > ancestor: SADD `limbo` + `unindexMaturing` (record kept);
then `ringRemoveAbove(ancestor)` and `setTip(ancestor)`. The replacement chain now processes
as a plain connected walk: no second fork search, one hash per height in the ring.
Re-inclusion is discovered NATURALLY by the block pipeline's promotion step: a limbo txid
found in a new block re-enters maturing (fresh height/blockHash, fired []) and leaves limbo —
no event at re-inclusion; milestones re-fire on the sweep with new-blockhash idempotency keys.
`export async function resolveLimbo(deps): Promise<void>` — runs after the TIP block finishes
(and at boot when already reconciled): for each txid still in limbo (a PROVEN conflict —
Outpoint tracking rule 4 — already left limbo inside `conflict` in the block pipeline, so it
is never adjudicated twice),
  - getMempoolEntry non-null → build `demoted` (confs 0, block fields = the OLD block,
    timestamp now) and `demoteToPending(rec, ev)` (one MULTI, enqueues it);
  - else → build `conflicted` (confs = last confirmed depth or 0, old block fields) and
    `conflict(txid, ev)` (one MULTI: full cleanup + enqueue). Terminal.
Because limbo is durable, a crash anywhere in the sequence re-resolves on the next block/boot.

### src/engine/blockPipeline.ts
`export function makeBlockProcessor(deps): (raw: Buffer) => Promise<void>` used by both the ZMQ
path and catch-up, plus `export function makeBlockHandler(processBlock, onFatal = fatal): (raw: Buffer) => Promise<void>`
which wraps that ONE processor in a serialization queue (index.ts builds a single processor
and hands it to both reconcile and makeBlockHandler). A rejected processBlock is fatal —
`onFatal` is injectable for tests only. Sequence for
a block B (hash H, prev P, height h from getBlockHeader(H)):
1. connectivity: tip = getTip(). If tip === null → first run: process B standalone (no gap walk).
   If P === tip.hash → connected. Else: findForkPoint (a pure gap yields empty disconnected);
   on a REORG first `enterLimboAndRewind(ancestor)`. Then, if blocks are missing
   (ancestor+1 < h), the PRUNE-WINDOW GUARD runs before walking: getBlockchainInfo; if the
   node is pruned and ancestor+1 < pruneheight, catch-up is impossible — `clearTracking()`,
   log the lost txids LOUDLY, jump tip/ring to B's parent and continue (watches unaffected;
   still-unconfirmed txs re-fire `seen` via the mempool reparse). Otherwise walk
   ancestor+1 .. h-1 via getBlockHash → getBlockRaw → process (in order, as NON-tip blocks),
   then B itself. After the OUTERMOST (tip) block completes: `resolveLimbo`.
2. per connected block: decodeBlock → setBlockTxids → `pendingInBlock()` + `limboTxids()` →
   the INPUT SCAN (Outpoint tracking rule 4: every block input → `outpointOwners`, chunked
   pipelined SMEMBERS; for EVERY claimant ≠ its spender: limbo → proven `conflict` with
   `timestamp = blockTime*1000`, and the local limbo snapshot forgets it; mined and not in
   limbo → error, skip; otherwise the guarded `replacePending`) →
   resolve the block's DISTINCT output addresses with `watchedSubset` ONCE (chunked at 1000
   addresses) → `matchAgainst` per tx: for each block tx that matches (check EVERY block tx,
   not just pending ∩ block — a payment never seen in the mempool still confirms): build
   MaturingRecord from the block's own decode, then `promoteToMaturing` (one MULTI) for
   every origin: limbo txid → re-inclusion (no event); pending txid → promotion; neither →
   never-seen, promoted unless its record already has height > 0 (block replay guard) or the
   txid is tombstoned (tracking already ended within TOMBSTONE_TTL_MS: a block replay after
   the final cleanup, or a reorg at exactly max-milestone depth — not a new payment).
   The `evaluated` flag is NOT a gate: it only says the mempool evaluator looked once —
   possibly before the address was watched, or before a crash lost the record — and a
   mined payment to a watched address must confirm regardless. A tracked tx that no
   longer matches any watch (watch removed mid-flight) ends tracking quietly.
3. milestone sweep: for each maturingEntries() entry: confs = h − height + 1; for each
   confirm milestone m in (fired ∌ m) with confs ≥ m: build `confirmed` (confs = m, block
   fields from the record, timestamp = blockTime*1000 of B) and `markFired(txid, fired+m, ev)`
   (fired + enqueue, one MULTI; fired stays sorted). When confs ≥ maxMilestone AND all
   milestones are fired → `finishMaturing(txid, now)` (final; tracking ends; tombstoned).
4. dropped check — TIP BLOCKS ONLY: it compares pending against the LIVE mempool, which is
   meaningless for historical blocks during a catch-up walk (a pending tx mined in a LATER
   missed block would be falsely reported dropped). replacePostBlockMempool(getRawMempool) →
   droppedPending() → for each: emit `dropped` (hex+matched come from the seen-time record:
   every tx entering `pending` also gets its record written under `maturing:{txid}` with
   height 0 / blockHash '' — a dropped tx cannot be re-fetched from a pruned node. The
   `maturing` ZSET still only indexes MINED txs; the record exists from seen onward):
   `dropPending(txid, ev)` — one GUARDED Lua (pending, evaluated so a rebroadcast can re-fire
   `seen`, record, its own claims, + enqueue) with `reason: 'evicted'`, a no-op
   (false, logged) when the txid is no longer pending-unmined — a mempool replacement that
   landed since `droppedPending()` was read already gave the verdict; replacements are the
   input scans' job.
5. TTL sweep — TIP BLOCKS ONLY: dueExpiries(now) → `expireWatch(addr, ev)` (one MULTI).
6. pruneEvaluated + pruneTombstones + pruneRetired (both now − TOMBSTONE_TTL_MS; tip only),
   setTip({hash: H, height: h}), ringPut, ringPrune(ringSize).
A tracked tx that no longer matches any watch → `endTracking(txid)` (no event).
There are no delivery-failure branches in the engine: every event is enqueued atomically
with its state change and the outbox retries it.

### src/engine/heartbeat.ts
`export function startHeartbeat(deps): {stop(): void}` — setInterval(heartbeatInterval s):
build HeartbeatEvent from getTip/watchCount/memoryInfo/outboxStats + one `rpc.getBlockCount`
(`nodeHeight`; `chainLag = nodeHeight − tipHeight`, both null when the RPC fails — warned,
the tick still goes out) and `sink.send` it
DIRECTLY (not via the outbox — proof-of-life must reflect now; a failed send is warned, the
next interval is the retry; an interval that finds the previous tick still in flight skips,
warned once per stall — ticks never overlap). Payload: `{version:1, event:'heartbeat', network, tipHeight,
nodeHeight, chainLag, watchCount, memoryUsedPct, outboxDepth, outboxOldestAgeSec (null when
empty), deadLetterCount, idempotencyKey, timestamp}`. Skipped when interval 0. A rejected
tick (store failure) is fatal; an RPC failure is not.

### src/boot/preflight.ts
`export async function preflight(deps): Promise<void>` — checks, in order, each with a clear
one-line log; throw Error (fatal) unless noted:
1. redis reachable; `maxmemoryPolicy()` — if a policy is returned and ≠ 'noeviction' → FATAL
   (eviction would silently delete watches); if CONFIG blocked → warn only.
2. rpc reachable; `getBlockchainInfo().chain` must match NETWORK (chain names: main, test,
   signet, regtest — map accordingly); log pruned status (pruned is fine — say so).
3. `getZmqNotifications()` must include pubrawtx AND pubrawblock → else FATAL with the exact
   bitcoin.conf lines to add.
4. log the capacity estimate for the configured redis maxmemory (formula:
   (maxBytes/1.5 − 45MB) / 330 bytes; skip when maxBytes null).

### src/boot/reconcile.ts
`export async function reconcile(deps): Promise<void>` — tip = getTip(). If null: initialize
tip/ring to current best block (getBestBlockHash + header; ringPut) — forward-only, no backfill.
Else: node best = getBestBlockHash(); if same as tip.hash → done. Else: process blocks from
tip → best via the blockPipeline's processor (handles gap, reorg, prune-window guard and limbo
resolution via the same logic). reconcile takes no limbo dep: index.ts calls `resolveLimbo`
unconditionally right after `reconcile` returns (a crash between limbo-rewind and resolution
must not leave displaced txs unadjudicated until the next block; no-op when limbo is empty).

### src/admin/server.ts
`export function startAdminServer(deps, onFatal = fatal): {close(): Promise<void>; port: Promise<number>}` —
node:http only, no framework. Only constructed when adminToken set. `port` resolves with
the bound port once listening (`ADMIN_PORT=0` = ephemeral; tests use it) and REJECTS with
the listen error (EADDRINUSE, EACCES) when the server errors before listening — a caller
awaiting it fails fast with the real cause; the error still takes the fatal path
(`onFatal` is injectable for tests only). Every route
except the GET probes (`/live`, `/ready`, `/health`, `/metrics`) requires
`authorization: Bearer <ADMIN_TOKEN>` (timingSafeEqual) → 401 otherwise. After auth, the
write routes (`POST /watches`, `DELETE /watches/:address`) answer 503 `{error: 'not ready:
reconciling'}` + `retry-after: 1` while `runtime.reconciled` is false (see "Health from chain
lag"); reads and probes never wait. Deps:
`store: Pick<Store, …>`, `rpc: Pick<Rpc, 'getBlockCount'>` (liveness probe AND node height),
`runtime: Readonly<Runtime>` (src/lib/types.ts), `config` incl. `readyMaxLag`.
- `POST /watches` body `{address: string, ttl?: number}` → validate with isValidAddress →
  422 `{error}` on invalid; addWatch(+expiry from ttl ?? watchDefaultTtl when > 0) → 201
  `{address, network, expiresAt: number | null}`. Idempotent.
- `DELETE /watches/:address` → 204 (idempotent; 204 even if absent).
- `GET /watches?cursor=0` → `{addresses, cursor}` (SSCAN passthrough; cursor "0" = done;
  a cursor that is not `/^\d+$/` → 400).
- `GET /watches/:address` → 200 `{address, watched: true, expiresAt}` or 404.
- `GET /live`, `GET /ready`, `GET /health` (= `/ready`), `GET /metrics` (no auth) — contracts
  under "Health from chain lag". `/metrics` is `text/plain; version=0.0.4; charset=utf-8`;
  its scrape-time reads run independently (`Promise.allSettled`): a failed read omits its
  own gauges (ONE warn line naming the failed reads), `weir_up` stays 1, the response is 200.
  `weir_outbox_oldest_age_seconds` is 0 (not absent) when the outbox is empty.
Reject bodies > 4KB with a real 413 response (`connection: close`; the socket is never
destroyed before the status is written). JSON errors as `{error: string}`. An unhandled
error inside a request handler → `500 {error: 'internal error'}` (request/response path:
never fatal); a server `'error'` event (listen failure) → `fatal` (background path).

### src/index.ts (integration)
loadConfig → Store.connect → preflight → admin server if token (probes answer from here:
`/live` 200, `/ready` 503 until reconciled) → startOutboxDrainer → ONE awaited `drainOnce()`
(logged; acks free memory before reconcile writes under a full redis) → reconcile →
resolveLimbo → `runtime.reconciled = true` → startZmq (rawtx → txHandler, rawblock →
blockHandler, gap → reparser; each receipt stamps `runtime.lastZmqTxAt` / `lastZmqBlockAt`
before decoding) → initial mempool reparse (async) → periodic reparse timer
(`setInterval(reparse, MEMPOOL_REPARSE_INTERVAL_MS)`, unref'd; the reparser's mutex skips
overlap; a rejection is fatal — background path) → startHeartbeat (takes `rpc` for
getBlockCount). Engine deps get NO sink (they enqueue); the sink goes only to the drainer and
the heartbeat. SIGINT/SIGTERM → `runtime.shuttingDown = true` FIRST, then close zmq, clear
the reparse timer, stop heartbeat, close admin, await drainer.stop(), store.quit, exit 0.
Log a startup banner: version, network, milestones, webhook target host, admin on/off.

## Coding conventions
- TypeScript strict, CommonJS module output; extensionless relative imports.
- No new runtime dependencies beyond package.json without updating this doc.
- Errors: never `catch {}`. Log with context tag matching module name.
- Tests import engine modules with in-memory fakes (tests/fakes.ts) — no redis/bitcoind needed.

## Docker
- `Dockerfile`: node:22-alpine, corepack/pnpm, install (zeromq needs python3 make g++ on
  alpine), build, prune dev deps, `CMD node dist/index.js`. Non-root user.
- `docker-compose.yml`: services `weir` (build, env from .env), `redis` (redis:7-alpine,
  `--appendonly yes --maxmemory ${REDIS_MAXMEMORY:-256mb} --maxmemory-policy noeviction`,
  volume), `bitcoind` under profile `regtest` (image bitcoin/bitcoin or ruimarinho/bitcoin-core,
  regtest conf with zmq + rpc for user weir). NO published ports on weir/redis; bitcoind RPC
  only on the compose network. `docker/bitcoin-regtest.conf` + signet + mainnet conf examples.
- `examples/catch.js`: zero-dep node http server on :9090, verifies x-weir-signature
  (WEBHOOK_SECRET env), pretty-prints events.
- `examples/regtest-demo.sh`: compose exec bitcoin-cli: create wallet, generate 101, get new
  address, SADD via redis-cli, send, mine — narrated with expected output.
