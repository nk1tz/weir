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
multi-key MULTIs without hash tags, so Redis Cluster is not a target.

## Single writer

weir is ONE writer. Every engine action — a ZMQ rawtx evaluation, a ZMQ rawblock, a whole
mempool reparse (initial, gap-triggered, periodic), boot reconciliation — is one item on the
engine queue (src/engine/queue.ts: a promise chain) and runs to completion before the next
item starts, in arrival order. Nothing else mutates engine state. Consequences, which are
the rules of this codebase:

- Every state change is ONE plain Redis MULTI grouping the writes that must land together
  (state + outpoint claims + outbox enqueue). An action reads what it needs, decides in
  memory, and execs one MULTI. Because no other writer exists, nothing can change between
  the read and the exec: there are NO guards, fences, tombstones, watermarks or conditional
  scripts anywhere, and there is no Lua. A read-then-write that looks racy is a sign
  something is off the queue — put it on the queue; never add a condition.
- A rejection inside a queue item is `fatal` (docker restarts the daemon, boot
  reconciliation heals). There is no "skip the failed item and keep going".
- Boot reconciliation (reconcile, ending in settleTip) is the queue's FIRST item; ZMQ opens only
  after it finishes, so nothing can be queued ahead of or during it.
- The queue is the reparser's mutex (the whole reparse body is one item) and the block
  handler's serialisation (a burst of blocks runs one at a time). Throughput is explicitly
  not a concern.
- NODE STATE. The queue serialises weir's writers; it cannot stop bitcoind from moving
  between a read and a write inside one item. One rule covers that, and it is a node
  probe, never a Redis condition: A WRITE REFLECTS THE NODE AT WRITE TIME; HISTORICAL
  PACKETS NEVER OVERWRITE A NEWER RECONCILED OUTCOME. Concretely: (a) the tip-only work
  snapshots the mempool, then confirms `getbestblockhash` is still the stored tip, and
  every step — limbo resolution included — decides from THAT snapshot, never a later
  probe (`settleTip`); (b) boot reconciles until the stored tip is the node's best and
  settles through that same path, never a separate resolution; (c) an evaluation about to
  write what asserts "this tx is in the mempool" (a `seen`, or a `dropped`/`replaced` of a
  claimant) asks `getmempoolentry` once first — absent → nothing is written, the packet is
  history (a gap-triggered reparse already reconciled the node's newer state, or the tx
  was mined/replaced meanwhile) and the block path or a later packet covers it.
- Three components run OUTSIDE the queue and are not writers of engine state:
  - the heartbeat only READS (tip, watch count, memory, outbox stats) and sends directly;
  - the admin server READS engine state and probes, and writes ONLY the watch set
    (`addresses` / `expiries`), which the engine reads but never treats as its own state —
    a watch renewed by the app while the TTL sweep expires it is decided by whichever
    write lands last, and that is accepted: expiry is unconditional;
  - the outbox drainer touches outbox keys only (`outbox`, `outbox:{id}`, `outbox:created`,
    `outbox:dead`); the engine only ever APPENDS new ids to the outbox, so the drainer is
    the outbox's one writer and its ack/retry/dead steps are plain MULTIs too.

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
Outpoint[]; absent on records written before outpoint tracking — read as `[]`)} — the
RECORD, which exists from seen-time (height 0) until tracking ends.
Working set (reconstructible): `pending` SET of txids (records at height 0); `evaluated` SET
of txids — the mempool reparse's DEDUPE LIST: every mempool txid evaluated since the last
tip block (matched or not), so a reparse fetches only what it has not looked at yet. It is
pruned at every tip block to the txids still in the mempool (a new mempool epoch). It is a
dedupe, not a guard: nothing decides from it except the reparse's fetch list and the
evaluator's early return.
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
There are no other key families: no tombstones, no retirement watermark, no mempool or
block scratch sets — set arithmetic happens in memory inside the queue item.

## Events (src/lib/types.ts — WRITTEN)

`seen | confirmed | dropped | demoted | conflicted | expired | heartbeat`.
TxEvent payload: `{version:1, event, network, txid, confs, matched:[{address,vout,valueSats}],
idempotencyKey, timestamp, blockHeight, blockHash, hex}` plus, per Outpoint tracking rule 6,
`reason`/`replacedBy` on `dropped` and `reason`/`conflictingTxid` on proven `conflicted`.
`expired` is address-scoped: `{version:1, event:'expired', network, address, idempotencyKey, timestamp}`.
It fires when the watch's lifetime ends, paid or not.
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

## Transaction lifecycle

One tracked transaction is a state machine. The physical state is the record + the set it
is a member of; `done` and `gone` are both "no record, no membership" and differ only in the
last event the consumer received.

| state | record | membership | meaning |
|---|---|---|---|
| `unseen` | none | none | weir knows nothing (or has finished/forgotten the tx) |
| `pending` | height 0 | `pending` (+ `evaluated`) | seen in the mempool, paying a watch |
| `maturing(h)` | height h | `maturing` index | mined at height h, below the max milestone |
| `limbo` | height h (old block) | `limbo` | its block was disconnected; awaiting the new chain |
| `done` | none | none | max milestone reached — tracking ended |
| `gone` | none | none | left by a verdict, or quietly |

Transitions — every code path is one of these, and each is one MULTI:

| from → to | trigger | event | written by |
|---|---|---|---|
| `unseen → pending` | mempool evaluation matches a watch | `seen` (when milestone 0) | `applyEvaluation` |
| `unseen → unseen` | evaluation matches nothing; or a repeated sighting of a tracked txid; or a historical packet (the tx is no longer in the node's mempool) | — (marked evaluated / nothing) | `applyEvaluation` / no-op |
| `unseen → maturing(h)` | mined paying a watch without a mempool sighting (missed ZMQ, direct-to-block, dropped-then-mined, re-inclusion after `done` at exactly max-milestone depth) | — (milestones follow) | `applyBlock.promoted` |
| `pending → maturing(h)` | mined | — | `applyBlock.promoted` |
| `maturing(h) → maturing(h)` | block at depth ≥ a milestone m not yet fired | `confirmed` (confs m, key with blockHash) | `applyBlock.fired` |
| `maturing(h) → done` | confs ≥ max milestone and every milestone enqueued | (last `confirmed` in the same MULTI) | `applyBlock.finished` |
| `pending → gone` | another tx spends one of its inputs (mempool or block) | `dropped` reason `replaced`, `replacedBy` | `applyEvaluation.dropped` / `applyBlock.dropped` |
| `pending → gone` | absent from the live mempool at a tip block | `dropped` reason `evicted` | `dropPending` (tip-only) |
| `maturing(h) → limbo` | its block is disconnected (reorg rewind) | — | `rewind` |
| `limbo → maturing(h')` | re-included by the new chain | — (milestones re-fire under h') | `applyBlock.promoted` |
| `limbo → gone` | a new-chain tx spends one of its inputs | `conflicted` reason `double-spend`, `conflictingTxid` | `applyBlock.conflicted` |
| `limbo → pending` | still in limbo when the tip settles, and in the validated mempool snapshot | `demoted` | `demoteToPending` (tip-only, block path or boot) |
| `limbo → gone` | still in limbo when the tip settles, absent from the snapshot | `conflicted` (by elimination) | `conflict` (tip-only, block path or boot) |
| `maturing(h) → limbo` | boot finds the stored tip off the node's active chain (a reorg while weir was down, not yet built past) | — | `rewind` (reconcile) |
| `pending \| limbo → gone` | mined but no longer paying any watch (watch removed or expired mid-flight) | — | `applyBlock.ended` |
| `any → gone` | prune-window reset (downtime longer than the node's prune window) | — (logged loudly) | `resetTracking` |

A watch's own lifecycle is one transition: `watched → expired` at a tip block whose wall
clock passed the deadline (`expireWatch`, event `expired`), unconditional.

Scenario → transitions (the nine E2E scenarios plus the two crash/burst cases):

| scenario | transitions |
|---|---|
| 1 happy path | `unseen → pending` (seen) → `maturing(h)` (confirmed:1) → `done` (confirmed:3) |
| 2 RBF fee-bump | A `pending → gone` (dropped replaced, replacedBy B) and B `unseen → pending` (seen), ONE MULTI; B then as scenario 1 |
| 3 redirect | A `pending → gone` (dropped replaced, replacedBy B) and B `unseen → unseen`, one MULTI |
| 4 reorg → demoted | `maturing(h) → limbo` (rewind) → replacement block, tip → `limbo → pending` (demoted) → next block `pending → maturing(h')` (confirmed:1 under h') → `done` |
| 5 reorg + double-spend | `maturing(h) → limbo` (rewind) → block with B spending its input: `limbo → gone` (conflicted double-spend, conflictingTxid B) inside that block's MULTI; resolveLimbo finds nothing |
| 6 TTL expiry | `watched → expired` at the next tip block; a pending tx of that address would go `pending → gone` quietly when mined |
| 7 webhook down | no state change: the events sit in the outbox and drain later |
| 8 restart mid-flight | `pending` survives the restart; boot reconciliation walks the 3 missed blocks (non-tip, non-tip, tip): `pending → maturing(h)` (confirmed:1) → `done` (confirmed:3); tip-only work at the last block only |
| 10 restart mid-reorg | `maturing(h) → limbo` by reconcile's rewind (the node's best is our tip's parent) → settle from the snapshot: `limbo → pending` (demoted, old block hash) → next block `pending → maturing(h')` (confirmed:1 under h') → `done` |
| 9 /ready | no transition; the probe reads `runtime.reconciled` |
| crash before exec | the block's MULTI never landed: NO transition, tip unchanged; boot reconciliation replays the block and every transition happens exactly once |
| reorg burst | blocks B1..Bn queued back to back; each block's MULTI applies its own transitions (rewind, promotions, proven conflicts); the tip-only work (eviction, TTL, `evaluated` prune, limbo resolution) runs only at the block that equals `getbestblockhash` — earlier blocks defer it, so a limbo tx stays in limbo until the node's view is settled |
| node moves during boot | reconcile processes best B1 (rewind: A → limbo); by its settle the node is at B2 → settle refuses (no resolution from a stale view); the next round processes B2, which re-includes A: `limbo → maturing` — never a false `conflicted` |
| historical packet | reparse (queued ahead of the packets of a ZMQ gap) records C; the queued packet for B (replaced by C) finds B absent from the node's mempool → nothing written; C's packet → already evaluated |

## Outbox (durable delivery)

Why: the events that REVERSE money (`dropped`, `demoted`, `conflicted`, `expired`) must not
be lost when the webhook is down, and no state transition may await the network (a slow
delivery inside a transition let a block race the `seen` write and corrupt the record).

Rules:
1. A state transition and its event are ONE MULTI. Every Store transition that produces an
   event appends `HSET outbox:{id} …` + `ZADD outbox now id` + `ZADD outbox:created now id`
   to the same MULTI as its state mutation. Either both persist or neither does. There are
   no conditional writes: the single writer (see Single writer) makes every read the
   transition acts on current.
2. Nothing in the engine awaits delivery. Engine code never calls the sink.
3. One drainer (src/delivery/outbox.ts) delivers: every `OUTBOX_POLL_MS` (1000, constant) it
   takes due events (`ZRANGEBYSCORE outbox -inf now LIMIT 0 50`), reads each hash, and sends
   them SERIALLY in score order (ties by the monotonic id = enqueue order, so events enqueued
   together arrive in order when the endpoint is healthy; across failures order is
   best-effort). The interval pass keeps taking batches while they come back full (a backlog
   drains at line rate). Per event: 2xx → `outboxAck` (MULTI DEL hash, ZREM outbox, ZREM
   created); failure → attempts+1, `lastError`, and either `outboxRetry` (MULTI: HSET + ZADD
   score = now + backoff, backoff = min(1000·2^(attempts−1), 300000) ms + up to 25% jitter)
   or, once `now − createdAt ≥ OUTBOX_MAX_AGE`, `outboxDead` (reads ZCARD dead + the
   overflow ids, then ONE MULTI: HSET, ZREM outbox + created, ZADD dead, and the cap — DEL
   the oldest dead hashes beyond `OUTBOX_DEAD_MAX` + ZREM them) with an error-level log
   naming the idempotencyKey. A dangling id (hash missing) is ZREM'd.
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
   the poll loop and waits for the in-flight drain pass. Every pass — interval or a direct
   `drainOnce()` — runs through ONE shared in-flight promise: a caller that finds a pass
   running waits for it, then runs; two passes never overlap and never double-send. That
   serialisation is why the outbox needs no conditional writes either.

Store surface (all methods live on Store, mirrored exactly in tests/fakes.ts):
`outboxDue(nowMs, limit): Promise<string[]>` (ids, ascending score), `outboxRead(id)` →
`{event: WeirEvent, attempts, createdAt, lastError} | null`, `outboxAck(id)`,
`outboxRetry(id, nextAtMs, attempts, lastError)`, `outboxDead(id, nowMs, attempts,
lastError, deadMax)`, `outboxStats(): Promise<{depth: number; oldestCreatedAt: number | null;
dead: number}>` (`oldestCreatedAt` = the lowest score in `outbox:created`, `ZRANGE 0 0
WITHSCORES` — exact and O(1)). Transition methods that enqueue are listed under
src/store/redis.ts.

## Outpoint tracking (replacement + proven conflicts)

Why: weir discards the inputs of the transactions it tracks, so an RBF replacement is only
noticed as `dropped` at the NEXT block (up to ~10 min), and `conflicted` is inferred from
mempool absence rather than proven. Inputs are literal bytes in every tx weir already
decodes — no txindex, no prevout resolution: weir only checks them against its OWN memory.

Model: an outpoint can legitimately have SEVERAL concurrent tracked spenders (a mempool
spender alongside a mined one during reorg lag), so ownership is a SET of claimants, never a
single owner. Claims never conflict; there is no force, no waitlist, no handover.

Rules:
1. `DecodedTx` carries `inputs: Outpoint[]` (`{txid, vout}`, prev-txid display-order hex);
   the coinbase input (txid all zeros, vout 0xffffffff) is omitted. `MaturingRecord` carries
   the same `inputs` so terminal cleanup can release them.
2. `weir:{net}:outpoint:{txid}:{vout}` SET of claimant txids (pending or maturing).
   - CLAIM = SADD, inside the MULTI that creates the record (`applyEvaluation`'s seen part)
     or promotes it (`applyBlock.promoted`). A tx's claims are complete or absent (same
     MULTI as its record).
   - RELEASE = SREM of ONLY the releaser's own txid, inside every MULTI fragment that
     deletes the record (drop, conflict, finish, end — each takes the record the caller
     read; `inputs` come from it). The key vanishes when empty. Nobody else's claim is ever
     touched. `demoteToPending` keeps claims (the tx stays tracked).
   - `resetTracking` DELs every `outpoint:*` key in the same MULTI as the records.
3. REPLACEMENT (mempool path, src/engine/txPipeline.ts) — ONE pass, one MULTI:
   `isEvaluated` → return; a record already exists (pending, maturing or limbo) → return (a
   repeated sighting, see txPipeline) → `outpointOwners(inputs)` (pipelined SMEMBERS, self
   excluded) → for EVERY distinct owner: read its record to build the payload (gone →
   nothing to do); record unmined → a `dropped` with `reason: 'replaced'`, `replacedBy:
   <this txid>`, key `{net}:{owner}:dropped:replaced:{replacedBy}` goes into this
   evaluation's `dropped` list; record MINED (maturing/limbo) → a mempool tx cannot
   displace a confirmed tx (bitcoind would not have relayed it; during reorg lag the block
   path adjudicates it — rule 4): warn, skip. Then match outputs; the whole outcome — SADD
   evaluated, every drop (SREM pending, SREM evaluated, release, DEL record, enqueue), and
   when matched the seen record (HSET height 0, SADD pending, claims, enqueue `seen`) — is
   `applyEvaluation`, one MULTI. Two spenders of one outpoint are evaluated one after the
   other (the queue), so the second always finds the first as a claimant and replaces it.
4. BLOCK PATH (src/engine/blockPipeline.ts, the input scan, before promotion): collect every
   input of every block tx (excluding coinbase), `outpointOwners` once, then for each owner
   ≠ its spender (the owner's record read for the payload; gone → skip):
   - owner in LIMBO → PROVEN conflict: `conflicted` with `conflictingTxid` = the spender,
     `reason: 'double-spend'`, timestamp = blockTime*1000 — into the block's `conflicted`;
   - owner's record MINED (height > 0) and not in limbo → impossible on a valid chain; log
     error, skip;
   - otherwise → `dropped` with `replacedBy` = the confirmed spender (a double-spend
     confirmed while ours sat in the mempool) — into the block's `dropped`.
   Every claimant of a spent outpoint is adjudicated (a field may have several). Running
   before promotion means a limbo tx cannot be both re-included and conflicted by one block.
   All of it lands in the block's ONE MULTI (`applyBlock`).
5. `resolveLimbo` keeps its by-elimination fallback (in the validated mempool snapshot →
   demoted, else conflicted) for txs the new chain neither re-included nor provably conflicted (e.g. an
   input spent by a tx weir never decoded because it was below the ring floor).
6. Payload additions (all OPTIONAL, additive — consumers ignoring them are unaffected):
   `dropped` gains `reason: 'replaced' | 'evicted'` (`evicted` is the residual verdict of the
   tip-block eviction check) and `replacedBy?: string`; `conflicted` gains
   `reason?: 'double-spend'` and `conflictingTxid?: string` when proven.
7. RETRY SAFETY NET: a tx missed by ZMQ without a sequence gap is un-evaluated and is picked
   up by the next mempool reparse — which runs PERIODICALLY (`MEMPOOL_REPARSE_INTERVAL_MS` =
   300 000, constant) as a queue item, not only at boot and on ZMQ gaps. Cost: one
   getrawmempool plus fetches of un-evaluated txids only.

Idempotency: `idem.replaced(net, txid, replacedBy)`; `conflicted` keeps its single key (one
terminal verdict per txid).

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
  settleTip) AND NOT shuttingDown AND chainLag ≤ `READY_MAX_LAG`. The runtime flags are
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
AFTER its MULTI exec'd (events_enqueued_total by event, once per event the MULTI carried),
the outbox drainer (deliveries_total, dead_lettered_total), the block pipeline
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
replaces the `lastBlockAt` closure; reconciled is set after reconcile settles; shuttingDown is
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
settleTip → mempool reparse) heals. Fatal sites: the ZMQ subscriber loop failing, ANY
engine-queue item rejecting (a rawtx evaluation, a block, a reparse — makeEngineQueue; no
"skip the failed item and keep the queue alive"), a heartbeat tick rejecting, the redis
client giving up reconnecting or ending unasked (src/store/redis.ts), the admin server's
`'error'` event (listen failure), and boot/shutdown failures. Every abnormal exit goes
through `fatal`; a clean shutdown exits 0.
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
classes' semantics exactly (e.g. a MULTI's fragments apply in the same order; the ring is a
ZSET member→score map, so two hashes can coexist at one height); a signature mismatch is
fixed in the fake, never in the real class. `FakeStore` also models the outbox (id → record,
queue as id → score, dead set) and exposes `outboxEvents(): WeirEvent[]` (queued events in
score-then-insertion order) so engine tests assert on what was ENQUEUED; delivery itself is
tested through the drainer with `FakeSink`. `FakeChain.getBestBlockHash` answers the highest
main-chain block (or `best` when a test sets it), so tests drive the tip-only gate the same
way bitcoind does.

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
unhandled, never swallowed; in index.ts every handler hands its work to the engine queue,
which is what makes the fire-and-forget safe. A subscriber-loop failure (other than close())
is fatal too. bitcoind publishes a tx's `rawtx` when it enters the mempool AND again for
every tx of a connected or disconnected block — always before the `rawblock` of a
connected block, on one socket, so the queue sees them in that order.

### src/engine/queue.ts
`export function makeEngineQueue(onFatal = fatal): { run(fn: () => Promise<void>): Promise<void> }`
— the one writer (see Single writer): a promise chain; `run` appends `fn` and resolves once
it has run. A rejection is `onFatal('engine', err)` — the returned promise never rejects.
`onFatal` is injectable for tests only.

### src/store/redis.ts
`export class Store` wrapping `redis` v4 client. Constructor `(url: string, network: Network)`.
`connect()/quit()`. Zero Lua. Reads are single commands or read-only pipelines; every
mutation is ONE MULTI. Keys via `keysFor`. Types exported: `Claimant` (= `Pick<MaturingRecord,
'txid' | 'inputs'>`), `Drop` (`{rec: Claimant; event: TxEvent}`), `EvaluationWrites`,
`BlockWrites`, `OutboxRecord`.
- watches: `isWatched(addr)`, `watchedSubset(addrs: string[]): Promise<string[]>` (one
  SMISMEMBER — return ALL matches, do not bail on first), `addWatch(addr, expiresAtMs?: number)`
  (MULTI: SADD + ZADD expiries / ZREM expiries), `removeWatch(addr): Promise<boolean>` (MULTI:
  SREM + ZREM expiries), `watchCount()`, `scanWatches(cursor: string): Promise<{cursor: string;
  addresses: string[]}>` (SSCAN, COUNT 1000), `dueExpiries(nowMs): Promise<Array<{address:
  string; expiresAtMs: number}>>`, `getExpiry(addr): Promise<number | null>` (ZSCORE expiries),
  `expireWatch(addr, event: ExpiredEvent)` — MULTI: SREM addresses, ZREM expiries, + enqueue.
- tracking reads: `isEvaluated(txid)`, `evaluatedTxids()`, `pendingTxids()`, `limboTxids()`,
  `maturingEntries(): Promise<Array<{txid: string; height: number}>>` (ZRANGE WITHSCORES,
  ascending), `readRecord(txid): Promise<MaturingRecord | null>`,
  `outpointOwners(outpoints: Outpoint[]): Promise<Map<string, string[]>>` (one SMEMBERS per
  prevout, pipelined in chunks of 1000; prevouts with claimants only, keyed `{txid}:{vout}`).
- tip / ring reads: `getTip(): Promise<Tip | null>`, `ringHashAt(height): Promise<string |
  null>`, `ringAll(): Promise<Array<{height; hash}>>` (ZRANGE 0 -1 WITHSCORES, ascending —
  INCLUDES height 0, which a `(0 +inf` range would miss on a fresh regtest ring holding
  only genesis).
- MULTI fragments (private, the building blocks every transition is composed of — the fake
  mirrors them 1:1): `enqueue` (HSET outbox:{id} + ZADD outbox + ZADD outbox:created),
  `tipOps` (HSET tip + ZADD blocks), `forgetOps(rec)` (SREM its own txid from each of its
  prevout SETs + DEL record), `dropOps(drop)` (SREM pending, SREM evaluated, forget, enqueue
  `dropped`), `conflictOps(rec, event)` (SREM pending, ZREM maturing, SREM limbo, forget,
  enqueue `conflicted`), `promoteOps(rec)` (HSET record, ZADD maturing, SREM pending, SREM
  limbo, SADD its claims).
- transitions, each ONE MULTI:
  `setTip(tip)` — tipOps (boot first-run initialisation);
  `applyEvaluation({txid, dropped: Drop[], seen: {rec, event | null} | null})` — SADD
  evaluated; dropOps for every replaced claimant; when `seen`: HSET record (height 0), SADD
  pending, SADD its claims, + enqueue `seen` when the event is non-null;
  `applyBlock({promoted, fired, finished, dropped, conflicted, ended, unindexed, tip,
  ringKeep})` — in this order: promoteOps for each promoted record; for each fired
  `{txid, fired, event}` HSET fired + enqueue `confirmed`; for each finished record ZREM
  maturing + forgetOps; dropOps for each dropped; conflictOps for each conflicted; for each
  ended record SREM pending, SREM limbo, forgetOps; ZREM maturing for the unindexed
  (dangling entries); tipOps; ZREMRANGEBYRANK blocks (keep `ringKeep`). A tx promoted and
  finished in the same block nets out to gone with its `confirmed` events enqueued;
  `rewind(ancestor: Tip, displaced: string[])` — SADD limbo + ZREM maturing for the
  displaced (records kept), ZREMRANGEBYSCORE blocks `(ancestor.height +inf`, tipOps;
  `dropPending(drop)` — dropOps (the tip-only eviction verdict, `reason: 'evicted'`);
  `demoteToPending(rec, event)` — HSET record back to height 0 / blockHash '' / fired [],
  SADD pending, SADD evaluated (the tx is in the mempool and evaluated: the reparse must not
  fetch it again), SREM limbo, + enqueue `demoted`; claims kept;
  `conflict(rec, event)` — conflictOps (the by-elimination verdict);
  `removeLimbo(txid)` — SREM limbo (a limbo entry whose record is gone: corruption cleanup);
  `forgetEvaluated(txids)` — SREM evaluated (no-op on []);
  `resetTracking(tip): Promise<string[]>` — SCAN `outpoint:*` and `maturing:*` (reads),
  collect the txids of limbo ∪ pending ∪ maturing, then ONE MULTI: DEL every scanned key,
  DEL maturing/pending/limbo/evaluated, tipOps. Preserves watches, expiries, the ring below
  and the outbox (queued events are still owed). Returns the lost txids.
- outbox: `outboxDue`, `outboxRead`, `outboxAck`, `outboxRetry`, `outboxDead`, `outboxStats`
  (contracts under Outbox).
- meta: `memoryInfo(): Promise<{usedBytes: number; maxBytes: number | null}>` (INFO memory),
  `maxmemoryPolicy(): Promise<string | null>` (CONFIG GET; null ONLY when the error is a
  command-access refusal — `/unknown command|not allowed|NOPERM|disabled|CONFIG/i`, e.g.
  managed redis — anything else, such as connection loss, is rethrown).
- metrics: `weir_events_enqueued_total{event}` is bumped once per event a MULTI carried,
  AFTER its exec.
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
`export function makeTxEvaluator(deps): (tx: DecodedTx) => Promise<void>` and
`export function makeRawTxHandler(deps): (raw: Buffer) => Promise<void>` (decode → evaluate).
Deps: `store`, `rpc: Pick<Rpc, 'getMempoolEntry'>`, `cfg`. Runs as one engine-queue item. Evaluate — reads, then ONE MULTI (Outpoint tracking rule 3):
if `isEvaluated` → return. If `readRecord` is non-null → return: the tx is already tracked
(pending, maturing or limbo) and this is a repeated sighting — bitcoind re-publishes `rawtx`
for every tx of a connected AND a disconnected block, after the tip prune forgot the txid
from `evaluated`; `seen` is the `unseen → pending` transition only, and a maturing/limbo
record must never be put back to height 0. Replacement check: `outpointOwners(inputs)`,
every distinct claimant ≠ this txid → its record (gone → skip; mined → warn, skip; unmined →
a `dropped`/`replaced` for this evaluation's `dropped` list). Match. If there is anything to
drop or a match: the NODE PROBE (Single writer) — `getMempoolEntry(txid)` null → log, return
with NOTHING written (not even evaluated). Build the `seen` TxEvent (confs 0, block fields
null, timestamp now) when matched and seenEnabled. Then `applyEvaluation({txid, dropped,
seen})` — one MULTI; nothing awaited from the network. A non-matching, non-replacing tx never
probes: it is just marked evaluated.

### src/engine/mempool.ts
`export function makeMempoolReparser(deps): () => Promise<void>` — the whole body is one
engine-queue item (index.ts), so it has no mutex and no snapshot key: getRawMempool →
`evaluatedTxids()` → fresh = mempool − evaluated → fetch `getRawTransactionVerbose` in
batches of 32 (reads only) → for each, IN ORDER: a tx that vanished (null) is skipped; a tx
that was mined meanwhile (`blockhash` set) is skipped too — the block pipeline owns mined txs
and a `seen` for it would be false; else decode → evaluate (reuse txPipeline's evaluator).
Exports `MEMPOOL_REPARSE_INTERVAL_MS` (300 000, constant): index.ts queues the reparser on
that interval as the retry safety net (Outpoint tracking rule 7).

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
`export async function enterLimboAndRewind(deps, ancestorHeight): Promise<void>` — reads the
maturing entries with inclusion height > ancestor and the ancestor's ring hash, then
`store.rewind(ancestor, displaced)`: ONE MULTI (SADD limbo + ZREM maturing, records kept;
ring truncated above the ancestor; tip = ancestor). The replacement chain now processes as
a plain connected walk: no second fork search, one hash per height in the ring.
Re-inclusion is discovered NATURALLY by the block pipeline's promotion step: a limbo txid
found in a new block re-enters maturing (fresh height/blockHash, fired []) and leaves limbo —
no event at re-inclusion; milestones re-fire on the sweep with new-blockhash idempotency keys.
`export async function resolveLimbo(deps, mempool: ReadonlySet<string>): Promise<void>` —
runs only inside `settleTip` (a block that is the node's tip, or boot), with the mempool
SNAPSHOT that path validated; it never probes the node itself (a live probe could describe
a block that landed after the validation). For each txid still in limbo (a PROVEN conflict —
Outpoint tracking rule 4 — already left limbo inside the block's MULTI, so it is never
adjudicated twice),
  - in the snapshot → build `demoted` (confs 0, block fields = the OLD block,
    timestamp now) and `demoteToPending(rec, ev)` (one MULTI, enqueues it);
  - else → build `conflicted` (confs = last confirmed depth or 0, old block fields) and
    `conflict(rec, ev)` (one MULTI: full cleanup + enqueue). Terminal.
Because limbo is durable, a crash anywhere in the sequence re-resolves on the next block/boot.

### src/engine/blockPipeline.ts
`export function makeBlockPipeline(deps): { processBlock(raw: Buffer): Promise<void>; settleTip(): Promise<boolean> }`
(`makeBlockProcessor(deps)` = its `processBlock` alone, what the ZMQ path queues). Boot
(reconcile) uses both, inside the queue's first item. `settleTip` is the tip-only work for
the STORED tip: snapshot `getrawmempool`, then `getbestblockhash`; equal to the stored tip →
steps a–d below from that snapshot, true; otherwise nothing written, false (the node moved
on; a newer block settles); no tip yet → true.
Sequence for a block B (hash H, prev P, height h from getBlockHeader(H)):
1. connectivity: tip = getTip(). If tip === null → first run: process B standalone (no gap walk).
   If H is the tip or already in the ring → duplicate, skip (a replay after a crash that
   happened AFTER the block's exec). If P === tip.hash → connected. Else: findForkPoint (a
   pure gap yields empty disconnected); on a REORG first `enterLimboAndRewind(ancestor)`
   (one MULTI). Then, if blocks are missing (ancestor+1 < h), the PRUNE-WINDOW GUARD runs
   before walking: getBlockchainInfo; if the node is pruned and ancestor+1 < pruneheight,
   catch-up is impossible — `resetTracking({hash: P, height: h−1})` (one MULTI: tracking
   wiped, tip/ring jumped to B's parent), log the lost txids LOUDLY, continue (watches
   unaffected; still-unconfirmed txs re-fire `seen` via the mempool reparse). Otherwise walk
   ancestor+1 .. h-1 via getBlockHash → getBlockRaw → process (in order, as NON-tip blocks),
   then B itself.
2. per connected block, READS then ONE MULTI: `pendingTxids` + `limboTxids` → the INPUT SCAN
   (Outpoint tracking rule 4: every block input → `outpointOwners`, chunked pipelined
   SMEMBERS; for EVERY claimant ≠ its spender: limbo → a proven `conflicted` with `timestamp
   = blockTime*1000` (and the local limbo snapshot forgets it); mined and not in limbo →
   error, skip; otherwise a `dropped`/`replaced` (the local pending snapshot forgets it)) →
   resolve the block's DISTINCT output addresses with `watchedSubset` ONCE (chunked at 1000
   addresses) → `matchAgainst` per tx: for each block tx that matches (check EVERY block tx,
   not just pending ∩ block — a payment never seen in the mempool still confirms): build the
   MaturingRecord from the block's own decode → `promoted`, for every origin: limbo txid →
   re-inclusion; pending txid → promotion; neither → never-seen. The `evaluated` flag is NOT
   a gate: it only says the mempool evaluator looked once — possibly before the address was
   watched, or before a crash lost the record — and a mined payment to a watched address
   must confirm regardless. A tracked (pending or limbo) tx that no longer matches any watch
   (watch removed mid-flight) → `ended` (no event).
3. milestone sweep, in memory: records = every `maturingEntries()` entry's record (a missing
   record → `unindexed`, warned) overlaid with the block's `promoted`; for each: confs = h −
   height + 1; for each confirm milestone m in (fired ∌ m) with confs ≥ m: `fired` gets
   `{txid, fired+m (sorted), confirmed event (confs = m, block fields from the record,
   timestamp = blockTime*1000 of B)}` — fired is recorded at ENQUEUE time; delivery retry is
   the outbox's job. When confs ≥ maxMilestone AND all milestones are fired → `finished`
   (tracking ends).
4. `applyBlock({promoted, fired, finished, dropped, conflicted, ended, unindexed, tip: {H, h},
   ringKeep: ringSize})` — the block's ONE MULTI, tip and ring included. Crash before exec =
   nothing happened; boot reconciliation replays B. `weir_blocks_processed_total` after it.
5. TIP-ONLY WORK — `settleTip`, only when `isTip` (false for the catch-up walk) and only
   when B is the node's CURRENT tip. The steps below consult node state (mempool, wall
   clock), which is wrong for a block that is not the node's tip: a catch-up block (a
   pending tx mined in a LATER missed block would be falsely evicted) or a queued burst
   during a reorg. Snapshot `getRawMempool` FIRST, then `getBestBlockHash`; if it is not H →
   log, return (the next tip block picks the work up). Snapshotting before the check means a
   block that lands between the two calls invalidates the snapshot instead of poisoning it;
   every step below decides from the snapshot, never a later probe. Each step is its own
   MULTI and safe to repeat:
   a. eviction check: every `pendingTxids()` member absent from the snapshot vanished
      without being mined (replacements were caught by the input scans, so this is the
      residual verdict): `dropPending({rec, event})` with `reason: 'evicted'`, key
      `idem.dropped(net, txid, h)`, hex + matched from the seen-time record (a dropped tx
      cannot be re-fetched from a pruned node; a missing record → error log, empty fields);
   b. TTL sweep: dueExpiries(now) → `expireWatch(addr, ev)`;
   c. `forgetEvaluated(evaluated − snapshot)` — the reparse dedupe list starts a new mempool
      epoch;
   d. `resolveLimbo(deps, snapshot)`.
There are no delivery-failure branches in the engine: every event is enqueued in the same
MULTI as its state change and the outbox retries it.

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
tick (store failure) is fatal; an RPC failure is not. Reads only — not an engine writer.

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
`export async function reconcile(deps: {cfg, store, rpc, processBlock, settleTip}): Promise<void>`
— rounds of (reconcileOnce → settleTip) until settle succeeds (the stored tip IS the node's
best as of a validated snapshot), capped at 20 rounds (then the next ZMQ block's gap walk
takes over, warned). reconcileOnce: tip = getTip(). If null: initialize tip/ring to the
current best block (getBestBlockHash + header; `setTip`, one MULTI) — forward-only, no
backfill. If best === tip.hash → nothing. Else, if the best block's height ≤ tip.height, the
stored tip cannot be on the node's active chain (a reorg while weir was down that the node
has not yet built past — e.g. `invalidateblock` with no replacement mined): `findForkPoint`
from best, `enterLimboAndRewind` (one MULTI; `weir_reorgs_total`), and if the fork point IS
best → nothing more to process. Otherwise (a gap, or a reorg the node has built past) hand
the best block to the processor, whose connectivity step walks/rewinds by itself. Then
`settleTip` — the same snapshot-validated path a tip block uses — resolves whatever is in
limbo (a crash between rewind and resolution, or the rewind just made). There is NO separate
boot-time limbo resolution: if the node advanced while we reconciled, settle refuses and the
next round processes the new best (which may re-include a limbo tx) before anything is
adjudicated.

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
  `{address, network, expiresAt: number | null}`. Idempotent. Not an engine write: the watch
  set is the engine's INPUT (see Single writer).
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
(logged; acks free memory before reconcile writes under a full redis) → `makeEngineQueue()`
+ `makeBlockPipeline` → the queue's first item, awaited: `reconcile` (which ends in
`settleTip`) → `runtime.reconciled = true` →
startZmq (rawtx → `engine.run(handleRawTx)`, rawblock → `engine.run(processBlock)`, gap →
`engine.run(reparse)`; each receipt stamps `runtime.lastZmqTxAt` / `lastZmqBlockAt` before
queuing) → initial mempool reparse (queued, not awaited) → periodic reparse timer
(`setInterval(() => engine.run(reparse), MEMPOOL_REPARSE_INTERVAL_MS)`, unref'd) →
startHeartbeat (takes `rpc` for getBlockCount). Engine deps get NO sink (they enqueue); the
sink goes only to the drainer and the heartbeat. SIGINT/SIGTERM → `runtime.shuttingDown =
true` FIRST, then close zmq, clear the reparse timer, stop heartbeat, close admin, await
drainer.stop(), store.quit, exit 0.
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
