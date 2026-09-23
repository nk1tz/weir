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
default 1000). There is no `WEBHOOK_MAX_RETRIES`: retry is the outbox's job, bounded by age.

## Redis schema (src/store/keys.ts — WRITTEN)

See `keysFor()`. Public: `addresses` SET (+ `expiries` ZSET member=address score=expiresAt-ms).
Durable chain view: `tip` HASH {hash,height}; `blocks` ZSET (member=hash, score=height,
pruned to `ringSize`); `maturing` ZSET (member=txid, score=inclusion height);
`maturing:{txid}` HASH {height, blockHash, matched(JSON), fired(JSON array), hex}.
Working set (reconstructible): `pending`, `evaluated`, `mempool:current`, `mempool:postBlock`,
`blockTxids` — all SETs of txids.
Reorg state (durable): `limbo` SET — txids whose inclusion block was disconnected by a reorg,
awaiting re-resolution (re-included by the new chain / demoted to mempool / conflicted). Kept
in redis so a crash mid-reorg finishes resolving on the next block or boot.
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
resurrect them (see `recordSeen`). Pruned on every tip block below now − TOMBSTONE_TTL_MS
(3,600,000; constant). The tombstone bounds the STORE side; the EVALUATION FENCE
(`MAX_EVALUATION_AGE_MS` = 600,000, far below the TTL) bounds the evaluator side, so no
evaluation can outlive the tombstone that guards it. `conflicted` needs no permanent set: an
invalid tx cannot re-enter the mempool, and the only other path back, a stale evaluation, is
fenced.

## Events (src/lib/types.ts — WRITTEN)

`seen | confirmed | dropped | demoted | conflicted | expired | heartbeat`.
TxEvent payload: `{version:1, event, network, txid, confs, matched:[{address,vout,valueSats}],
idempotencyKey, timestamp, blockHeight, blockHash, hex}`.
`expired` is address-scoped: `{version:1, event:'expired', network, address, idempotencyKey, timestamp}`.
`heartbeat`: `{version:1, event:'heartbeat', network, tipHeight, watchCount, memoryUsedPct,
outboxDepth, outboxOldestAgeSec, deadLetterCount, idempotencyKey, timestamp}`.
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
   to the same MULTI as its state mutation. Either both persist or neither does. The one
   exception in MECHANISM (not in atomicity) is `recordSeen`: it is a Lua script (EVAL) rather
   than a MULTI, because it legitimately races the block pipeline (a tx can be mined while its
   mempool evaluation is between its read and its write) and needs a guard a MULTI cannot
   express — see src/store/redis.ts.
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
   `recordSeen(rec, event, startedAtMs)` refuses (returns false, warns with the age) when
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
  Transaction.getId(); EVERY output present in `outputs` with real `vout` index and `valueSats`;
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
  `recordSeen(rec: MaturingRecord, event: TxEvent | null, startedAtMs: number): Promise<boolean>`
  — HSET record (height 0), SADD pending, SADD evaluated, + enqueue when event is non-null
  (null when seen is disabled). ONE Lua script, FENCED (refused when now − startedAtMs >
  MAX_EVALUATION_AGE_MS, see Outbox rule 8) and GUARDED: a no-op (resolves false) when `evaluated` already
  holds the txid (a duplicate concurrent evaluation), or the record already has height > 0
  (the block pipeline mined + promoted it between this evaluation's read and its write —
  without the guard a late `seen` write would put a mined record back to height 0), or the
  txid is TOMBSTONED (tracking already ended and the record is GONE, so the first two guards
  cannot see it: a mempool RPC that read the tx before the block and returned after the
  final-milestone cleanup would otherwise recreate a height-0 pending record and the next
  block would emit a false `dropped` for a confirmed payment);
  `markFired(txid, fired: number[], event: TxEvent)` — HSET fired + enqueue `confirmed`
  (fired is recorded at ENQUEUE time; delivery retry is the outbox's job);
  `dropPending(txid, event: TxEvent)` — SREM pending, SREM evaluated, DEL record, + enqueue;
  `demoteToPending(rec, event: TxEvent)` — HSET record back to height 0/blockHash ''/fired [],
  SADD pending, SADD evaluated, SREM limbo, + enqueue `demoted` (`evaluated` is part of it
  because the tip prune forgot the txid when it was mined; without it the next reparse would
  emit a second `seen`);
  `conflict(txid, event: TxEvent)` — SREM pending, ZREM maturing, DEL record, SREM limbo,
  ZADD tombstones, + enqueue;
  `expireWatch(addr, event: ExpiredEvent)` — SREM addresses, ZREM expiries, + enqueue;
  `endTracking(txid)` — SREM pending, SREM limbo, DEL record (no event: watch removed mid-flight);
  `finishMaturing(txid, nowMs)` — DEL record, ZREM maturing, ZADD tombstones score=nowMs (no
  event: the final-milestone cleanup — tracking ENDED). `dropPending` deliberately does NOT
  tombstone: a rebroadcast may legitimately re-fire `seen`.
- tombstones: `isTombstoned(txid)` (ZSCORE non-nil), `pruneTombstones(beforeMs)`
  (ZREMRANGEBYSCORE -inf beforeMs; the tip block calls it with now − TOMBSTONE_TTL_MS).
- maturing: `promoteToMaturing(rec: MaturingRecord)` — THE mined-tx promotion, one MULTI:
  HSET record, ZADD maturing, SREM pending, SREM limbo, SADD evaluated (every promotion
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
- `clearTracking(): Promise<string[]>` — prune-window guard: wipe maturing index + records +
  pending + limbo + evaluated + mempool scratch, PRESERVING watches/expiries/tip/ring AND the
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
Evaluate: if `isEvaluated` → return. Match. If no match → markEvaluated, done. If match:
build the `seen` TxEvent (confs 0, block fields null, timestamp now) when seenEnabled, then
`recordSeen(rec, event | null, startedAtMs)` — one atomic fenced + guarded step, nothing
awaited from the network; `false` (a concurrent path already tracks or mined the tx, or the
evaluation is older than MAX_EVALUATION_AGE_MS) is logged and is the end of the evaluation.
The old "delivery failed → leave un-evaluated" path no longer exists: the outbox owns retry.

### src/engine/mempool.ts
`export function makeMempoolReparser(deps): () => Promise<void>` — module-level mutex (skip
if running). getRawMempool → replaceCurrentMempool → newMempoolTxids → for each (bounded
concurrency 32): startedAtMs = now → getRawTransactionVerbose → decode → evaluate(tx,
startedAtMs) (reuse txPipeline evaluator; the fence clock starts BEFORE the RPC);
tx that vanished (null) is skipped; tx that was mined meanwhile (`blockhash` set) is skipped
too — the block pipeline owns mined txs and a `seen` for it would be false. Then
clearCurrentMempool.

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
(and at boot when already reconciled): for each txid still in limbo,
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
   `dropPending(txid, ev)` — one MULTI (pending, evaluated so a rebroadcast can re-fire
   `seen`, record, + enqueue).
5. TTL sweep — TIP BLOCKS ONLY: dueExpiries(now) → `expireWatch(addr, ev)` (one MULTI).
6. pruneEvaluated + pruneTombstones(now − TOMBSTONE_TTL_MS) (tip only), setTip({hash: H,
   height: h}), ringPut, ringPrune(ringSize).
A tracked tx that no longer matches any watch → `endTracking(txid)` (no event).
There are no delivery-failure branches in the engine: every event is enqueued atomically
with its state change and the outbox retries it.

### src/engine/heartbeat.ts
`export function startHeartbeat(deps): {stop(): void}` — setInterval(heartbeatInterval s):
build HeartbeatEvent from getTip/watchCount/memoryInfo/outboxStats and `sink.send` it
DIRECTLY (not via the outbox — proof-of-life must reflect now; a failed send is warned, the
next interval is the retry). Payload: `{version:1, event:'heartbeat', network, tipHeight,
watchCount, memoryUsedPct, outboxDepth, outboxOldestAgeSec (null when empty), deadLetterCount,
idempotencyKey, timestamp}`. Skipped when interval 0. A rejected tick is fatal.

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
`export function startAdminServer(deps): {close(): Promise<void>; port: Promise<number>}` —
node:http only, no framework. Only constructed when adminToken set. `port` resolves with
the bound port once listening (`ADMIN_PORT=0` = ephemeral; tests use it). Every route
(except /health) requires `authorization: Bearer <ADMIN_TOKEN>` (timingSafeEqual) → 401 otherwise.
- `POST /watches` body `{address: string, ttl?: number}` → validate with isValidAddress →
  422 `{error}` on invalid; addWatch(+expiry from ttl ?? watchDefaultTtl when > 0) → 201
  `{address, network, expiresAt: number | null}`. Idempotent.
- `DELETE /watches/:address` → 204 (idempotent; 204 even if absent).
- `GET /watches?cursor=0` → `{addresses, cursor}` (SSCAN passthrough; cursor "0" = done;
  a cursor that is not `/^\d+$/` → 400).
- `GET /watches/:address` → 200 `{address, watched: true, expiresAt}` or 404.
- `GET /health` (no auth) → 200/503 `{ok, redis, rpc, tipHeight, secondsSinceLastBlock, watchCount,
  outboxDepth, outboxOldestAgeSec, deadLetterCount}` (`ok` is still redis && rpc — outbox
  depth is a signal for the operator, not a readiness failure: the daemon is healthy when the
  consumer is down).
Reject bodies > 4KB with a real 413 response (`connection: close`; the socket is never
destroyed before the status is written). JSON errors as `{error: string}`. An unhandled
error inside a request handler → `500 {error: 'internal error'}` (request/response path:
never fatal); a server `'error'` event (listen failure) → `fatal` (background path).

### src/index.ts (integration)
loadConfig → Store.connect → preflight → startOutboxDrainer → ONE awaited `drainOnce()`
(logged; acks free memory before reconcile writes under a full redis) → reconcile →
resolveLimbo → startZmq (rawtx → txHandler, rawblock → blockHandler, gap → reparser) →
initial mempool reparse (async) → startHeartbeat → admin server if token. Engine deps get
NO sink (they enqueue); the sink goes only to the drainer and the heartbeat. SIGINT/SIGTERM
→ close zmq, stop heartbeat, close admin, await drainer.stop(), store.quit, exit 0.
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
