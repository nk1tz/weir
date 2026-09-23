/**
 * Store — the daemon's redis-backed memory, one redis@4 client, keys via `keysFor(network)`.
 * Spec: docs/DESIGN.md "src/store/redis.ts" (and "Redis schema").
 *
 * INVARIANTS
 * - A `maturing:{txid}` record exists from SEEN-time onward (`recordSeen`, height 0 /
 *   blockHash ''); the `maturing` ZSET only ever indexes MINED txs. `promoteToMaturing` =
 *   one MULTI (HSET record + ZADD + leave pending/limbo + mark evaluated), `removeMaturing` =
 *   DEL record + ZREM, `unindexMaturing` = ZREM only (the maturing→limbo transition keeps
 *   the record), `demoteToPending` = one MULTI (record back to height 0 + SADD pending +
 *   SADD evaluated + SREM limbo — evaluated so the next reparse does not re-fire `seen`).
 * - OUTBOX: every transition that produces an event enqueues it ATOMICALLY with its state
 *   mutation — `HSET outbox:{id}` + `ZADD outbox now id` + `ZADD outbox:created now id`
 *   appended to the same MULTI (`enqueue`). `recordSeen` is the one transition that
 *   legitimately races the block pipeline (a tx can be mined while its mempool evaluation
 *   is in flight), so it is a Lua script instead of a MULTI: same atomicity, plus guards
 *   that refuse to double-track an already-evaluated txid, overwrite an already-mined
 *   record, or resurrect a TOMBSTONED txid (tracking ended: final milestone or conflict).
 *   Nothing here awaits the network; delivery is the drainer's job (src/delivery/outbox.ts).
 * - Event ids are monotonic (`makeEventId`): ms prefix + per-ms sequence + random suffix,
 *   so redis' tie order for equal ZSET scores (by member) IS enqueue order.
 * - RETIREMENT WATERMARK: `dropPending` and `replacePending` ZADD the txid into `retired`
 *   (score = exit time). `recordSeen` refuses (stale) an evaluation that STARTED at or before
 *   that exit: a duplicate evaluation (ZMQ + reparse) that was in flight when the tx was
 *   replaced must not resurrect it as a fresh pending record (a second verdict later). An
 *   evaluation started after the exit is a real rebroadcast and records normally; the
 *   watermark stays (a later exit overwrites it) and is pruned with the tombstones. Drops do
 *   NOT tombstone: a dropped-then-mined tx must still confirm through the block path.
 * - EVALUATION FENCE: `recordSeen` and `replacePending` refuse an evaluation older than MAX_EVALUATION_AGE_MS
 *   (measured from ZMQ receipt / the reparser's RPC issue). Tombstones bound the STORE side
 *   for TOMBSTONE_TTL_MS; the fence bounds the EVALUATOR side, so a response parked longer
 *   than the tombstone can never land after the tombstone is pruned.
 * - OUTPOINTS: `outpoint:{txid}:{vout}` is a SET of claimant txids — every pending or
 *   maturing spender of that prevout — so a replacement (mempool) or a confirmed double-spend
 *   (block) is detected from the tx bytes alone. CLAIM = SADD (`recordSeen`'s Lua,
 *   `promoteToMaturing`'s MULTI; never conflicts), RELEASE = SREM of ONLY the releaser's own
 *   txid inside every transition that deletes the record (`dropPending`, `replacePending`,
 *   `conflict`, `finishMaturing`, `endTracking` — the prelude's `release` reads `inputs`
 *   from the record). `demoteToPending` keeps claims; `clearTracking` DELs every
 *   `outpoint:*` key. The Lua scripts build these key names from a prefix ARGV — fine on the
 *   single redis instance weir targets (see Topology), not on Redis Cluster.
 * - NO DECISION FROM A STALE READ: every mutating transition re-validates the record's LIVE
 *   state inside its Lua and returns 0 — nothing written, nothing enqueued — when a caller's
 *   read is stale: `replacePending` (fenced) and `dropPending` both require the txid to be
 *   pending AND its record to exist with height 0; a second caller never gets a second
 *   verdict. `sweepOrphanRecords` deletes a record only while it has no live membership.
 *   The block + reorg path is one serialized writer, so `conflict`, `finishMaturing`,
 *   `endTracking`, `demoteToPending`, `promoteToMaturing` and `markFired` carry no guard.
 * - Single redis instance: multi-key MULTI/EVAL without hash tags — not Redis Cluster.
 * - Bounded reconnects: boot fails fast on a bad REDIS_URL (connect() rejects). At runtime,
 *   exhausting the retries is FATAL from inside the reconnect strategy: node-redis emits no
 *   terminal event when it gives up, it just leaves a closed client — with no command in
 *   flight nothing would ever notice (a zombie process). An 'end' we did not ask for
 *   (quit()) is fatal for the same reason.
 */

import { randomBytes } from 'node:crypto'
import { createClient } from 'redis'
import type { ExpiredEvent, MaturingRecord, Network, Outpoint, Tip, TxEvent, WeirEvent } from '../lib/types'
import { describeError, fatal, log } from '../lib/log'
import { type Keys, keysFor, outpointField } from './keys'

const CTX = 'store'

/** Connection attempts before giving up (boot: connect() rejects; runtime: fatal). */
const MAX_RECONNECTS = 5

/** SMEMBERS per pipelined round trip when resolving outpoint claimants. */
const OUTPOINT_CHUNK = 1000

/**
 * Managed redis (Upstash, ElastiCache, ...) blocks CONFIG with an explicit command-access
 * denial. Only those messages count as "blocked"; an auth failure (WRONGPASS/NOAUTH — Upstash's
 * WRONGPASS text even contains "disabled") or any transport error must surface as-is.
 */
const CONFIG_BLOCKED = /unknown command|unknown subcommand|NOPERM|no permissions|not allowed|not permitted|disabled/i
const AUTH_FAILURE = /WRONGPASS|NOAUTH|AUTH failed/i
function isConfigBlocked(msg: string): boolean {
  return CONFIG_BLOCKED.test(msg) && !AUTH_FAILURE.test(msg)
}

type Client = ReturnType<typeof createClient>
type Multi = ReturnType<Client['multi']>

let eventSeqMs = -1
let eventSeq = 0

/**
 * Outbox event id: `<nowMs padded 15>-<seq within that ms, padded 8>-<8 hex random>`. ZSET
 * ties (equal scores) sort by member, so ids enqueued in the same millisecond deliver in
 * enqueue order; the sequence restarts at 0 whenever the millisecond changes.
 */
export function makeEventId(nowMs: number): string {
  if (nowMs !== eventSeqMs) {
    eventSeqMs = nowMs
    eventSeq = 0
  } else {
    eventSeq++
  }
  return `${String(nowMs).padStart(15, '0')}-${String(eventSeq).padStart(8, '0')}-${randomBytes(4).toString('hex')}`
}

/**
 * An evaluation older than this (from ZMQ receipt, or from the moment the reparser issued
 * getrawtransaction) is refused by `recordSeen`. MUST stay far below the block pipeline's
 * TOMBSTONE_TTL_MS: the tombstone covers the store for an hour, the fence guarantees no
 * evaluation can outlive it.
 */
export const MAX_EVALUATION_AGE_MS = 600_000

/** What `recordSeen` did: `recorded`; `skipped` (a concurrent path already tracks, mined or ended the tx); `stale` (the fence or the retirement watermark refused it, warned). */
export type RecordSeenOutcome = 'recorded' | 'skipped' | 'stale'
/** What `replacePending` did: `replaced` (event enqueued); `skipped` (no longer pending-unmined — nothing written); `stale` (the fence refused it, warned — nothing written). */
export type ReplaceOutcome = 'replaced' | 'skipped' | 'stale'

/** What the drainer reads back for one queued event (`outboxRead`). */
export interface OutboxRecord {
  event: WeirEvent
  attempts: number
  /** unix ms when the event was enqueued — the dead-letter age clock */
  createdAt: number
  lastError: string | null
}

/**
 * Shared Lua prelude, prepended to every releasing/enqueuing script:
 * - `pendingUnmined(pending, recordPrefix, txid)`: the LIVE-state check the guarded
 *   transitions re-validate — a member of `pending` whose record exists with height 0.
 * - `release(record, outpointPrefix, txid)`: for each of the record's `inputs`, SREM ONLY
 *   `txid` from that prevout's claimant SET (`outpointPrefix .. '{txid}:{vout}'`; the key
 *   vanishes when empty). Nobody else's claim is ever touched.
 * - `enqueue(...)`: the outbox write.
 */
const LUA_PRELUDE = `
local function pendingUnmined(pending, recordPrefix, txid)
  if redis.call('SISMEMBER', pending, txid) == 0 then return false end
  local h = redis.call('HGET', recordPrefix .. txid, 'height')
  return h and tonumber(h) == 0
end
local function release(record, outpointPrefix, txid)
  local inputs = redis.call('HGET', record, 'inputs')
  if not inputs then return end
  for _, o in ipairs(cjson.decode(inputs)) do redis.call('SREM', outpointPrefix .. o.txid .. ':' .. o.vout, txid) end
end
local function enqueue(outbox, created, rec, id, payload, event, key, now)
  redis.call('HSET', rec, 'payload', payload, 'event', event, 'idempotencyKey', key, 'attempts', '0', 'createdAt', now)
  redis.call('ZADD', outbox, now, id)
  redis.call('ZADD', created, now, id)
end
`.trim()

/**
 * recordSeen as ONE atomic script (see module doc). Returns 1 when the tx was recorded;
 * -1 when the evaluation is STALE (now − startedAt > maxAge: the fence); 0 when it was
 * skipped because a concurrent path already handled it: `evaluated` holds the txid (a
 * duplicate evaluation), the record already has height > 0 (the block pipeline mined +
 * promoted it between this evaluation's read and its write), or the txid is TOMBSTONED
 * (tracking already ENDED — final milestone or conflict — and the record is gone, so the
 * first two guards would let a stale evaluation resurrect it as a height-0 pending record
 * and the next block would emit a false `dropped`); -3 when the txid is RETIRED (dropped or
 * replaced) at a time this evaluation predates (`startedAt <= exitAt`: a duplicate evaluation
 * that was in flight when the tx left — reported as `stale`; a later evaluation is a real
 * rebroadcast and passes). The tx's inputs are CLAIMED in the same step: SADD txid into
 * each prevout's SET (ARGV[15] = the `{txid}:{vout}` fields joined by ',' — empty = none; a
 * comma never appears in hex or a number, so the split is exact — ARGV[16] = the key prefix).
 * KEYS: record, pending, evaluated, outbox, outbox record (unused when ARGV[7] is ''), tombstones, outbox:created, retired.
 * ARGV: txid, height, blockHash, matched, fired, hex, eventId, payload, event, idempotencyKey, nowMs, startedAtMs, maxAgeMs, inputs, outpointFields, outpointPrefix.
 */
const RECORD_SEEN_LUA = `${LUA_PRELUDE}
if tonumber(ARGV[11]) - tonumber(ARGV[12]) > tonumber(ARGV[13]) then return -1 end
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then return 0 end
local h = redis.call('HGET', KEYS[1], 'height')
if h and tonumber(h) > 0 then return 0 end
if redis.call('ZSCORE', KEYS[6], ARGV[1]) then return 0 end
local exitAt = redis.call('ZSCORE', KEYS[8], ARGV[1])
if exitAt and tonumber(ARGV[12]) <= tonumber(exitAt) then return -3 end
redis.call('HSET', KEYS[1], 'height', ARGV[2], 'blockHash', ARGV[3], 'matched', ARGV[4], 'fired', ARGV[5], 'hex', ARGV[6], 'inputs', ARGV[14])
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[1])
for field in string.gmatch(ARGV[15], '[^,]+') do redis.call('SADD', ARGV[16] .. field, ARGV[1]) end
if ARGV[7] ~= '' then enqueue(KEYS[4], KEYS[7], KEYS[5], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11]) end
return 1
`.trim()

/**
 * replacePending: the SPENDER's own state comes first — an evaluation that predates its own
 * tx's exit (`ZSCORE retired spender` ≥ startedAt) or whose tx is tombstoned may not act on
 * anyone (-3: stale, nothing written); then fenced (-1: stale, nothing written) and GUARDED
 * (0: the owner is not pending-unmined — not in `pending`, or its record is missing or
 * already has height > 0: the block pipeline mined it, or another caller replaced it first —
 * nothing written, nothing enqueued). Otherwise SREM pending, SREM evaluated, release its
 * claims, DEL record, ZADD retired (the watermark, score = now), enqueue `dropped`; returns 1.
 * KEYS: record, pending, evaluated, outbox, outbox record, outbox:created, retired, tombstones.
 * ARGV: txid, eventId, payload, event, idempotencyKey, nowMs, startedAtMs, maxAgeMs, recordPrefix, outpointPrefix, spenderTxid.
 */
const REPLACE_PENDING_LUA = `${LUA_PRELUDE}
local spenderExit = redis.call('ZSCORE', KEYS[7], ARGV[11])
if spenderExit and tonumber(ARGV[7]) <= tonumber(spenderExit) then return -3 end
if redis.call('ZSCORE', KEYS[8], ARGV[11]) then return -3 end
if tonumber(ARGV[6]) - tonumber(ARGV[7]) > tonumber(ARGV[8]) then return -1 end
if not pendingUnmined(KEYS[2], ARGV[9], ARGV[1]) then return 0 end
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[1])
release(KEYS[1], ARGV[10], ARGV[1])
redis.call('DEL', KEYS[1])
redis.call('ZADD', KEYS[7], ARGV[6], ARGV[1])
enqueue(KEYS[4], KEYS[6], KEYS[5], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
return 1
`.trim()

/**
 * dropPending (the tip-block dropped check, serialized — no fence): GUARDED like
 * replacePending (0 when the txid is no longer pending-unmined: a mempool replacement or
 * the block pipeline got there first — nothing written, nothing enqueued). Otherwise SREM
 * pending, SREM evaluated, release its claims, DEL record, ZADD retired (the watermark),
 * enqueue `dropped`; returns 1.
 * KEYS: record, pending, evaluated, outbox, outbox record, outbox:created, retired.
 * ARGV: txid, eventId, payload, event, idempotencyKey, nowMs, recordPrefix, outpointPrefix.
 */
const DROP_PENDING_LUA = `${LUA_PRELUDE}
if not pendingUnmined(KEYS[2], ARGV[7], ARGV[1]) then return 0 end
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[1])
release(KEYS[1], ARGV[8], ARGV[1])
redis.call('DEL', KEYS[1])
redis.call('ZADD', KEYS[7], ARGV[6], ARGV[1])
enqueue(KEYS[4], KEYS[6], KEYS[5], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
return 1
`.trim()

/**
 * conflict: SREM pending, ZREM maturing, release its claims, DEL record, SREM limbo, ZADD
 * tombstones, enqueue `conflicted`.
 * KEYS: record, pending, maturing, limbo, tombstones, outbox, outbox record, outbox:created.
 * ARGV: txid, eventId, payload, event, idempotencyKey, nowMs, outpointPrefix.
 */
const CONFLICT_LUA = `${LUA_PRELUDE}
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
release(KEYS[1], ARGV[7], ARGV[1])
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[4], ARGV[1])
redis.call('ZADD', KEYS[5], ARGV[6], ARGV[1])
enqueue(KEYS[6], KEYS[8], KEYS[7], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6])
return 1
`.trim()

/**
 * finishMaturing: release its claims, DEL record, ZREM maturing, ZADD tombstones (no event).
 * KEYS: record, maturing, tombstones. ARGV: txid, nowMs, outpointPrefix.
 */
const FINISH_MATURING_LUA = `${LUA_PRELUDE}
release(KEYS[1], ARGV[3], ARGV[1])
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
return 1
`.trim()

/**
 * endTracking: SREM pending, SREM limbo, release its claims, DEL record (no event).
 * KEYS: record, pending, limbo. ARGV: txid, outpointPrefix.
 */
const END_TRACKING_LUA = `${LUA_PRELUDE}
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[1])
release(KEYS[1], ARGV[2], ARGV[1])
redis.call('DEL', KEYS[1])
return 1
`.trim()

/**
 * clearTracking, atomically: for every txid in ARGV[3..] release its OWN claims (from its
 * record's `inputs`) and DEL its record (ARGV[1] = the record key prefix, ARGV[2] = the
 * outpoint key prefix), then DEL every tracking key. The bulk `outpoint:*` SCAN + DEL runs
 * BEFORE this script and the orphan sweep (SWEEP_ORPHAN_LUA) after it, outside it.
 * KEYS: maturing, pending, limbo, evaluated, mempool:current, mempool:postBlock, block:txids.
 */
const CLEAR_TRACKING_LUA = `${LUA_PRELUDE}
for i = 3, #ARGV do
  release(ARGV[1] .. ARGV[i], ARGV[2], ARGV[i])
  redis.call('DEL', ARGV[1] .. ARGV[i])
end
redis.call('DEL', unpack(KEYS))
return #ARGV - 2
`.trim()

/**
 * The orphan sweep's per-record step: release the record's OWN claims and DEL it ONLY while
 * the txid has no live membership (not pending, not in limbo, not in the maturing index) — a
 * record a concurrent recordSeen/promotion just created is left alone. Returns 1 when deleted.
 * KEYS: record, pending, limbo, maturing. ARGV: txid, outpointPrefix.
 */
const SWEEP_ORPHAN_LUA = `${LUA_PRELUDE}
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then return 0 end
if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then return 0 end
if redis.call('ZSCORE', KEYS[4], ARGV[1]) then return 0 end
release(KEYS[1], ARGV[2], ARGV[1])
redis.call('DEL', KEYS[1])
return 1
`.trim()

/**
 * outboxRetry: a NO-OP (0) when the hash is gone — a concurrent ack won the race and an
 * HSET here would create a partial hash that throws on every later read.
 * KEYS: outbox record, outbox. ARGV: attempts, lastError, nextAtMs, id.
 */
const OUTBOX_RETRY_LUA = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'attempts', ARGV[1], 'lastError', ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
return 1
`.trim()

/**
 * outboxDead, atomically: same no-op guard; HSET attempts/lastError; ZREM outbox + created;
 * ZADD dead; then cap the dead set at deadMax by dropping the OLDEST overflow entries with
 * their hashes (ZRANGE the overflow, DEL each hash, ZREMRANGEBYRANK).
 * KEYS: outbox record, outbox, outbox:dead, outbox:created.
 * ARGV: attempts, lastError, id, nowMs, deadMax, outbox hash key prefix.
 */
const OUTBOX_DEAD_LUA = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'attempts', ARGV[1], 'lastError', ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[3])
redis.call('ZREM', KEYS[4], ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[3])
local n = redis.call('ZCARD', KEYS[3])
local max = tonumber(ARGV[5])
if n > max then
  local overflow = redis.call('ZRANGE', KEYS[3], 0, n - max - 1)
  for _, dead in ipairs(overflow) do redis.call('DEL', ARGV[6] .. dead) end
  redis.call('ZREMRANGEBYRANK', KEYS[3], 0, n - max - 1)
end
return 1
`.trim()

/** MaturingRecord hash fields: height (string int), blockHash, matched (JSON), fired (JSON), hex, inputs (JSON). */
function recordToHash(rec: MaturingRecord): Record<string, string> {
  return {
    height: String(rec.height),
    blockHash: rec.blockHash,
    matched: JSON.stringify(rec.matched),
    fired: JSON.stringify(rec.fired),
    hex: rec.hex,
    inputs: JSON.stringify(rec.inputs),
  }
}

/** `outpoints` HASH fields for a tx's inputs, or [] when it spends nothing (coinbase). */
function outpointFields(inputs: Outpoint[]): string[] {
  return inputs.map(outpointField)
}

/** Outbox hash fields at enqueue time (lastError is only written by retry/dead). */
function outboxHash(event: WeirEvent, nowMs: number): Record<string, string> {
  return {
    payload: JSON.stringify(event),
    event: event.event,
    idempotencyKey: event.idempotencyKey,
    attempts: '0',
    createdAt: String(nowMs),
  }
}

function hashToRecord(txid: string, h: Record<string, string>): MaturingRecord {
  const height = h['height']
  const blockHash = h['blockHash']
  const matched = h['matched']
  const fired = h['fired']
  const hex = h['hex']
  const inputs = h['inputs']
  if (
    height === undefined ||
    blockHash === undefined ||
    matched === undefined ||
    fired === undefined ||
    hex === undefined
  ) {
    throw new Error(`[${CTX}] corrupt maturing record for ${txid}: missing fields (${Object.keys(h).join(',')})`)
  }
  return {
    txid,
    height: Number.parseInt(height, 10),
    blockHash,
    matched: JSON.parse(matched) as MaturingRecord['matched'],
    fired: JSON.parse(fired) as number[],
    hex,
    // A record written before outpoint tracking has no `inputs`: its outpoints were never
    // indexed, so there is nothing to look up or remove — not corruption.
    inputs: inputs === undefined ? [] : (JSON.parse(inputs) as Outpoint[]),
  }
}

export class Store {
  private readonly client: Client
  private readonly keys: Keys
  /** true once connect() resolved — giving up before that is a boot failure, not a crash */
  private ready = false
  /** set by quit(): the 'end' that follows is expected */
  private closing = false

  constructor(url: string, network: Network) {
    this.client = createClient({
      url,
      socket: {
        connectTimeout: 10_000,
        reconnectStrategy: (retries: number, cause: Error) => this.reconnectDecision(retries, cause),
      },
    })
    this.keys = keysFor(network)
    // node-redis emits socket errors as 'error' events (and reconnects); an
    // unhandled 'error' event would crash the process mid-reconnect. Log them —
    // commands issued while disconnected still reject, so nothing is swallowed.
    this.client.on('error', (err: unknown) => {
      log.error(CTX, `redis client error: ${describeError(err)}`)
    })
    // node-redis emits 'end' only after a deliberate quit()/disconnect(). Any other 'end'
    // means the client is closed for good with nobody awaiting a command → zombie → fatal.
    this.client.on('end', () => {
      if (this.closing) return
      fatal(CTX, new Error('redis connection closed unexpectedly'))
    })
  }

  /**
   * Bounded exponential backoff (250ms x2, capped 2s). On exhaustion: before connect()
   * resolved, return the Error so connect() rejects and boot fails fast with a clear
   * message; at runtime, node-redis would swallow that Error and go quiet with a closed
   * client (see module doc) — crash instead.
   */
  private reconnectDecision(retries: number, cause: Error): number | Error {
    if (retries < MAX_RECONNECTS) return Math.min(250 * 2 ** retries, 2000)
    const err = new Error(`[${CTX}] redis unreachable after ${retries} connection attempts: ${describeError(cause)}`)
    if (this.ready && !this.closing) fatal(CTX, err)
    return err
  }

  async connect(): Promise<void> {
    await this.client.connect()
    this.ready = true
  }

  async quit(): Promise<void> {
    this.closing = true
    await this.client.quit()
  }

  // ── watches ────────────────────────────────────────────────────────────────

  async isWatched(addr: string): Promise<boolean> {
    return this.client.sIsMember(this.keys.addresses, addr)
  }

  /** One SMISMEMBER round trip. Returns ALL watched addresses, input order — no early bail. */
  async watchedSubset(addrs: string[]): Promise<string[]> {
    if (addrs.length === 0) return []
    const flags = await this.client.smIsMember(this.keys.addresses, addrs)
    return addrs.filter((_, i) => flags[i])
  }

  /** SADD; with expiresAtMs also ZADD expiries — atomically (MULTI). Without it, any stale expiry is cleared. */
  async addWatch(addr: string, expiresAtMs?: number): Promise<void> {
    if (expiresAtMs !== undefined) {
      await this.client
        .multi()
        .sAdd(this.keys.addresses, addr)
        .zAdd(this.keys.expiries, { score: expiresAtMs, value: addr })
        .exec()
    } else {
      await this.client.multi().sAdd(this.keys.addresses, addr).zRem(this.keys.expiries, addr).exec()
    }
  }

  /** SREM (+ ZREM expiries). True when the address was actually watched. */
  async removeWatch(addr: string): Promise<boolean> {
    const replies = await this.client
      .multi()
      .sRem(this.keys.addresses, addr)
      .zRem(this.keys.expiries, addr)
      .exec()
    return Number(replies[0] ?? 0) > 0
  }

  async watchCount(): Promise<number> {
    return this.client.sCard(this.keys.addresses)
  }

  /** SSCAN passthrough, COUNT 1000. Cursor "0" means iteration complete. */
  async scanWatches(cursor: string): Promise<{ cursor: string; addresses: string[] }> {
    const res = await this.client.sScan(this.keys.addresses, Number.parseInt(cursor, 10), { COUNT: 1000 })
    return { cursor: String(res.cursor), addresses: res.members }
  }

  /** Watches whose expiry deadline is at or before nowMs. */
  async dueExpiries(nowMs: number): Promise<Array<{ address: string; expiresAtMs: number }>> {
    const members = await this.client.zRangeByScoreWithScores(this.keys.expiries, '-inf', nowMs)
    return members.map((m) => ({ address: m.value, expiresAtMs: m.score }))
  }

  async clearExpiry(addr: string): Promise<void> {
    await this.client.zRem(this.keys.expiries, addr)
  }

  /** ZSCORE expiries — expiresAt unix ms for a TTL'd watch, null when it has no expiry. */
  async getExpiry(addr: string): Promise<number | null> {
    return this.client.zScore(this.keys.expiries, addr)
  }

  // ── evaluated / pending ────────────────────────────────────────────────────

  async isEvaluated(txid: string): Promise<boolean> {
    return this.client.sIsMember(this.keys.evaluated, txid)
  }

  async markEvaluated(txid: string): Promise<void> {
    await this.client.sAdd(this.keys.evaluated, txid)
  }

  async pendingTxids(): Promise<string[]> {
    return this.client.sMembers(this.keys.pending)
  }

  // ── block / mempool bookkeeping ────────────────────────────────────────────

  /** Replace the blockTxids scratch set with the latest block's txids. */
  async setBlockTxids(txids: string[]): Promise<void> {
    const multi = this.client.multi().del(this.keys.blockTxids)
    if (txids.length > 0) multi.sAdd(this.keys.blockTxids, txids)
    await multi.exec()
  }

  /** SINTER pending ∩ blockTxids — watched pending txs mined in the latest block. */
  async pendingInBlock(): Promise<string[]> {
    return this.client.sInter([this.keys.pending, this.keys.blockTxids])
  }

  async replaceCurrentMempool(txids: string[]): Promise<void> {
    const multi = this.client.multi().del(this.keys.mempoolCurrent)
    if (txids.length > 0) multi.sAdd(this.keys.mempoolCurrent, txids)
    await multi.exec()
  }

  /**
   * SDIFF current − evaluated — txids needing evaluation this reparse. Deliberately NOT
   * minus a previous snapshot: `evaluated` alone decides (a dropped tx is un-evaluated
   * again, so a rebroadcast re-fires `seen` even though the mempool "did not change").
   */
  async newMempoolTxids(): Promise<string[]> {
    return this.client.sDiff([this.keys.mempoolCurrent, this.keys.evaluated])
  }

  /** DEL the transient reparse snapshot once evaluation is done. */
  async clearCurrentMempool(): Promise<void> {
    await this.client.del(this.keys.mempoolCurrent)
  }

  async replacePostBlockMempool(txids: string[]): Promise<void> {
    const multi = this.client.multi().del(this.keys.mempoolPostBlock)
    if (txids.length > 0) multi.sAdd(this.keys.mempoolPostBlock, txids)
    await multi.exec()
  }

  /** SDIFF pending − postBlock − blockTxids — pending txs that vanished without being mined. */
  async droppedPending(): Promise<string[]> {
    return this.client.sDiff([this.keys.pending, this.keys.mempoolPostBlock, this.keys.blockTxids])
  }

  /** SINTERSTORE evaluated = evaluated ∩ postBlock — forget txs no longer in the mempool. */
  async pruneEvaluated(): Promise<void> {
    await this.client.sInterStore(this.keys.evaluated, [this.keys.evaluated, this.keys.mempoolPostBlock])
  }

  // ── tip / block ring ───────────────────────────────────────────────────────

  async getTip(): Promise<Tip | null> {
    const h = await this.client.hGetAll(this.keys.tip)
    if (Object.keys(h).length === 0) return null
    const hash = h['hash']
    const height = h['height']
    if (hash === undefined || height === undefined) {
      throw new Error(`[${CTX}] corrupt tip hash: fields (${Object.keys(h).join(',')})`)
    }
    return { hash, height: Number.parseInt(height, 10) }
  }

  async setTip(tip: Tip): Promise<void> {
    await this.client.hSet(this.keys.tip, { hash: tip.hash, height: String(tip.height) })
  }

  async ringPut(height: number, hash: string): Promise<void> {
    await this.client.zAdd(this.keys.blocks, { score: height, value: hash })
  }

  async ringHashAt(height: number): Promise<string | null> {
    const hashes = await this.client.zRangeByScore(this.keys.blocks, height, height)
    return hashes[0] ?? null
  }

  /** Rewind the ring after a reorg: drop every entry strictly above `height` so the
   *  one-hash-per-height invariant holds when the replacement chain is recorded. */
  async ringRemoveAbove(height: number): Promise<void> {
    await this.client.zRemRangeByScore(this.keys.blocks, `(${height}`, '+inf')
  }

  /** Every ring entry, ascending by height (ZRANGE 0 -1 WITHSCORES). Height 0 included. */
  async ringAll(): Promise<Array<{ height: number; hash: string }>> {
    const members = await this.client.zRangeWithScores(this.keys.blocks, 0, -1)
    return members.map((m) => ({ height: m.score, hash: m.value }))
  }

  /** ZREMRANGEBYRANK — keep only the `keep` highest entries. */
  async ringPrune(keep: number): Promise<void> {
    await this.client.zRemRangeByRank(this.keys.blocks, 0, -(keep + 1))
  }

  // ── per-txid records (exist from seen-time; see module doc) ────────────────

  async readRecord(txid: string): Promise<MaturingRecord | null> {
    const h = await this.client.hGetAll(this.keys.maturingRecord(txid))
    if (Object.keys(h).length === 0) return null
    return hashToRecord(txid, h)
  }

  // ── transitions that ENQUEUE (state mutation + outbox, atomically) ─────────

  /** Append `HSET outbox:{id}` + `ZADD outbox nowMs id` + `ZADD outbox:created nowMs id` to a MULTI (see module doc). */
  private enqueue(multi: Multi, event: WeirEvent, nowMs: number): void {
    const id = makeEventId(nowMs)
    multi
      .hSet(this.keys.outboxRecord(id), outboxHash(event, nowMs))
      .zAdd(this.keys.outbox, { score: nowMs, value: id })
      .zAdd(this.keys.outboxCreated, { score: nowMs, value: id })
  }

  /**
   * A tx paying a watched address entered the mempool: HSET record (height 0), SADD
   * pending, SADD evaluated, claim its inputs (SADD txid into each prevout's SET), + enqueue
   * `seen` when `event` is non-null (null when seen events are disabled). One Lua script,
   * guarded (see RECORD_SEEN_LUA). `skipped` when a guard fired — a concurrent path already
   * tracks (or mined) the tx; `stale` (warned) when the evaluation is older than
   * MAX_EVALUATION_AGE_MS (`startedAtMs` = ZMQ receipt / RPC issue time) or when it started
   * at or before the txid's last exit (`retired` watermark: a duplicate evaluation that was
   * in flight when the tx was dropped/replaced): the txid stays un-evaluated and the next
   * reparse redoes it with a fresh view.
   */
  async recordSeen(rec: MaturingRecord, event: TxEvent | null, startedAtMs: number): Promise<RecordSeenOutcome> {
    const h = recordToHash(rec)
    const nowMs = Date.now()
    const id = event === null ? '' : makeEventId(nowMs)
    const reply = await this.client.eval(RECORD_SEEN_LUA, {
      keys: [
        this.keys.maturingRecord(rec.txid),
        this.keys.pending,
        this.keys.evaluated,
        this.keys.outbox,
        id === '' ? this.keys.outbox : this.keys.outboxRecord(id),
        this.keys.tombstones,
        this.keys.outboxCreated,
        this.keys.retired,
      ],
      arguments: [
        rec.txid,
        h['height'] as string,
        h['blockHash'] as string,
        h['matched'] as string,
        h['fired'] as string,
        h['hex'] as string,
        id,
        event === null ? '' : JSON.stringify(event),
        event === null ? '' : event.event,
        event === null ? '' : event.idempotencyKey,
        String(nowMs),
        String(startedAtMs),
        String(MAX_EVALUATION_AGE_MS),
        h['inputs'] as string,
        outpointFields(rec.inputs).join(','),
        this.keys.outpointPrefix,
      ],
    })
    const code = Number(reply)
    if (code === -1) {
      log.warn(
        CTX,
        `refused stale evaluation of ${rec.txid}: started ${nowMs - startedAtMs}ms ago (fence ${MAX_EVALUATION_AGE_MS}ms) — left un-evaluated for the next reparse`,
      )
      return 'stale'
    }
    if (code === -3) {
      log.warn(
        CTX,
        `refused evaluation of ${rec.txid} that predates its last drop/replacement (started ${nowMs - startedAtMs}ms ago) — left un-evaluated; a rebroadcast is picked up by the next reparse`,
      )
      return 'stale'
    }
    return code === 1 ? 'recorded' : 'skipped'
  }

  /** The outbox part of an EVAL: id + the ARGV tail every enqueuing script takes (eventId, payload, event, idempotencyKey). */
  private eventArgs(event: WeirEvent, nowMs: number): { id: string; args: [string, string, string, string] } {
    const id = makeEventId(nowMs)
    return { id, args: [id, JSON.stringify(event), event.event, event.idempotencyKey] }
  }

  /** A milestone was reached: HSET fired + enqueue `confirmed`, one MULTI. */
  async markFired(txid: string, fired: number[], event: TxEvent): Promise<void> {
    const multi = this.client.multi().hSet(this.keys.maturingRecord(txid), { fired: JSON.stringify(fired) })
    this.enqueue(multi, event, Date.now())
    await multi.exec()
  }

  /**
   * A pending tx left the mempool unmined (the tip-block dropped check): SREM pending, SREM
   * evaluated (a rebroadcast may legitimately re-fire `seen`), release its outpoints, DEL
   * record, + enqueue `dropped` (reason `evicted`) — one GUARDED Lua (DROP_PENDING_LUA):
   * resolves false, nothing written or enqueued, when the txid is no longer pending-unmined
   * (a mempool replacement or the block pipeline handled it since the caller's read).
   */
  async dropPending(txid: string, event: TxEvent): Promise<boolean> {
    const nowMs = Date.now()
    const { id, args } = this.eventArgs(event, nowMs)
    const reply = await this.client.eval(DROP_PENDING_LUA, {
      keys: [this.keys.maturingRecord(txid), this.keys.pending, this.keys.evaluated, this.keys.outbox, this.keys.outboxRecord(id), this.keys.outboxCreated, this.keys.retired],
      arguments: [txid, ...args, String(nowMs), this.keys.maturingRecord(''), this.keys.outpointPrefix],
    })
    return Number(reply) === 1
  }

  /**
   * A pending tx was REPLACED — another tx (mempool or block) spent one of its inputs. ONE
   * fenced, guarded Lua (REPLACE_PENDING_LUA): a no-op resolving false when the evaluation
   * is stale (warned) or when the txid is no longer pending-unmined (not pending, record
   * missing, or already mined) — the caller decided from a read that the block pipeline or
   * another caller has since overtaken, and nothing is written or enqueued. Otherwise the
   * dropPending mutation (+ the retirement watermark) + enqueue `dropped` with reason
   * `replaced` / `replacedBy`. No tombstone: the original may legitimately return if the
   * replacement is itself dropped. Outcome: `replaced` | `skipped` | `stale` — a caller that
   * saw `stale` must leave its own evaluation un-evaluated (the next reparse redoes it).
   * `spenderTxid` is the tx whose evaluation asks: the Lua refuses (`stale`) when THAT tx is
   * retired at an exit ≥ `startedAtMs` or tombstoned — an evaluation that predates its own
   * tx's exit may not act on anyone (it would replace the very tx that replaced it).
   */
  async replacePending(txid: string, event: TxEvent, startedAtMs: number, spenderTxid: string): Promise<ReplaceOutcome> {
    const nowMs = Date.now()
    const { id, args } = this.eventArgs(event, nowMs)
    const reply = await this.client.eval(REPLACE_PENDING_LUA, {
      keys: [
        this.keys.maturingRecord(txid),
        this.keys.pending,
        this.keys.evaluated,
        this.keys.outbox,
        this.keys.outboxRecord(id),
        this.keys.outboxCreated,
        this.keys.retired,
        this.keys.tombstones,
      ],
      arguments: [txid, ...args, String(nowMs), String(startedAtMs), String(MAX_EVALUATION_AGE_MS), this.keys.maturingRecord(''), this.keys.outpointPrefix, spenderTxid],
    })
    const code = Number(reply)
    if (code === -3) {
      log.warn(
        CTX,
        `refused replacement of ${txid} by ${spenderTxid}: the evaluation of ${spenderTxid} (started ${nowMs - startedAtMs}ms ago) predates its own drop/replacement, or it is tombstoned — nothing changed`,
      )
      return 'stale'
    }
    if (code === -1) {
      log.warn(
        CTX,
        `refused stale replacement of ${txid} by ${event.replacedBy ?? '?'}: evaluation started ${nowMs - startedAtMs}ms ago (fence ${MAX_EVALUATION_AGE_MS}ms) — nothing changed`,
      )
      return 'stale'
    }
    return code === 1 ? 'replaced' : 'skipped'
  }

  /**
   * A watch passed its deadline unpaid: SREM addresses, ZREM expiries, + enqueue `expired`,
   * one MULTI.
   */
  async expireWatch(addr: string, event: ExpiredEvent): Promise<void> {
    const multi = this.client.multi().sRem(this.keys.addresses, addr).zRem(this.keys.expiries, addr)
    this.enqueue(multi, event, Date.now())
    await multi.exec()
  }

  /**
   * Tracking ends without an event (the watch was removed mid-flight): SREM pending,
   * SREM limbo, release its outpoints, DEL record — one Lua (END_TRACKING_LUA). The
   * maturing index never holds such a tx.
   */
  async endTracking(txid: string): Promise<void> {
    await this.client.eval(END_TRACKING_LUA, {
      keys: [this.keys.maturingRecord(txid), this.keys.pending, this.keys.limbo],
      arguments: [txid, this.keys.outpointPrefix],
    })
  }

  // ── maturing (mined, below max milestone) ──────────────────────────────────

  /**
   * The ONE mined-tx promotion, atomically (MULTI): HSET record, ZADD maturing index,
   * SREM pending, SREM limbo, SADD evaluated, then CLAIM its inputs — one SADD into each
   * prevout's claimant SET (idempotent for a tx that claimed at seen-time; the input scan,
   * which runs before promotion, is what adjudicates the other claimants). Used for every
   * promotion branch (pending, limbo re-inclusion, never-seen) so a crash can never leave a
   * half-promoted tx.
   */
  async promoteToMaturing(rec: MaturingRecord): Promise<void> {
    const multi = this.client
      .multi()
      .hSet(this.keys.maturingRecord(rec.txid), recordToHash(rec))
      .zAdd(this.keys.maturing, { score: rec.height, value: rec.txid })
      .sRem(this.keys.pending, rec.txid)
      .sRem(this.keys.limbo, rec.txid)
      .sAdd(this.keys.evaluated, rec.txid)
    for (const o of rec.inputs) multi.sAdd(this.keys.outpointKey(o), rec.txid)
    await multi.exec()
  }

  /**
   * The ONE reorg demotion (limbo → pending), atomically (MULTI): HSET record back to
   * height 0 / blockHash '' / fired [], SADD pending, SADD evaluated, SREM limbo, + enqueue
   * `demoted`. Marking it evaluated is essential: the tip prune forgot the txid from
   * `evaluated` when it was mined, so without this the next mempool reparse would
   * re-evaluate it and emit a second `seen` under the same idempotency key.
   */
  async demoteToPending(rec: MaturingRecord, event: TxEvent): Promise<void> {
    const demoted: MaturingRecord = { ...rec, height: 0, blockHash: '', fired: [] }
    const multi = this.client
      .multi()
      .hSet(this.keys.maturingRecord(rec.txid), recordToHash(demoted))
      .sAdd(this.keys.pending, rec.txid)
      .sAdd(this.keys.evaluated, rec.txid)
      .sRem(this.keys.limbo, rec.txid)
    this.enqueue(multi, event, Date.now())
    await multi.exec()
  }

  /**
   * The terminal reorg outcome (limbo → gone): SREM pending, ZREM maturing, release its
   * outpoints, DEL record, SREM limbo, ZADD tombstones (tracking ENDED), + enqueue
   * `conflicted` — one Lua (CONFLICT_LUA).
   */
  async conflict(txid: string, event: TxEvent): Promise<void> {
    const nowMs = Date.now()
    const { id, args } = this.eventArgs(event, nowMs)
    await this.client.eval(CONFLICT_LUA, {
      keys: [
        this.keys.maturingRecord(txid),
        this.keys.pending,
        this.keys.maturing,
        this.keys.limbo,
        this.keys.tombstones,
        this.keys.outbox,
        this.keys.outboxRecord(id),
        this.keys.outboxCreated,
      ],
      arguments: [txid, ...args, String(nowMs), this.keys.outpointPrefix],
    })
  }

  /**
   * The final-milestone cleanup (tracking ENDED): DEL record, ZREM maturing, ZADD
   * tombstones score=nowMs, one Lua. The tombstone stops a stale evaluation (a mempool
   * RPC that read the tx before the block and returned after this cleanup) from
   * resurrecting the txid as a height-0 pending record — the next block would report
   * a false `dropped` for a payment that confirmed. `dropPending` deliberately does NOT
   * tombstone: a rebroadcast may legitimately re-fire `seen`. Releases its outpoints too —
   * one Lua (FINISH_MATURING_LUA).
   */
  async finishMaturing(txid: string, nowMs: number): Promise<void> {
    await this.client.eval(FINISH_MATURING_LUA, {
      keys: [this.keys.maturingRecord(txid), this.keys.maturing, this.keys.tombstones],
      arguments: [txid, String(nowMs), this.keys.outpointPrefix],
    })
  }

  /** ZSCORE tombstones — true when the txid's tracking ended within the tombstone TTL. */
  async isTombstoned(txid: string): Promise<boolean> {
    return (await this.client.zScore(this.keys.tombstones, txid)) !== null
  }

  /** ZREMRANGEBYSCORE tombstones -inf beforeMs — the tip-block prune. */
  async pruneTombstones(beforeMs: number): Promise<void> {
    await this.client.zRemRangeByScore(this.keys.tombstones, '-inf', beforeMs)
  }

  /** ZREMRANGEBYSCORE retired -inf beforeMs — the tip-block prune of the retirement watermark (same TTL as tombstones). */
  async pruneRetired(beforeMs: number): Promise<void> {
    await this.client.zRemRangeByScore(this.keys.retired, '-inf', beforeMs)
  }

  /** Every maturing txid with its inclusion height (ZSET score), ascending. */
  async maturingEntries(): Promise<Array<{ txid: string; height: number }>> {
    const members = await this.client.zRangeWithScores(this.keys.maturing, 0, -1)
    return members.map((m) => ({ txid: m.value, height: m.score }))
  }

  /** One ZREM from the maturing index ONLY — the records stay (limbo transition). No-op on empty. */
  async unindexMaturing(txids: string[]): Promise<void> {
    if (txids.length === 0) return
    await this.client.zRem(this.keys.maturing, txids)
  }

  /** DEL record + ZREM index, atomically (MULTI) — dangling-index cleanup (no tombstone; `finishMaturing` ends tracking). */
  async removeMaturing(txid: string): Promise<void> {
    await this.client
      .multi()
      .del(this.keys.maturingRecord(txid))
      .zRem(this.keys.maturing, txid)
      .exec()
  }

  // ── outpoints (prevout → claimant txids) ────────────────────────────────────

  /**
   * Which tracked txs claim these prevouts: one SMEMBERS per prevout, pipelined (MULTI) in
   * chunks of 1000. Returns only the prevouts with claimants, keyed by `{txid}:{vout}`.
   */
  async outpointOwners(outpoints: Outpoint[]): Promise<Map<string, string[]>> {
    const owners = new Map<string, string[]>()
    for (let i = 0; i < outpoints.length; i += OUTPOINT_CHUNK) {
      const chunk = outpoints.slice(i, i + OUTPOINT_CHUNK)
      const multi = this.client.multi()
      for (const o of chunk) multi.sMembers(this.keys.outpointKey(o))
      const replies = (await multi.exec()) as unknown[]
      chunk.forEach((o, j) => {
        const members = replies[j]
        if (Array.isArray(members) && members.length > 0) owners.set(outpointField(o), members.map(String))
      })
    }
    return owners
  }

  // ── limbo (reorg-displaced txids awaiting re-resolution) ───────────────────

  async addLimbo(txids: string[]): Promise<void> {
    if (txids.length === 0) return
    await this.client.sAdd(this.keys.limbo, txids)
  }

  async limboTxids(): Promise<string[]> {
    return this.client.sMembers(this.keys.limbo)
  }

  async removeLimbo(txid: string): Promise<void> {
    await this.client.sRem(this.keys.limbo, txid)
  }

  /**
   * Nuclear option for the prune-window guard: downtime exceeded what the pruned node
   * can replay, so all in-flight tx tracking is unrecoverable. The txids are collected from
   * limbo/pending/maturing, then ONE Lua (CLEAR_TRACKING_LUA) DELs their records AND every
   * tracking key (maturing index, pending, limbo, evaluated, mempool scratch, block txids)
   * atomically. Every `outpoint:*` claimant SET is SCANned and DELeted BEFORE that Lua (the
   * bulk pass) and the orphan `maturing:*` records are swept by `sweepOrphanRecords` AFTER
   * it, so a `recordSeen` landing after the Lua keeps its record, its memberships and its
   * claims; both Luas also release each record's OWN claims before deleting it, so a
   * `recordSeen` landing between the bulk pass and the Lua leaves no orphan claim behind.
   * PRESERVES the watch set, expiries, tip, ring AND the outbox (queued events
   * are still owed — `maturing:*` never matches `outbox*`). Returns the txids whose tracking
   * was lost so the caller can log them loudly.
   */
  async clearTracking(): Promise<string[]> {
    for await (const key of this.client.scanIterator({ MATCH: `${this.keys.outpointPrefix}*`, COUNT: 500 })) {
      await this.client.del(key)
    }
    const lost = [
      ...new Set<string>([
        ...(await this.limboTxids()),
        ...(await this.pendingTxids()),
        ...(await this.maturingEntries()).map((e) => e.txid),
      ]),
    ]
    await this.client.eval(CLEAR_TRACKING_LUA, {
      keys: [
        this.keys.maturing,
        this.keys.pending,
        this.keys.limbo,
        this.keys.evaluated,
        this.keys.mempoolCurrent,
        this.keys.mempoolPostBlock,
        this.keys.blockTxids,
      ],
      arguments: [this.keys.maturingRecord(''), this.keys.outpointPrefix, ...lost],
    })
    await this.sweepOrphanRecords()
    return lost
  }

  /**
   * Delete every `maturing:{txid}` record that belongs to NO live membership (a crash between
   * a record write and its index, or a record the clearTracking Lua did not know about).
   * SCAN the record pattern (the maturing ZSET key itself has no trailing colon, so the
   * pattern cannot match it); each candidate is deleted by SWEEP_ORPHAN_LUA, which re-checks
   * pending/limbo/maturing membership atomically and releases the record's own claims — a
   * record a concurrent recordSeen just created stays. Returns the number deleted.
   */
  async sweepOrphanRecords(): Promise<number> {
    const prefix = this.keys.maturingRecord('')
    let deleted = 0
    for await (const key of this.client.scanIterator({ MATCH: this.keys.maturingRecord('*'), COUNT: 500 })) {
      const txid = key.slice(prefix.length)
      const reply = await this.client.eval(SWEEP_ORPHAN_LUA, {
        keys: [key, this.keys.pending, this.keys.limbo, this.keys.maturing],
        arguments: [txid, this.keys.outpointPrefix],
      })
      deleted += Number(reply)
    }
    return deleted
  }

  // ── outbox (the drainer's surface; contracts under DESIGN "Outbox") ────────

  /** Ids due at or before nowMs, ascending by score (ZRANGEBYSCORE -inf now LIMIT 0 limit). */
  async outboxDue(nowMs: number, limit: number): Promise<string[]> {
    return this.client.zRangeByScore(this.keys.outbox, '-inf', nowMs, { LIMIT: { offset: 0, count: limit } })
  }

  /** The queued event + retry bookkeeping; null when the hash is gone (dangling id). */
  async outboxRead(id: string): Promise<OutboxRecord | null> {
    const h = await this.client.hGetAll(this.keys.outboxRecord(id))
    if (Object.keys(h).length === 0) return null
    const payload = h['payload']
    const createdAt = h['createdAt']
    if (payload === undefined || createdAt === undefined) {
      throw new Error(`[${CTX}] corrupt outbox record ${id}: missing fields (${Object.keys(h).join(',')})`)
    }
    const lastError = h['lastError']
    return {
      event: JSON.parse(payload) as WeirEvent,
      attempts: Number.parseInt(h['attempts'] ?? '0', 10),
      createdAt: Number.parseInt(createdAt, 10),
      lastError: lastError === undefined || lastError === '' ? null : lastError,
    }
  }

  /** Delivered (or dangling): DEL hash + ZREM queue + ZREM created, one MULTI. */
  async outboxAck(id: string): Promise<void> {
    await this.client
      .multi()
      .del(this.keys.outboxRecord(id))
      .zRem(this.keys.outbox, id)
      .zRem(this.keys.outboxCreated, id)
      .exec()
  }

  /**
   * Failed attempt, still within OUTBOX_MAX_AGE: HSET attempts/lastError + ZADD the next due
   * time — one Lua, a no-op when the hash is gone (see OUTBOX_RETRY_LUA). Resolves false then.
   */
  async outboxRetry(id: string, nextAtMs: number, attempts: number, lastError: string): Promise<boolean> {
    const reply = await this.client.eval(OUTBOX_RETRY_LUA, {
      keys: [this.keys.outboxRecord(id), this.keys.outbox],
      arguments: [String(attempts), lastError, String(nextAtMs), id],
    })
    return Number(reply) === 1
  }

  /**
   * Given up: HSET attempts/lastError, ZREM queue + created, ZADD dead (score = nowMs), and
   * the dead cap (oldest overflow dropped WITH their hashes) — one Lua, atomically; a no-op
   * when the hash is gone (see OUTBOX_DEAD_LUA). Resolves false then.
   */
  async outboxDead(id: string, nowMs: number, attempts: number, lastError: string, deadMax: number): Promise<boolean> {
    const reply = await this.client.eval(OUTBOX_DEAD_LUA, {
      keys: [this.keys.outboxRecord(id), this.keys.outbox, this.keys.outboxDead, this.keys.outboxCreated],
      arguments: [String(attempts), lastError, id, String(nowMs), String(deadMax), this.keys.outboxRecord('')],
    })
    return Number(reply) === 1
  }

  /** depth = ZCARD outbox, dead = ZCARD dead, oldestCreatedAt = the lowest score in `outbox:created` (exact). */
  async outboxStats(): Promise<{ depth: number; oldestCreatedAt: number | null; dead: number }> {
    const [depth, dead, oldest] = await Promise.all([
      this.client.zCard(this.keys.outbox),
      this.client.zCard(this.keys.outboxDead),
      this.client.zRangeWithScores(this.keys.outboxCreated, 0, 0),
    ])
    return { depth, oldestCreatedAt: oldest[0]?.score ?? null, dead }
  }

  // ── meta ───────────────────────────────────────────────────────────────────

  /** Parse INFO memory. maxmemory "0" (unlimited) → maxBytes null. */
  async memoryInfo(): Promise<{ usedBytes: number; maxBytes: number | null }> {
    const raw = await this.client.info('memory')
    const usedMatch = raw.match(/^used_memory:(\d+)/m)
    if (usedMatch === null || usedMatch[1] === undefined) {
      throw new Error(`[${CTX}] INFO memory did not contain used_memory`)
    }
    const maxMatch = raw.match(/^maxmemory:(\d+)/m)
    const maxRaw = maxMatch?.[1]
    return {
      usedBytes: Number.parseInt(usedMatch[1], 10),
      maxBytes: maxRaw === undefined || maxRaw === '0' ? null : Number.parseInt(maxRaw, 10),
    }
  }

  /**
   * CONFIG GET maxmemory-policy. Managed redis (Elasticache, Upstash, ...) blocks CONFIG
   * with a command-access error — the ONE permitted swallow in weir: log at warn, return
   * null. Any other failure (connection loss) is rethrown: it is not "blocked".
   */
  async maxmemoryPolicy(): Promise<string | null> {
    try {
      const res = await this.client.configGet('maxmemory-policy')
      return res['maxmemory-policy'] ?? null
    } catch (err) {
      if (!isConfigBlocked(describeError(err))) throw err
      log.warn(CTX, `CONFIG GET maxmemory-policy blocked (managed redis?): ${describeError(err)}`)
      return null
    }
  }
}
