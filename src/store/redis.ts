/**
 * Store — the daemon's redis-backed memory, one redis@4 client, keys via `keysFor(network)`.
 * Spec: docs/DESIGN.md "src/store/redis.ts" (and "Redis schema", "Single writer").
 *
 * INVARIANTS
 * - ONE WRITER. Every engine state change runs inside the engine queue (src/engine/queue.ts),
 *   so no two transitions ever interleave. A transition therefore reads what it needs, decides
 *   in memory, and writes ONE plain MULTI. There are no conditional scripts, no guards that
 *   re-check live state, no fences, tombstones or watermarks: nothing can have changed
 *   between the read and the write.
 * - A `maturing:{txid}` record exists from SEEN-time onward (height 0 / blockHash ''); the
 *   `maturing` ZSET only ever indexes MINED txs.
 * - OUTBOX: every transition that produces an event enqueues it in the SAME MULTI as its
 *   state mutation — `HSET outbox:{id}` + `ZADD outbox now id` + `ZADD outbox:created now id`
 *   (`enqueue`). Either both persist or neither does. Nothing here awaits the network;
 *   delivery is the drainer's job (src/delivery/outbox.ts).
 * - Event ids are monotonic (`makeEventId`): ms prefix + per-ms sequence + random suffix,
 *   so redis' tie order for equal ZSET scores (by member) IS enqueue order.
 * - THE RING HOLDS EXACTLY ONE HASH PER HEIGHT: every put (`tipOps`) removes the entry at that
 *   height before adding, and rewinds remove every height above the fork. So "already in the
 *   ring" means exactly "this hash at this height" — a true duplicate — and a different hash
 *   at a known height is a replacement the connectivity path handles.
 * - OUTPOINTS: `outpoint:{txid}:{vout}` is a SET of claimant txids — every pending or
 *   maturing spender of that prevout. CLAIM = SADD inside the transition that creates or
 *   promotes a record; RELEASE = SREM of ONLY the releaser's own txid inside every transition
 *   that deletes a record (the caller passes the record it read; `inputs` come from it).
 * - Single redis instance: multi-key MULTI without hash tags — not Redis Cluster.
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
import { metrics } from '../lib/metrics'
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

/** What the drainer reads back for one queued event (`outboxRead`). */
export interface OutboxRecord {
  event: WeirEvent
  attempts: number
  /** unix ms when the event was enqueued — the dead-letter age clock */
  createdAt: number
  lastError: string | null
}

/** A record (or the part of it a deletion needs): its txid and the prevouts it claims. */
export type Claimant = Pick<MaturingRecord, 'txid' | 'inputs'>

/** A `pending → gone` step: the record that leaves and the `dropped` event it carries. */
export interface Drop {
  rec: Claimant
  event: TxEvent
}

/**
 * Everything ONE mempool evaluation writes (`applyEvaluation`, one MULTI): the tx is marked
 * evaluated; every pending claimant it replaced leaves (`dropped`, reason `replaced`); and,
 * when it pays a watch, its own record enters `pending` with its claims and the `seen`
 * event (null when seen events are disabled).
 */
export interface EvaluationWrites {
  txid: string
  dropped: Drop[]
  seen: { rec: MaturingRecord; event: TxEvent | null } | null
}

/**
 * Everything ONE connected block writes (`applyBlock`, one MULTI). The block pipeline reads
 * first, computes every transition, then hands them here; the tip and ring move in the same
 * transaction, so a crash before exec leaves NOTHING of the block behind and boot
 * reconciliation replays it whole.
 */
export interface BlockWrites {
  /** mined txs paying a watch (pending / limbo re-inclusion / never-seen) → maturing at this block, claims SADDed */
  promoted: MaturingRecord[]
  /** milestone reached: HSET fired + enqueue `confirmed` */
  fired: Array<{ txid: string; fired: number[]; event: TxEvent }>
  /** tracking ended at the max milestone: release claims, DEL record, ZREM maturing (no event) */
  finished: Claimant[]
  /** pending txs an input of which this block spent: pending → gone + `dropped` (reason `replaced`) */
  dropped: Drop[]
  /** limbo txs an input of which this block spent: limbo → gone + proven `conflicted` */
  conflicted: Array<{ rec: Claimant; event: TxEvent }>
  /** tracked txs mined but no longer matching any watch: gone, no event */
  ended: Claimant[]
  /** maturing index entries with no record (corruption cleanup): ZREM only */
  unindexed: string[]
  tip: Tip
  /** ring entries to keep (cfg.ringSize) */
  ringKeep: number
}

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

  /** SADD; with expiresAtMs also ZADD expiries — one MULTI. Without it, any stale expiry is cleared. */
  async addWatch(addr: string, expiresAtMs?: number): Promise<void> {
    const multi = this.client.multi().sAdd(this.keys.addresses, addr)
    if (expiresAtMs !== undefined) multi.zAdd(this.keys.expiries, { score: expiresAtMs, value: addr })
    else multi.zRem(this.keys.expiries, addr)
    await multi.exec()
  }

  /** SREM (+ ZREM expiries), one MULTI. True when the address was actually watched. */
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

  /** ZSCORE expiries — expiresAt unix ms for a TTL'd watch, null when it has no expiry. */
  async getExpiry(addr: string): Promise<number | null> {
    return this.client.zScore(this.keys.expiries, addr)
  }

  /** A watch's lifetime ended: SREM addresses, ZREM expiries + enqueue `expired`, one MULTI. */
  async expireWatch(addr: string, event: ExpiredEvent): Promise<void> {
    const multi = this.client.multi().sRem(this.keys.addresses, addr).zRem(this.keys.expiries, addr)
    this.enqueue(multi, event, Date.now())
    await multi.exec()
    this.noteEnqueued(event)
  }

  // ── tracking reads ─────────────────────────────────────────────────────────

  async isEvaluated(txid: string): Promise<boolean> {
    return this.client.sIsMember(this.keys.evaluated, txid)
  }

  async evaluatedTxids(): Promise<string[]> {
    return this.client.sMembers(this.keys.evaluated)
  }

  async pendingTxids(): Promise<string[]> {
    return this.client.sMembers(this.keys.pending)
  }

  async limboTxids(): Promise<string[]> {
    return this.client.sMembers(this.keys.limbo)
  }

  /** Every maturing txid with its inclusion height (ZSET score), ascending. */
  async maturingEntries(): Promise<Array<{ txid: string; height: number }>> {
    const members = await this.client.zRangeWithScores(this.keys.maturing, 0, -1)
    return members.map((m) => ({ txid: m.value, height: m.score }))
  }

  /** The per-txid record (exists from seen-time); null when absent. */
  async readRecord(txid: string): Promise<MaturingRecord | null> {
    const h = await this.client.hGetAll(this.keys.maturingRecord(txid))
    if (Object.keys(h).length === 0) return null
    return hashToRecord(txid, h)
  }

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

  /** HSET tip + ZADD the block into the ring, one MULTI (boot first-run initialisation). */
  async setTip(tip: Tip): Promise<void> {
    const multi = this.client.multi()
    this.tipOps(multi, tip)
    await multi.exec()
  }

  /** Boot's ring normalisation: DEL blocks + ZADD every entry, one MULTI (the entries come from the tip's header ancestry). */
  async rebuildRing(entries: Array<{ height: number; hash: string }>): Promise<void> {
    const multi = this.client.multi().del(this.keys.blocks)
    if (entries.length > 0) multi.zAdd(this.keys.blocks, entries.map((e) => ({ score: e.height, value: e.hash })))
    await multi.exec()
  }

  async ringHashAt(height: number): Promise<string | null> {
    const hashes = await this.client.zRangeByScore(this.keys.blocks, height, height)
    return hashes[0] ?? null
  }

  /** Every ring entry, ascending by height (ZRANGE 0 -1 WITHSCORES). Height 0 included. */
  async ringAll(): Promise<Array<{ height: number; hash: string }>> {
    const members = await this.client.zRangeWithScores(this.keys.blocks, 0, -1)
    return members.map((m) => ({ height: m.score, hash: m.value }))
  }

  // ── MULTI fragments (private) ──────────────────────────────────────────────

  /** Append `HSET outbox:{id}` + `ZADD outbox nowMs id` + `ZADD outbox:created nowMs id` to a MULTI. */
  private enqueue(multi: Multi, event: WeirEvent, nowMs: number): void {
    const id = makeEventId(nowMs)
    multi
      .hSet(this.keys.outboxRecord(id), outboxHash(event, nowMs))
      .zAdd(this.keys.outbox, { score: nowMs, value: id })
      .zAdd(this.keys.outboxCreated, { score: nowMs, value: id })
  }

  /** `weir_events_enqueued_total{event}` — bumped by every transition AFTER its MULTI exec'd. */
  private noteEnqueued(event: WeirEvent): void {
    metrics.counters.inc('weir_events_enqueued_total', { event: event.event })
  }

  /** HSET tip + the ring put — ONE hash per height: whatever sat at that height is removed first. */
  private tipOps(multi: Multi, tip: Tip): void {
    multi
      .hSet(this.keys.tip, { hash: tip.hash, height: String(tip.height) })
      .zRemRangeByScore(this.keys.blocks, tip.height, tip.height)
      .zAdd(this.keys.blocks, { score: tip.height, value: tip.hash })
  }

  /** Release the record's own claims (SREM only its txid from each prevout SET) and DEL the record. */
  private forgetOps(multi: Multi, rec: Claimant): void {
    for (const o of rec.inputs) multi.sRem(this.keys.outpointKey(o), rec.txid)
    multi.del(this.keys.maturingRecord(rec.txid))
  }

  /** pending → gone: SREM pending, SREM evaluated (a rebroadcast may re-fire `seen`), forget, + `dropped`. */
  private dropOps(multi: Multi, d: Drop, nowMs: number): void {
    multi.sRem(this.keys.pending, d.rec.txid).sRem(this.keys.evaluated, d.rec.txid)
    this.forgetOps(multi, d.rec)
    this.enqueue(multi, d.event, nowMs)
  }

  /** limbo → gone (terminal): SREM pending, ZREM maturing, SREM limbo, forget, + `conflicted`. */
  private conflictOps(multi: Multi, rec: Claimant, event: TxEvent, nowMs: number): void {
    multi.sRem(this.keys.pending, rec.txid).zRem(this.keys.maturing, rec.txid).sRem(this.keys.limbo, rec.txid)
    this.forgetOps(multi, rec)
    this.enqueue(multi, event, nowMs)
  }

  /** The mined-tx promotion: HSET record, ZADD maturing, SREM pending, SREM limbo, SADD its claims. */
  private promoteOps(multi: Multi, rec: MaturingRecord): void {
    multi
      .hSet(this.keys.maturingRecord(rec.txid), recordToHash(rec))
      .zAdd(this.keys.maturing, { score: rec.height, value: rec.txid })
      .sRem(this.keys.pending, rec.txid)
      .sRem(this.keys.limbo, rec.txid)
    for (const o of rec.inputs) multi.sAdd(this.keys.outpointKey(o), rec.txid)
  }

  // ── transitions (each ONE MULTI) ───────────────────────────────────────────

  /**
   * One mempool evaluation, one MULTI: SADD evaluated; each replaced claimant leaves
   * (`dropOps`); when the tx pays a watch, HSET its record (height 0), SADD pending, SADD
   * its claims, + enqueue `seen` when the event is non-null.
   */
  async applyEvaluation(w: EvaluationWrites): Promise<void> {
    const nowMs = Date.now()
    const multi = this.client.multi().sAdd(this.keys.evaluated, w.txid)
    for (const d of w.dropped) this.dropOps(multi, d, nowMs)
    if (w.seen !== null) {
      const rec = w.seen.rec
      multi.hSet(this.keys.maturingRecord(rec.txid), recordToHash(rec)).sAdd(this.keys.pending, rec.txid)
      for (const o of rec.inputs) multi.sAdd(this.keys.outpointKey(o), rec.txid)
      if (w.seen.event !== null) this.enqueue(multi, w.seen.event, nowMs)
    }
    await multi.exec()
    for (const d of w.dropped) this.noteEnqueued(d.event)
    if (w.seen?.event) this.noteEnqueued(w.seen.event)
  }

  /**
   * One connected block, one MULTI, in this order: promotions → fired milestones →
   * finished (release + DEL + ZREM) → replaced pending (drop) → proven conflicts → ended
   * tracking → dangling index entries → tip + ring put + ring prune. A tx promoted and
   * finished in the same block nets out to gone with its `confirmed` events enqueued.
   */
  async applyBlock(w: BlockWrites): Promise<void> {
    const nowMs = Date.now()
    const multi = this.client.multi()
    for (const rec of w.promoted) this.promoteOps(multi, rec)
    for (const f of w.fired) {
      multi.hSet(this.keys.maturingRecord(f.txid), { fired: JSON.stringify(f.fired) })
      this.enqueue(multi, f.event, nowMs)
    }
    for (const rec of w.finished) {
      multi.zRem(this.keys.maturing, rec.txid)
      this.forgetOps(multi, rec)
    }
    for (const d of w.dropped) this.dropOps(multi, d, nowMs)
    for (const c of w.conflicted) this.conflictOps(multi, c.rec, c.event, nowMs)
    for (const rec of w.ended) {
      multi.sRem(this.keys.pending, rec.txid).sRem(this.keys.limbo, rec.txid)
      this.forgetOps(multi, rec)
    }
    if (w.unindexed.length > 0) multi.zRem(this.keys.maturing, w.unindexed)
    this.tipOps(multi, w.tip)
    multi.zRemRangeByRank(this.keys.blocks, 0, -(w.ringKeep + 1))
    await multi.exec()
    for (const f of w.fired) this.noteEnqueued(f.event)
    for (const d of w.dropped) this.noteEnqueued(d.event)
    for (const c of w.conflicted) this.noteEnqueued(c.event)
  }

  /**
   * The reorg rewind, one MULTI: every displaced maturing txid → SADD limbo + ZREM maturing
   * (records kept), ring truncated strictly above the fork point, tip = the fork point.
   */
  async rewind(ancestor: Tip, displaced: string[]): Promise<void> {
    const multi = this.client.multi()
    if (displaced.length > 0) multi.sAdd(this.keys.limbo, displaced).zRem(this.keys.maturing, displaced)
    multi.zRemRangeByScore(this.keys.blocks, `(${ancestor.height}`, '+inf')
    this.tipOps(multi, ancestor)
    await multi.exec()
  }

  /** A pending tx left the mempool unmined (the tip-block eviction check): `dropOps`, one MULTI. */
  async dropPending(d: Drop): Promise<void> {
    const multi = this.client.multi()
    this.dropOps(multi, d, Date.now())
    await multi.exec()
    this.noteEnqueued(d.event)
  }

  /**
   * The reorg demotion (limbo → pending), one MULTI: HSET record back to height 0 /
   * blockHash '' / fired [], SADD pending, SADD evaluated (the tx is in the mempool and
   * evaluated — the reparse must not fetch it again), SREM limbo, + enqueue `demoted`.
   * Claims are kept: the tx stays tracked.
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
    this.noteEnqueued(event)
  }

  /** The terminal reorg outcome by elimination (limbo → gone): `conflictOps`, one MULTI. */
  async conflict(rec: Claimant, event: TxEvent): Promise<void> {
    const multi = this.client.multi()
    this.conflictOps(multi, rec, event, Date.now())
    await multi.exec()
    this.noteEnqueued(event)
  }

  /** A limbo entry whose record is gone (corruption cleanup): SREM limbo, one MULTI. */
  async removeLimbo(txid: string): Promise<void> {
    await this.client.multi().sRem(this.keys.limbo, txid).exec()
  }

  /** Forget txids from the reparse dedupe list (tip blocks: those no longer in the mempool), one MULTI. No-op on []. */
  async forgetEvaluated(txids: string[]): Promise<void> {
    if (txids.length === 0) return
    await this.client.multi().sRem(this.keys.evaluated, txids).exec()
  }

  /**
   * The prune-window reset: downtime exceeded what the pruned node can replay, so every
   * in-flight tracking is unrecoverable. SCAN every record and claimant SET (reads), then ONE
   * MULTI DELs them all plus the tracking sets (maturing, pending, limbo, evaluated) and jumps
   * the tip/ring to `tip`. PRESERVES watches, expiries, the ring below and the outbox (queued
   * events are still owed). Returns the txids whose tracking was lost, for the caller's log.
   */
  async resetTracking(tip: Tip): Promise<string[]> {
    const doomed: string[] = []
    for await (const key of this.client.scanIterator({ MATCH: `${this.keys.outpointPrefix}*`, COUNT: 500 })) doomed.push(key)
    for await (const key of this.client.scanIterator({ MATCH: this.keys.maturingRecord('*'), COUNT: 500 })) doomed.push(key)
    const lost = [
      ...new Set<string>([
        ...(await this.limboTxids()),
        ...(await this.pendingTxids()),
        ...(await this.maturingEntries()).map((e) => e.txid),
      ]),
    ]
    const multi = this.client.multi()
    if (doomed.length > 0) multi.del(doomed)
    multi.del([this.keys.maturing, this.keys.pending, this.keys.limbo, this.keys.evaluated])
    this.tipOps(multi, tip)
    await multi.exec()
    return lost
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

  /** Failed attempt, still within OUTBOX_MAX_AGE: HSET attempts/lastError + ZADD the next due time, one MULTI. */
  async outboxRetry(id: string, nextAtMs: number, attempts: number, lastError: string): Promise<void> {
    await this.client
      .multi()
      .hSet(this.keys.outboxRecord(id), { attempts: String(attempts), lastError })
      .zAdd(this.keys.outbox, { score: nextAtMs, value: id })
      .exec()
  }

  /**
   * Given up: HSET attempts/lastError, ZREM queue + created, ZADD dead (score = nowMs), and
   * the dead cap — the oldest entries beyond `deadMax` (read with ZCARD + ZRANGE first) are
   * dropped WITH their hashes — one MULTI.
   */
  async outboxDead(id: string, nowMs: number, attempts: number, lastError: string, deadMax: number): Promise<void> {
    const overflow = (await this.client.zCard(this.keys.outboxDead)) + 1 - deadMax
    const oldest = overflow > 0 ? await this.client.zRange(this.keys.outboxDead, 0, overflow - 1) : []
    const multi = this.client
      .multi()
      .hSet(this.keys.outboxRecord(id), { attempts: String(attempts), lastError })
      .zRem(this.keys.outbox, id)
      .zRem(this.keys.outboxCreated, id)
      .zAdd(this.keys.outboxDead, { score: nowMs, value: id })
    for (const dead of oldest) multi.del(this.keys.outboxRecord(dead)).zRem(this.keys.outboxDead, dead)
    await multi.exec()
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
