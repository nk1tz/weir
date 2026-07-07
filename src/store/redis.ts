/**
 * Store — the daemon's redis-backed memory. Per DESIGN.md "src/store/redis.ts".
 *
 * Wraps a single redis@4 client. All keys come from `keysFor(network)`.
 *
 * Record lifecycle note (DESIGN.md blockPipeline step 4, authoritative resolution):
 * a `maturing:{txid}` HASH record exists from *seen-time* onward — `putRecord` is
 * called (height 0, blockHash '') when a tx enters `pending`, so hex + matched are
 * available for later `dropped`/mined transitions even though the tx can no longer
 * be fetched from a pruned node once evicted. The `maturing` ZSET, by contrast,
 * only ever indexes *mined* txs: `addMaturing` = putRecord + ZADD, `removeMaturing`
 * = deleteRecord + ZREM.
 */

import { createClient } from 'redis'
import { MaturingRecord, Network, Tip } from '../lib/types'
import { log } from '../lib/log'
import { Keys, keysFor } from './keys'

const CTX = 'store'

type Client = ReturnType<typeof createClient>

/** AggregateError (e.g. dual-stack ECONNREFUSED) has an empty .message — surface the inner ones. */
function describeErr(err: unknown): string {
  if (err instanceof AggregateError) {
    const inner = err.errors.map((e) => (e instanceof Error ? e.message : String(e))).join('; ')
    return err.message ? `${err.message}: ${inner}` : inner || 'AggregateError (no detail)'
  }
  if (err instanceof Error) return err.message || err.name
  return String(err)
}

/** MaturingRecord hash fields: height (string int), blockHash, matched (JSON), fired (JSON), hex. */
function recordToHash(rec: MaturingRecord): Record<string, string> {
  return {
    height: String(rec.height),
    blockHash: rec.blockHash,
    matched: JSON.stringify(rec.matched),
    fired: JSON.stringify(rec.fired),
    hex: rec.hex,
  }
}

function hashToRecord(txid: string, h: Record<string, string>): MaturingRecord {
  const height = h['height']
  const blockHash = h['blockHash']
  const matched = h['matched']
  const fired = h['fired']
  const hex = h['hex']
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
  }
}

export class Store {
  private readonly client: Client
  private readonly keys: Keys

  constructor(url: string, network: Network) {
    this.client = createClient({
      url,
      socket: {
        connectTimeout: 10_000,
        // Bounded retries: boot fails FAST on a bad REDIS_URL instead of hanging on
        // the default infinite reconnect loop; at runtime, exhausting retries closes
        // the client, in-flight commands reject, and the engine crashes per the
        // log-and-crash policy (docker restarts us; boot reconciliation heals).
        reconnectStrategy: (retries: number) =>
          retries >= 5
            ? new Error(`[${CTX}] redis unreachable after ${retries} connection attempts`)
            : Math.min(250 * 2 ** retries, 2000),
      },
    })
    this.keys = keysFor(network)
    // node-redis emits socket errors as 'error' events (and reconnects); an
    // unhandled 'error' event would crash the process mid-reconnect. Log them —
    // commands issued while disconnected still reject, so nothing is swallowed.
    this.client.on('error', (err: unknown) => {
      log.error(CTX, `redis client error: ${describeErr(err)}`)
    })
  }

  async connect(): Promise<void> {
    await this.client.connect()
  }

  async quit(): Promise<void> {
    await this.client.quit()
  }

  // ── watches ────────────────────────────────────────────────────────────────

  async isWatched(addr: string): Promise<boolean> {
    return this.client.sIsMember(this.keys.addresses, addr)
  }

  /**
   * Pipelined SISMEMBER for every address (redis@4 auto-pipelines concurrent
   * commands). Returns ALL watched addresses — no early bail.
   */
  async watchedSubset(addrs: string[]): Promise<string[]> {
    if (addrs.length === 0) return []
    const flags = await Promise.all(addrs.map((a) => this.client.sIsMember(this.keys.addresses, a)))
    const watched: string[] = []
    for (let i = 0; i < addrs.length; i++) {
      const addr = addrs[i]
      if (flags[i] && addr !== undefined) watched.push(addr)
    }
    return watched
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
    return { cursor: String(res.cursor), addresses: res.members.map((m) => m.toString()) }
  }

  /** Watches whose expiry deadline is at or before nowMs. */
  async dueExpiries(nowMs: number): Promise<Array<{ address: string; expiresAtMs: number }>> {
    const members = await this.client.zRangeByScoreWithScores(this.keys.expiries, '-inf', nowMs)
    return members.map((m) => ({ address: m.value.toString(), expiresAtMs: m.score }))
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

  async unmarkEvaluated(txid: string): Promise<void> {
    await this.client.sRem(this.keys.evaluated, txid)
  }

  async addPending(txid: string): Promise<void> {
    await this.client.sAdd(this.keys.pending, txid)
  }

  async removePending(txid: string): Promise<void> {
    await this.client.sRem(this.keys.pending, txid)
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

  /** SDIFF current − previous − evaluated — txids needing evaluation this reparse. */
  async newMempoolTxids(): Promise<string[]> {
    return this.client.sDiff([this.keys.mempoolCurrent, this.keys.mempoolPrevious, this.keys.evaluated])
  }

  /** current → previous. SDIFFSTORE with a single source key copies (or clears when empty). */
  async rotateMempool(): Promise<void> {
    await this.client
      .multi()
      .sDiffStore(this.keys.mempoolPrevious, [this.keys.mempoolCurrent])
      .del(this.keys.mempoolCurrent)
      .exec()
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
    const first = hashes[0]
    return first === undefined ? null : first.toString()
  }

  /** Rewind the ring after a reorg: drop every entry strictly above `height` so the
   *  one-hash-per-height invariant holds when the replacement chain is recorded. */
  async ringRemoveAbove(height: number): Promise<void> {
    await this.client.zRemRangeByScore(this.keys.blocks, `(${height}`, '+inf')
  }

  /** Ring entries strictly above `height`, ascending. */
  async ringAbove(height: number): Promise<Array<{ height: number; hash: string }>> {
    const members = await this.client.zRangeByScoreWithScores(this.keys.blocks, `(${height}`, '+inf')
    return members.map((m) => ({ height: m.score, hash: m.value.toString() }))
  }

  /** ZREMRANGEBYRANK — keep only the `keep` highest entries. */
  async ringPrune(keep: number): Promise<void> {
    await this.client.zRemRangeByRank(this.keys.blocks, 0, -(keep + 1))
  }

  // ── per-txid records (exist from seen-time; see module doc) ────────────────

  /** HSET the record hash only — no ZSET index. Used when a tx enters `pending`. */
  async putRecord(rec: MaturingRecord): Promise<void> {
    await this.client.hSet(this.keys.maturingRecord(rec.txid), recordToHash(rec))
  }

  async readRecord(txid: string): Promise<MaturingRecord | null> {
    const h = await this.client.hGetAll(this.keys.maturingRecord(txid))
    if (Object.keys(h).length === 0) return null
    return hashToRecord(txid, h)
  }

  async deleteRecord(txid: string): Promise<void> {
    await this.client.del(this.keys.maturingRecord(txid))
  }

  // ── maturing (mined, below max milestone) ──────────────────────────────────

  /** putRecord + ZADD maturing index, atomically (MULTI). */
  async addMaturing(rec: MaturingRecord): Promise<void> {
    await this.client
      .multi()
      .hSet(this.keys.maturingRecord(rec.txid), recordToHash(rec))
      .zAdd(this.keys.maturing, { score: rec.height, value: rec.txid })
      .exec()
  }

  async getMaturing(txid: string): Promise<MaturingRecord | null> {
    return this.readRecord(txid)
  }

  /** Every maturing txid with its inclusion height (ZSET score), ascending. */
  async maturingEntries(): Promise<Array<{ txid: string; height: number }>> {
    const members = await this.client.zRangeWithScores(this.keys.maturing, 0, -1)
    return members.map((m) => ({ txid: m.value.toString(), height: m.score }))
  }

  /** ZREM from the maturing index ONLY — the per-txid record stays (limbo transition). */
  async unindexMaturing(txid: string): Promise<void> {
    await this.client.zRem(this.keys.maturing, txid)
  }

  // ── limbo (reorg-displaced txids awaiting re-resolution) ───────────────────

  async addLimbo(txids: string[]): Promise<void> {
    if (txids.length === 0) return
    await this.client.sAdd(this.keys.limbo, txids)
  }

  async limboTxids(): Promise<string[]> {
    const members = await this.client.sMembers(this.keys.limbo)
    return members.map((m) => m.toString())
  }

  async removeLimbo(txid: string): Promise<void> {
    await this.client.sRem(this.keys.limbo, txid)
  }

  /**
   * Nuclear option for the prune-window guard: downtime exceeded what the pruned node
   * can replay, so all in-flight tx tracking is unrecoverable. Clears every tracking
   * structure (maturing index + records, pending, limbo, evaluated, mempool scratch)
   * while PRESERVING the watch set, expiries, tip and ring. Returns the txids whose
   * tracking was lost so the caller can log them loudly.
   */
  async clearTracking(): Promise<string[]> {
    const lost = new Set<string>([
      ...(await this.limboTxids()),
      ...(await this.pendingTxids()),
      ...(await this.maturingEntries()).map((e) => e.txid),
    ])
    // Per-txid record hashes: scan the maturing:{txid} pattern (the maturing ZSET key
    // itself has no trailing colon, so the pattern cannot match it).
    const pattern = this.keys.maturingRecord('*')
    for await (const key of this.client.scanIterator({ MATCH: pattern, COUNT: 500 })) {
      await this.client.del(key.toString())
    }
    await this.client.del([
      this.keys.maturing,
      this.keys.pending,
      this.keys.limbo,
      this.keys.evaluated,
      this.keys.mempoolPrevious,
      this.keys.mempoolCurrent,
      this.keys.mempoolPostBlock,
      this.keys.blockTxids,
    ])
    return [...lost]
  }

  async setMaturingFired(txid: string, fired: number[]): Promise<void> {
    await this.client.hSet(this.keys.maturingRecord(txid), { fired: JSON.stringify(fired) })
  }

  /** Re-inclusion after a reorg: new ZSET score + new height/blockHash + fired reset, atomically (MULTI). */
  async moveMaturing(txid: string, newHeight: number, newBlockHash: string): Promise<void> {
    await this.client
      .multi()
      .zAdd(this.keys.maturing, { score: newHeight, value: txid })
      .hSet(this.keys.maturingRecord(txid), {
        height: String(newHeight),
        blockHash: newBlockHash,
        fired: '[]',
      })
      .exec()
  }

  /** deleteRecord + ZREM index, atomically (MULTI). */
  async removeMaturing(txid: string): Promise<void> {
    await this.client
      .multi()
      .del(this.keys.maturingRecord(txid))
      .zRem(this.keys.maturing, txid)
      .exec()
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
   * CONFIG GET maxmemory-policy. Managed redis (Elasticache, Upstash, ...) blocks
   * CONFIG — the ONE permitted swallow in weir: log at warn, return null.
   */
  async maxmemoryPolicy(): Promise<string | null> {
    try {
      const res = await this.client.configGet('maxmemory-policy')
      return res['maxmemory-policy'] ?? null
    } catch (err) {
      log.warn(
        CTX,
        `CONFIG GET maxmemory-policy blocked (managed redis?): ${err instanceof Error ? err.message : String(err)}`,
      )
      return null
    }
  }
}
