import type { Network, Outpoint } from '../lib/types'

/**
 * The complete redis footprint. `addresses` (and `expiries` alongside it) is the
 * PUBLIC input API — everything else is the daemon's memory: read if curious, never write.
 */
export interface Keys {
  /** SET of watched address strings — the public contract */
  addresses: string
  /** ZSET address -> expiresAt unix ms, for TTL'd watches */
  expiries: string

  /** HASH {hash, height} — current chain tip as weir last saw it */
  tip: string
  /** ZSET blockHash scored by height — recent-block ring for reorg detection */
  blocks: string
  /** ZSET txid scored by inclusion height — the maturing sweep index */
  maturing: string
  /** HASH per maturing tx (height, blockHash, matched json, fired json, hex) */
  maturingRecord: (txid: string) => string

  /** SET watched txids seen in mempool, awaiting first confirmation */
  pending: string
  /** SET txids displaced by a reorg, awaiting re-resolution as the new chain is processed */
  limbo: string
  /** SET txids already evaluated (skip on re-sight) */
  evaluated: string
  /** SET transient snapshot during a reparse */
  mempoolCurrent: string
  /** SET mempool right after a block, feeds the dropped check */
  mempoolPostBlock: string
  /** SET latest block's txids, intersection scratch space */
  blockTxids: string
  /** SET per prevout — `outpoint:{txid}:{vout}` → claimant txids (pending + maturing spenders; replacement / proven-conflict detection) */
  outpointKey: (o: Outpoint) => string
  /** `outpoint:` — the prefix every outpoint SET key starts with (SCAN pattern; the Lua scripts build keys from it) */
  outpointPrefix: string

  /** ZSET eventId scored by nextAttemptAt unix ms — the durable delivery queue */
  outbox: string
  /** HASH per queued event (payload json, event, idempotencyKey, attempts, createdAt, lastError) */
  outboxRecord: (eventId: string) => string
  /** ZSET eventId scored by deadAt unix ms — events given up on after OUTBOX_MAX_AGE (capped) */
  outboxDead: string
  /** ZSET eventId scored by createdAt unix ms — the exact "oldest queued event" index (mirrors `outbox` membership) */
  outboxCreated: string
  /** ZSET txid scored by doneAt unix ms — txids whose tracking ENDED; a stale evaluation must not resurrect them */
  tombstones: string
  /** ZSET txid scored by exitAt unix ms — the RETIREMENT WATERMARK: txids dropped/replaced (not terminal); an evaluation that STARTED before exitAt must not resurrect them, a later one (a rebroadcast) may */
  retired: string
}

export function keysFor(network: Network): Keys {
  const p = `weir:${network}`
  return {
    addresses: `${p}:addresses`,
    expiries: `${p}:expiries`,
    tip: `${p}:tip`,
    blocks: `${p}:blocks`,
    maturing: `${p}:maturing`,
    maturingRecord: (txid: string) => `${p}:maturing:${txid}`,
    pending: `${p}:pending`,
    limbo: `${p}:limbo`,
    evaluated: `${p}:evaluated`,
    mempoolCurrent: `${p}:mempool:current`,
    mempoolPostBlock: `${p}:mempool:postBlock`,
    blockTxids: `${p}:block:txids`,
    outpointKey: (o: Outpoint) => `${p}:outpoint:${outpointField(o)}`,
    outpointPrefix: `${p}:outpoint:`,
    outbox: `${p}:outbox`,
    outboxRecord: (eventId: string) => `${p}:outbox:${eventId}`,
    outboxDead: `${p}:outbox:dead`,
    outboxCreated: `${p}:outbox:created`,
    tombstones: `${p}:tombstones`,
    retired: `${p}:retired`,
  }
}

/** One prevout as `{txid}:{vout}` — the tail of its SET key and the key of every claimants Map. */
export function outpointField(o: Outpoint): string {
  return `${o.txid}:${o.vout}`
}

/** Idempotency keys — consumers dedupe on these; shape is part of the public contract. */
export const idem = {
  seen: (net: Network, txid: string) => `${net}:${txid}:seen`,
  confirmed: (net: Network, txid: string, milestone: number, blockHash: string) =>
    `${net}:${txid}:confirmed:${milestone}:${blockHash}`,
  dropped: (net: Network, txid: string, tipHeight: number) => `${net}:${txid}:dropped:${tipHeight}`,
  replaced: (net: Network, txid: string, replacedBy: string) => `${net}:${txid}:dropped:replaced:${replacedBy}`,
  demoted: (net: Network, txid: string, fromBlockHash: string) => `${net}:${txid}:demoted:${fromBlockHash}`,
  conflicted: (net: Network, txid: string) => `${net}:${txid}:conflicted`,
  expired: (net: Network, address: string, expiresAtMs: number) => `${net}:${address}:expired:${expiresAtMs}`,
  heartbeat: (net: Network, timestampMs: number) => `${net}:heartbeat:${timestampMs}`,
}
