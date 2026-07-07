import { Network } from '../lib/types'

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
  /** SET last full-reparse mempool snapshot */
  mempoolPrevious: string
  /** SET transient snapshot during a reparse */
  mempoolCurrent: string
  /** SET mempool right after a block, feeds the dropped check */
  mempoolPostBlock: string
  /** SET latest block's txids, intersection scratch space */
  blockTxids: string
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
    mempoolPrevious: `${p}:mempool:previous`,
    mempoolCurrent: `${p}:mempool:current`,
    mempoolPostBlock: `${p}:mempool:postBlock`,
    blockTxids: `${p}:block:txids`,
  }
}

/** Idempotency keys — consumers dedupe on these; shape is part of the public contract. */
export const idem = {
  seen: (net: Network, txid: string) => `${net}:${txid}:seen`,
  confirmed: (net: Network, txid: string, milestone: number, blockHash: string) =>
    `${net}:${txid}:confirmed:${milestone}:${blockHash}`,
  dropped: (net: Network, txid: string, tipHeight: number) => `${net}:${txid}:dropped:${tipHeight}`,
  demoted: (net: Network, txid: string, fromBlockHash: string) => `${net}:${txid}:demoted:${fromBlockHash}`,
  conflicted: (net: Network, txid: string) => `${net}:${txid}:conflicted`,
  expired: (net: Network, address: string, expiresAtMs: number) => `${net}:${address}:expired:${expiresAtMs}`,
  heartbeat: (net: Network, timestampMs: number) => `${net}:heartbeat:${timestampMs}`,
}
