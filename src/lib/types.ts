export type Network = 'mainnet' | 'testnet' | 'signet' | 'regtest'

export type ScriptType = 'p2wpkh' | 'p2wsh' | 'p2tr' | 'p2pkh' | 'p2sh'

/** One decoded transaction output. address is null for script types weir doesn't match. */
export interface DecodedOutput {
  vout: number
  valueSats: number
  address: string | null
  scriptType: ScriptType | null
}

/** A previous output spent by a transaction input — the literal bytes in the tx, never resolved. */
export interface Outpoint {
  /** prev txid, display-order hex */
  txid: string
  vout: number
}

export interface DecodedTx {
  txid: string
  hex: string
  /** every input's prevout; the coinbase input (txid all zeros, vout 0xffffffff) is omitted */
  inputs: Outpoint[]
  outputs: DecodedOutput[]
}

export interface DecodedBlock {
  hash: string
  prevHash: string
  /** block header time, unix seconds */
  time: number
  txs: DecodedTx[]
}

/** An output that pays a watched address — what webhook consumers care about. */
export interface MatchedOutput {
  address: string
  vout: number
  valueSats: number
}

export type EventType =
  | 'seen'
  | 'confirmed'
  | 'dropped'
  | 'demoted'
  | 'conflicted'
  | 'expired'
  | 'heartbeat'

interface EventBase {
  version: 1
  network: Network
  /** unique per logical occurrence; consumers MUST dedupe on this (delivery is at-least-once) */
  idempotencyKey: string
  /** unix ms */
  timestamp: number
}

/** seen | confirmed | dropped | demoted | conflicted */
export interface TxEvent extends EventBase {
  event: Exclude<EventType, 'expired' | 'heartbeat'>
  txid: string
  /** 0 for seen/dropped/demoted, milestone N for confirmed, last-known depth for conflicted */
  confs: number
  matched: MatchedOutput[]
  blockHeight: number | null
  blockHash: string | null
  hex: string
  /**
   * `dropped`: `replaced` (an input was spent by another tx weir saw — mempool or block) or
   * `evicted` (the residual verdict of the tip-block dropped check). `conflicted`:
   * `double-spend` when the new chain provably spent one of the tx's inputs.
   */
  reason?: 'replaced' | 'evicted' | 'double-spend'
  /** `dropped` with reason `replaced`: the txid that spent one of this tx's inputs */
  replacedBy?: string
  /** `conflicted` with reason `double-spend`: the confirmed txid that spent one of this tx's inputs */
  conflictingTxid?: string
}

/** a TTL'd watch passed its deadline without being paid */
export interface ExpiredEvent extends EventBase {
  event: 'expired'
  address: string
}

/** periodic proof-of-life (dead-man's switch) */
export interface HeartbeatEvent extends EventBase {
  event: 'heartbeat'
  tipHeight: number | null
  watchCount: number
  /** 0-100, or null when redis has no maxmemory set */
  memoryUsedPct: number | null
  /** events queued for delivery (undelivered, still retrying) */
  outboxDepth: number
  /** age in seconds of the oldest queued event, null when the outbox is empty */
  outboxOldestAgeSec: number | null
  /** events given up on after OUTBOX_MAX_AGE (capped at OUTBOX_DEAD_MAX) */
  deadLetterCount: number
}

export type WeirEvent = TxEvent | ExpiredEvent | HeartbeatEvent

/** Per-tx record kept while a tx is between first confirmation and max milestone. */
export interface MaturingRecord {
  txid: string
  /** height of the block that included it */
  height: number
  blockHash: string
  matched: MatchedOutput[]
  /** milestones whose `confirmed` event has been ENQUEUED (e.g. [1] after the 1-conf event) */
  fired: number[]
  hex: string
  /** the tx's inputs (see Outpoint) — kept so terminal cleanup can remove them from `outpoints` */
  inputs: Outpoint[]
}

export interface Tip {
  hash: string
  height: number
}
