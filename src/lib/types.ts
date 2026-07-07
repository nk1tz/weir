export type Network = 'mainnet' | 'testnet' | 'signet' | 'regtest'

export type ScriptType = 'p2wpkh' | 'p2wsh' | 'p2tr' | 'p2pkh' | 'p2sh'

/** One decoded transaction output. address is null for script types weir doesn't match. */
export interface DecodedOutput {
  vout: number
  valueSats: number
  address: string | null
  scriptType: ScriptType | null
}

export interface DecodedTx {
  txid: string
  hex: string
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
}

export type WeirEvent = TxEvent | ExpiredEvent | HeartbeatEvent

/** Per-tx record kept while a tx is between first confirmation and max milestone. */
export interface MaturingRecord {
  txid: string
  /** height of the block that included it */
  height: number
  blockHash: string
  matched: MatchedOutput[]
  /** milestones already successfully delivered (e.g. [1] after the 1-conf event) */
  fired: number[]
  hex: string
}

export interface Tip {
  hash: string
  height: number
}
