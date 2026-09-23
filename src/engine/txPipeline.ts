import type { DecodedTx, Network, TxEvent } from '../lib/types'
import type { Store } from '../store/redis'
import { idem } from '../store/keys'
import { log } from '../lib/log'
import { matchTx } from './matcher'

const CTX = 'txPipeline'

/** Structural deps — tests pass in-memory fakes (tests/fakes.ts). No sink: events are enqueued. */
export interface TxEvaluatorDeps {
  store: Pick<Store, 'isEvaluated' | 'markEvaluated' | 'watchedSubset' | 'recordSeen'>
  cfg: { network: Network; seenEnabled: boolean }
}

export interface RawTxHandlerDeps extends TxEvaluatorDeps {
  decodeRawTx(raw: Buffer | string, network: Network): DecodedTx
}

/**
 * Evaluate one decoded transaction against the watch set.
 *
 * - already evaluated → no-op.
 * - no matched outputs → markEvaluated only.
 * - matched → `recordSeen(rec, event | null)`: ONE atomic store step that persists the
 *   seen-time record (height 0 / blockHash '' — a dropped tx cannot be re-fetched from a
 *   pruned node), marks the tx pending + evaluated, and enqueues the `seen` event (null when
 *   milestone 0 is not configured). Nothing here awaits the network; the outbox delivers.
 *   recordSeen resolves false when a concurrent path (a block mining the tx, or a duplicate
 *   evaluation) got there first — then there is nothing left to do — or when the evaluation
 *   is older than MAX_EVALUATION_AGE_MS (the fence; the txid stays un-evaluated).
 *
 * `startedAtMs` is when this evaluation's view of the tx was taken: ZMQ receipt for the
 * rawtx path, the moment getrawtransaction was issued for the reparser. It is the fence's
 * clock — a response parked for longer than the tombstone TTL must never land.
 */
export function makeTxEvaluator(deps: TxEvaluatorDeps): (tx: DecodedTx, startedAtMs: number) => Promise<void> {
  const { store, cfg } = deps

  return async function evaluateTx(tx: DecodedTx, startedAtMs: number): Promise<void> {
    if (await store.isEvaluated(tx.txid)) return

    const matched = await matchTx(tx, store)
    if (matched.length === 0) {
      await store.markEvaluated(tx.txid)
      return
    }

    const event: TxEvent | null = cfg.seenEnabled
      ? {
          version: 1,
          event: 'seen',
          network: cfg.network,
          txid: tx.txid,
          confs: 0,
          matched,
          idempotencyKey: idem.seen(cfg.network, tx.txid),
          timestamp: Date.now(),
          blockHeight: null,
          blockHash: null,
          hex: tx.hex,
        }
      : null

    const recorded = await store.recordSeen(
      { txid: tx.txid, height: 0, blockHash: '', matched, fired: [], hex: tx.hex },
      event,
      startedAtMs,
    )
    if (!recorded) {
      log.info(CTX, `${tx.txid} not recorded (tracked by a concurrent path, or a stale evaluation) — seen skipped`)
    }
  }
}

/** ZMQ rawtx path: decode → evaluate, with startedAtMs = receipt time. */
export function makeRawTxHandler(deps: RawTxHandlerDeps): (raw: Buffer) => Promise<void> {
  const evaluate = makeTxEvaluator(deps)
  return async function handleRawTx(raw: Buffer): Promise<void> {
    const startedAtMs = Date.now()
    const tx = deps.decodeRawTx(raw, deps.cfg.network)
    await evaluate(tx, startedAtMs)
  }
}
