import type { DecodedTx, Network, TxEvent } from '../lib/types'
import type { Store } from '../store/redis'
import type { Sink } from '../delivery/webhook'
import { idem } from '../store/keys'
import { log } from '../lib/log'
import { matchTx } from './matcher'

const CTX = 'txPipeline'

/** Structural deps — tests pass in-memory fakes (tests/fakes.ts). */
export interface TxEvaluatorDeps {
  store: Pick<Store, 'isEvaluated' | 'markEvaluated' | 'addPending' | 'putRecord' | 'watchedSubset'>
  sink: Sink
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
 * - matched, seen enabled (milestone 0 configured) → deliver a `seen` TxEvent first;
 *   only on delivery success does the tx become evaluated + pending — a failed
 *   delivery leaves it un-evaluated so the next mempool reparse retries it.
 * - matched, seen disabled → evaluated + pending directly, nothing delivered.
 *
 * When a tx enters `pending` we also persist its per-txid record (same hash shape
 * as maturing records, height 0 / blockHash '') so later `dropped`/mined transitions
 * still have hex+matched available — a dropped tx cannot be re-fetched from a pruned
 * node with no txindex. The `maturing` ZSET itself still only indexes mined txs.
 */
export function makeTxEvaluator(deps: TxEvaluatorDeps): (tx: DecodedTx) => Promise<void> {
  const { store, sink, cfg } = deps

  return async function evaluateTx(tx: DecodedTx): Promise<void> {
    if (await store.isEvaluated(tx.txid)) return

    const matched = await matchTx(tx, store)
    if (matched.length === 0) {
      await store.markEvaluated(tx.txid)
      return
    }

    if (cfg.seenEnabled) {
      const event: TxEvent = {
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
      const delivered = await sink.deliver(event)
      if (!delivered) {
        // Deliberately NOT marked evaluated: the next mempool reparse re-evaluates
        // this txid and retries the seen delivery.
        log.warn(CTX, `seen delivery failed for ${tx.txid}; leaving un-evaluated for reparse retry`)
        return
      }
    }

    // Record before pending: anything in `pending` must have hex+matched on hand
    // for the dropped/mined transitions (see doc comment above).
    await store.putRecord({
      txid: tx.txid,
      height: 0,
      blockHash: '',
      matched,
      fired: [],
      hex: tx.hex,
    })
    await store.addPending(tx.txid)
    await store.markEvaluated(tx.txid)
  }
}

/** ZMQ rawtx path: decode → evaluate. */
export function makeRawTxHandler(deps: RawTxHandlerDeps): (raw: Buffer) => Promise<void> {
  const evaluate = makeTxEvaluator(deps)
  return async function handleRawTx(raw: Buffer): Promise<void> {
    const tx = deps.decodeRawTx(raw, deps.cfg.network)
    await evaluate(tx)
  }
}
