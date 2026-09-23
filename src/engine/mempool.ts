import type { DecodedTx, Network } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'

/** Structural deps — tests pass in-memory fakes (tests/fakes.ts). */
export interface MempoolReparserDeps {
  rpc: Pick<Rpc, 'getRawMempool' | 'getRawTransactionVerbose'>
  store: Pick<Store, 'evaluatedTxids'>
  cfg: { network: Network }
  decodeRawTx(raw: Buffer | string, network: Network): DecodedTx
  /** the txPipeline evaluator (makeTxEvaluator) */
  evaluate(tx: DecodedTx): Promise<void>
}

/** getrawtransaction calls in flight per batch — reads only; evaluations stay serial. */
const FETCH_BATCH = 32

/**
 * How often index.ts runs the reparser unprompted (DESIGN "Outpoint tracking" rule 7): the
 * safety net for a tx ZMQ missed without a sequence gap. One getrawmempool plus fetches of
 * un-evaluated txids only.
 */
export const MEMPOOL_REPARSE_INTERVAL_MS = 300_000

/**
 * Full mempool reparse: snapshot the node's mempool, evaluate every txid not in the
 * `evaluated` dedupe list, one after the other. The whole body is ONE engine-queue item
 * (index.ts), which is why it needs no mutex and no snapshot key in redis: no block and no
 * other evaluation can interleave with it. Errors propagate; nothing is swallowed here.
 */
export function makeMempoolReparser(deps: MempoolReparserDeps): () => Promise<void> {
  return async function reparseMempool(): Promise<void> {
    const txids = await deps.rpc.getRawMempool()
    const evaluated = new Set(await deps.store.evaluatedTxids())
    const fresh = txids.filter((t) => !evaluated.has(t))

    for (let i = 0; i < fresh.length; i += FETCH_BATCH) {
      const batch = fresh.slice(i, i + FETCH_BATCH)
      const fetched = await Promise.all(batch.map((txid) => deps.rpc.getRawTransactionVerbose(txid)))
      for (const verbose of fetched) {
        if (verbose === null) continue // vanished between snapshot and fetch — skip
        // Mined between snapshot and fetch: the block pipeline owns it; a `seen` now would be false.
        if (verbose.blockhash !== undefined) continue
        await deps.evaluate(deps.decodeRawTx(verbose.hex, deps.cfg.network))
      }
    }
  }
}
