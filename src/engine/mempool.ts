import type { DecodedTx, Network } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import { log } from '../lib/log'

const CTX = 'mempool'

/** Structural deps — tests pass in-memory fakes (tests/fakes.ts). */
export interface MempoolReparserDeps {
  rpc: Pick<Rpc, 'getRawMempool' | 'getRawTransactionVerbose'>
  store: Pick<Store, 'replaceCurrentMempool' | 'newMempoolTxids' | 'clearCurrentMempool'>
  cfg: { network: Network }
  decodeRawTx(raw: Buffer | string, network: Network): DecodedTx
  /** the txPipeline evaluator (makeTxEvaluator); startedAtMs = when getrawtransaction was issued */
  evaluate(tx: DecodedTx, startedAtMs: number): Promise<void>
}

const FETCH_CONCURRENCY = 32

/**
 * How often index.ts runs the reparser unprompted (DESIGN "Outpoint tracking" rule 7): the
 * safety net for a fence-refused evaluation or a tx ZMQ missed without a sequence gap. One
 * getrawmempool plus fetches of un-evaluated txids only; the mutex skips overlap.
 */
export const MEMPOOL_REPARSE_INTERVAL_MS = 300_000

/** Run fn over items with at most `limit` in flight. Rejections propagate. */
async function mapBounded<T>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<void>,
): Promise<void> {
  let next = 0
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const i = next++
      if (i >= items.length) return
      await fn(items[i]!)
    }
  })
  await Promise.all(workers)
}

/**
 * Full mempool reparse: snapshot the node's mempool, evaluate every txid not already
 * evaluated, then drop the snapshot.
 *
 * Mutex (held in the factory closure — one reparser instance exists per daemon):
 * overlapping invocations are skipped, not queued. Errors propagate to the caller
 * after the mutex is released; nothing is swallowed here.
 */
export function makeMempoolReparser(deps: MempoolReparserDeps): () => Promise<void> {
  let running = false

  return async function reparseMempool(): Promise<void> {
    if (running) {
      log.info(CTX, 'reparse already running; skipping')
      return
    }
    running = true
    try {
      const txids = await deps.rpc.getRawMempool()
      await deps.store.replaceCurrentMempool(txids)
      const fresh = await deps.store.newMempoolTxids()

      await mapBounded(fresh, FETCH_CONCURRENCY, async (txid) => {
        // The fence clock starts BEFORE the RPC: what comes back is the node's view as of now.
        const startedAtMs = Date.now()
        const verbose = await deps.rpc.getRawTransactionVerbose(txid)
        if (verbose === null) return // vanished between snapshot and fetch — skip
        // Mined between snapshot and fetch: the block pipeline owns it; a `seen` now would be false.
        if (verbose.blockhash !== undefined) return
        const tx = deps.decodeRawTx(verbose.hex, deps.cfg.network)
        await deps.evaluate(tx, startedAtMs)
      })

      await deps.store.clearCurrentMempool()
    } finally {
      running = false
    }
  }
}
