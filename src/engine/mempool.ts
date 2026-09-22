import type { DecodedTx, Network } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import { log } from '../lib/log'

const CTX = 'mempool'

/** Structural deps — tests pass in-memory fakes (tests/fakes.ts). */
export interface MempoolReparserDeps {
  rpc: Pick<Rpc, 'getRawMempool' | 'getRawTransactionVerbose'>
  store: Pick<Store, 'replaceCurrentMempool' | 'newMempoolTxids' | 'rotateMempool'>
  cfg: { network: Network }
  decodeRawTx(raw: Buffer | string, network: Network): DecodedTx
  /** the txPipeline evaluator (makeTxEvaluator) */
  evaluate(tx: DecodedTx): Promise<void>
}

const FETCH_CONCURRENCY = 32

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
 * Full mempool reparse: snapshot the node's mempool, evaluate only the txids not
 * seen in the previous snapshot and not already evaluated, then rotate snapshots.
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
        const verbose = await deps.rpc.getRawTransactionVerbose(txid)
        if (verbose === null) return // vanished between snapshot and fetch — skip
        const tx = deps.decodeRawTx(verbose.hex, deps.cfg.network)
        await deps.evaluate(tx)
      })

      await deps.store.rotateMempool()
    } finally {
      running = false
    }
  }
}
