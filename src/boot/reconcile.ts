/**
 * Boot reconciliation: align weir's durable tip with the node's active chain, then settle
 * the tip through the SAME snapshot-validated path a block uses (`settleTip`).
 *
 * - First run: initialize tip/ring forward-only (no history backfill).
 * - The stored tip is NOT on the node's active chain (a reorg while weir was down that the
 *   node has not yet built past our tip: its best is at or below our height): find the
 *   fork point from the node's best block and run the ordinary rewind (limbo + ring + tip,
 *   one MULTI). Without this, handing the best block to the processor would hit its
 *   "already in the ring" duplicate check and the displaced txs would stay maturing.
 * - Any remaining distance (gap, or a reorg the node has built past) is handled by feeding
 *   the best block to the block pipeline's processor, whose connectivity step walks by
 *   itself.
 * - Then `settleTip`: the tip-only work (eviction, TTL, evaluated prune, limbo resolution)
 *   from a mempool snapshot validated against getbestblockhash. If the node moved on while
 *   we reconciled, settle refuses and the loop goes again — boot never resolves limbo from
 *   a view of the node that a block it has not processed could contradict. Bounded: after
 *   MAX_ROUNDS the next ZMQ block (a gap walk) takes over.
 *
 * Spec: docs/DESIGN.md "src/boot/reconcile.ts".
 */
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import type { Network } from '../lib/types'
import { enterLimboAndRewind, findForkPoint } from '../engine/reorg'
import { log } from '../lib/log'
import { metrics } from '../lib/metrics'

const CTX = 'reconcile'

/** rounds of reconcile + settle before giving the node up as still moving */
const MAX_ROUNDS = 20

export interface ReconcileDeps {
  cfg: { network: Network }
  store: Pick<Store, 'getTip' | 'setTip' | 'ringAll' | 'ringHashAt' | 'rewind' | 'maturingEntries'>
  rpc: Pick<Rpc, 'getBestBlockHash' | 'getBlockHeader' | 'getBlockRaw'>
  /** the block pipeline's processor (makeBlockPipeline) — handles gap AND reorg */
  processBlock(raw: Buffer): Promise<void>
  /** the block pipeline's tip settler (makeBlockPipeline) — the snapshot-validated tip-only work */
  settleTip(): Promise<boolean>
}

/** One round: tip vs best. Returns once the stored tip is the node's best (as of the reads made). */
async function reconcileOnce(deps: ReconcileDeps): Promise<void> {
  const tip = await deps.store.getTip()
  const best = await deps.rpc.getBestBlockHash()

  if (tip === null) {
    // First run: start tracking from the node's current best block. Forward-only.
    const header = await deps.rpc.getBlockHeader(best)
    await deps.store.setTip({ hash: best, height: header.height }) // tip + ring, one MULTI
    log.info(CTX, `first run — initialized tip to ${best}@${header.height} (forward-only, no backfill)`)
    return
  }

  if (best === tip.hash) {
    log.info(CTX, `tip ${tip.hash}@${tip.height} matches node best block — nothing to reconcile`)
    return
  }

  const bestHeader = await deps.rpc.getBlockHeader(best)
  if (bestHeader.height <= tip.height) {
    // The node's chain is no higher than ours, so our tip cannot be on it: rewind to the
    // fork point first (the processor would otherwise see best as an already-known block).
    const { ancestorHeight, disconnected } = await findForkPoint(deps, best, bestHeader.height + 1)
    if (disconnected.length > 0) {
      metrics.counters.inc('weir_reorgs_total')
      log.warn(
        CTX,
        `stored tip ${tip.hash}@${tip.height} is not on the node's active chain (best ${best}@${bestHeader.height}): ` +
          `fork point height=${ancestorHeight}, ${disconnected.length} block(s) disconnected while weir was down`,
      )
      await enterLimboAndRewind(deps, ancestorHeight)
      if ((await deps.store.ringHashAt(ancestorHeight)) === best) return // the fork point IS the node's best
    }
  }

  log.info(CTX, `tip ${tip.hash}@${tip.height} behind/diverged from node best ${best} — processing catch-up`)
  const raw = await deps.rpc.getBlockRaw(best)
  await deps.processBlock(raw)
}

export async function reconcile(deps: ReconcileDeps): Promise<void> {
  for (let round = 1; ; round++) {
    await reconcileOnce(deps)
    if (await deps.settleTip()) {
      log.info(CTX, 'reconcile complete')
      return
    }
    if (round >= MAX_ROUNDS) {
      log.warn(CTX, `node still advancing after ${round} reconcile rounds — the next block settles the tip`)
      return
    }
    log.info(CTX, `node advanced during reconcile (round ${round}) — reconciling again`)
  }
}
