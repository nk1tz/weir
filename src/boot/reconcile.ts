/**
 * Boot reconciliation: align weir's durable tip with the node's best block.
 * First run initializes tip/ring forward-only (no history backfill). Otherwise any
 * distance between our tip and the node's best block — gap or reorg — is handled by
 * feeding the best block to the block pipeline's processor, whose connectivity step
 * walks/catches up by itself. Leftover reorg limbo is adjudicated by the caller
 * (src/index.ts runs `resolveLimbo` right after reconcile, unconditionally).
 *
 * Spec: docs/DESIGN.md "src/boot/reconcile.ts".
 */
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import { log } from '../lib/log'

const CTX = 'reconcile'

export interface ReconcileDeps {
  store: Pick<Store, 'getTip' | 'setTip'>
  rpc: Pick<Rpc, 'getBestBlockHash' | 'getBlockHeader' | 'getBlockRaw'>
  /** the block pipeline's processor (makeBlockProcessor) — handles gap AND reorg */
  processBlock(raw: Buffer): Promise<void>
}

export async function reconcile(deps: ReconcileDeps): Promise<void> {
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

  log.info(CTX, `tip ${tip.hash}@${tip.height} behind/diverged from node best ${best} — processing catch-up`)
  const raw = await deps.rpc.getBlockRaw(best)
  await deps.processBlock(raw)
  log.info(CTX, 'reconcile complete')
}
