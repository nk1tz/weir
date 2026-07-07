/**
 * Boot reconciliation: align weir's durable tip with the node's best block.
 * First run initializes tip/ring forward-only (no history backfill). Otherwise any
 * distance between our tip and the node's best block — gap or reorg — is handled by
 * feeding the best block to the block pipeline's processor, whose connectivity step
 * walks/catches up by itself.
 *
 * Spec: docs/DESIGN.md "src/boot/reconcile.ts".
 */
import { Tip } from '../lib/types'

interface Log {
  info(ctx: string, msg: string): void
  warn(ctx: string, msg: string): void
  error(ctx: string, msg: string): void
}

const consoleLog: Log = {
  info: (ctx, msg) => console.log(`[info] [${ctx}] ${msg}`),
  warn: (ctx, msg) => console.warn(`[warn] [${ctx}] ${msg}`),
  error: (ctx, msg) => console.error(`[error] [${ctx}] ${msg}`),
}

const CTX = 'reconcile'

export interface ReconcileDeps {
  store: {
    getTip(): Promise<Tip | null>
    setTip(tip: Tip): Promise<void>
    ringPut(height: number, hash: string): Promise<void>
  }
  rpc: {
    getBestBlockHash(): Promise<string>
    getBlockHeader(hash: string): Promise<{ height: number; previousblockhash?: string; time: number }>
    getBlockRaw(hash: string): Promise<Buffer>
  }
  /** the block pipeline's processor (makeBlockProcessor) — handles gap AND reorg */
  processBlock(raw: Buffer): Promise<void>
  /**
   * Adjudicates leftover reorg-limbo txs (src/engine/reorg.ts resolveLimbo). Called when
   * there is nothing to catch up: a crash between limbo-rewind and resolution would
   * otherwise leave displaced txs unadjudicated until the next block arrives.
   */
  resolveLimbo?: () => Promise<void>
  log?: Log
}

export async function reconcile(deps: ReconcileDeps): Promise<void> {
  const log = deps.log ?? consoleLog

  const tip = await deps.store.getTip()
  const best = await deps.rpc.getBestBlockHash()

  if (tip === null) {
    // First run: start tracking from the node's current best block. Forward-only.
    const header = await deps.rpc.getBlockHeader(best)
    await deps.store.ringPut(header.height, best)
    await deps.store.setTip({ hash: best, height: header.height })
    log.info(CTX, `first run — initialized tip to ${best}@${header.height} (forward-only, no backfill)`)
    return
  }

  if (best === tip.hash) {
    log.info(CTX, `tip ${tip.hash}@${tip.height} matches node best block — nothing to reconcile`)
    if (deps.resolveLimbo) await deps.resolveLimbo()
    return
  }

  log.info(CTX, `tip ${tip.hash}@${tip.height} behind/diverged from node best ${best} — processing catch-up`)
  const raw = await deps.rpc.getBlockRaw(best)
  await deps.processBlock(raw)
  log.info(CTX, 'reconcile complete')
}
