/**
 * Boot reconciliation: align weir's durable tip with the node's active chain, then settle
 * the tip through the SAME snapshot-validated path a block uses (`settleTip`).
 *
 * - First, the ring invariant (one hash per height) is made true (`normalizeRing`).
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
 *   a view of the node that a block it has not processed could contradict. UNBOUNDED: it
 *   returns only once settle succeeded (stored tip == the node's best, snapshot validated);
 *   until then `/ready` stays 503 and each round logs both heights, so a node that never
 *   converges is visible rather than silently left with an unprocessed block.
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

export interface ReconcileDeps {
  cfg: { network: Network; ringSize: number }
  store: Pick<Store, 'getTip' | 'setTip' | 'ringAll' | 'rebuildRing' | 'rewind' | 'maturingEntries' | 'resetTracking'>
  rpc: Pick<Rpc, 'getBestBlockHash' | 'getBlockHeader' | 'getBlockHeaderIfKnown' | 'getBlockRaw'>
  /** the block pipeline's processor (makeBlockPipeline) — handles gap AND reorg */
  processBlock(raw: Buffer): Promise<void>
  /** the block pipeline's tip settler (makeBlockPipeline) — the snapshot-validated tip-only work */
  settleTip(): Promise<boolean>
}

/**
 * The ring INVARIANT — exactly one hash per height — made true at boot rather than tolerated:
 * a ring written before the invariant (v0.1 data) may hold two hashes at one height, which
 * makes the fork search and the rewind disagree about which one is the ancestor. If any
 * height has more than one hash, rebuild the ring from the stored tip's header ancestry
 * (`getblockheader` down `ringSize` heights) in one MULTI. A stored tip the node does not
 * know at all → the prune-window reset (tracking wiped, tip/ring jumped to the node's best).
 */
async function normalizeRing(deps: ReconcileDeps): Promise<void> {
  const tip = await deps.store.getTip()
  if (tip === null) return
  const ring = await deps.store.ringAll()
  const perHeight = new Map<number, number>()
  for (const e of ring) perHeight.set(e.height, (perHeight.get(e.height) ?? 0) + 1)
  const dirty = [...perHeight].filter(([, n]) => n > 1).map(([h]) => h)
  if (dirty.length === 0) return
  log.warn(CTX, `ring holds more than one hash at height(s) ${dirty.join(', ')} (written before the one-per-height invariant) — rebuilding it from the stored tip's ancestry`)

  /** the header ancestry of `from`, newest first, at most ringSize entries; [] when the node does not know `from` */
  const ancestry = async (from: string): Promise<Array<{ height: number; hash: string }>> => {
    const entries: Array<{ height: number; hash: string }> = []
    let hash: string | undefined = from
    while (hash !== undefined && entries.length < deps.cfg.ringSize) {
      const header = await deps.rpc.getBlockHeaderIfKnown(hash)
      if (header === null) break
      entries.push({ height: header.height, hash })
      hash = header.previousblockhash
    }
    return entries
  }

  let entries = await ancestry(tip.hash)
  if (entries.length === 0) {
    // The node does not know our tip at all: the prune-window reset (tracking wiped, tip
    // jumped to the node's best), then the ring from the best's ancestry.
    const best = await deps.rpc.getBestBlockHash()
    const bestHeader = await deps.rpc.getBlockHeader(best)
    const lost = await deps.store.resetTracking({ hash: best, height: bestHeader.height })
    log.error(
      CTX,
      `the node does not know the stored tip ${tip.hash}@${tip.height} — re-initializing forward-only from ${best}@${bestHeader.height}. ` +
        `TRACKING LOST for ${lost.length} in-flight tx(s)${lost.length > 0 ? `: ${lost.join(', ')}` : ''}.`,
    )
    entries = await ancestry(best)
  }
  await deps.store.rebuildRing(entries)
  log.info(CTX, `ring rebuilt: ${entries.length} block(s), ${entries[entries.length - 1]!.height}..${entries[0]!.height}`)
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
    const { ancestorHeight, ancestorHash, disconnected } = await findForkPoint(deps, best, bestHeader.height + 1)
    if (disconnected.length > 0) {
      metrics.counters.inc('weir_reorgs_total')
      log.warn(
        CTX,
        `stored tip ${tip.hash}@${tip.height} is not on the node's active chain (best ${best}@${bestHeader.height}): ` +
          `fork point height=${ancestorHeight}, ${disconnected.length} block(s) disconnected while weir was down`,
      )
      await enterLimboAndRewind(deps, { height: ancestorHeight, hash: ancestorHash })
      if (ancestorHash === best) return // the fork point IS the node's best
    }
  }

  log.info(CTX, `tip ${tip.hash}@${tip.height} behind/diverged from node best ${best} — processing catch-up`)
  const raw = await deps.rpc.getBlockRaw(best)
  await deps.processBlock(raw)
}

/** Resolves only once the stored tip is the node's best and the tip is settled — never before. */
export async function reconcile(deps: ReconcileDeps): Promise<void> {
  await normalizeRing(deps) // before any block work: the fork search and the rewind must agree
  for (let round = 1; ; round++) {
    await reconcileOnce(deps)
    if (await deps.settleTip()) {
      log.info(CTX, `reconcile complete (${round} round${round === 1 ? '' : 's'})`)
      return
    }
    const tip = await deps.store.getTip()
    const best = await deps.rpc.getBestBlockHash()
    const bestHeight = (await deps.rpc.getBlockHeader(best)).height
    log.info(CTX, `round ${round}: node advanced during reconcile — stored tip ${tip?.hash ?? 'none'}@${tip?.height ?? '-'}, node best ${best}@${bestHeight}; reconciling again`)
  }
}
