/**
 * Reorg handling — the limbo model. Spec: docs/DESIGN.md "src/engine/reorg.ts".
 *
 * INVARIANTS
 * - Nothing is adjudicated at detection time: a pruned/no-txindex node cannot answer
 *   "which block is txid X in now?". Displaced maturing txs go to the durable `limbo` SET
 *   (record kept), the ring/tip rewind to the fork point, and the replacement chain then
 *   processes as a plain connected walk (one hash per height, no second fork search).
 * - Re-inclusion is discovered by the block pipeline's promotion step (no event; milestones
 *   re-fire under the new blockHash). `resolveLimbo` runs only after the TIP block finishes
 *   (and at boot), when the node's mempool reflects the new chain: present → demoted,
 *   absent → conflicted (terminal).
 * - Limbo is durable, so a crash between rewind and resolution re-resolves on the next
 *   block or boot.
 */
import type { Network, TxEvent } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import type { Sink } from '../delivery/webhook'
import { idem } from '../store/keys'
import { log } from '../lib/log'

const CTX = 'reorg'

export type ReorgStore = Pick<
  Store,
  | 'ringHashAt'
  | 'ringAll'
  | 'ringRemoveAbove'
  | 'setTip'
  | 'maturingEntries'
  | 'unindexMaturing'
  | 'removeMaturing'
  | 'addLimbo'
  | 'limboTxids'
  | 'removeLimbo'
  | 'demoteToPending'
  | 'removePending'
  | 'readRecord'
  | 'deleteRecord'
>

export type ReorgRpc = Pick<Rpc, 'getBlockHeader' | 'getMempoolEntry'>

export interface ForkPointDeps {
  store: Pick<ReorgStore, 'ringAll' | 'ringHashAt'>
  rpc: Pick<ReorgRpc, 'getBlockHeader'>
}

export interface ReorgDeps {
  cfg: { network: Network }
  store: ReorgStore
  rpc: ReorgRpc
  sink: Sink
}

/**
 * Walk back from `incomingPrevHash` via getBlockHeader until a header's hash matches the
 * ring entry at its height. Ring entries above that ancestor are the disconnected blocks.
 * Handles the pure-gap case too (walk lands exactly on the tip → disconnected is empty).
 * If the walk exits the ring (reorg deeper than tracked), log loudly and treat the lowest
 * ring entry as the ancestor — that is the documented bound (ring depth = reorg shield).
 */
export async function findForkPoint(
  deps: ForkPointDeps,
  incomingPrevHash: string,
  incomingHeight: number,
): Promise<{ ancestorHeight: number; disconnected: Array<{ height: number; hash: string }> }> {
  // Snapshot the whole ring once (ascending): per-height hashes plus the walk's lower bound.
  // ringAll, not a range above 0: a fresh regtest ring holds genesis at height 0.
  const ring = await deps.store.ringAll()
  if (ring.length === 0) {
    log.error(CTX, 'ring is empty during fork-point search — cannot detect reorg, treating incoming block as connected')
    return { ancestorHeight: incomingHeight - 1, disconnected: [] }
  }
  const byHeight = new Map<number, string>(ring.map((e) => [e.height, e.hash]))
  const minHeight = ring[0]!.height

  let hash = incomingPrevHash
  let height = incomingHeight - 1
  while (height >= minHeight) {
    if (byHeight.get(height) === hash) {
      return { ancestorHeight: height, disconnected: ring.filter((e) => e.height > height) }
    }
    const header = await deps.rpc.getBlockHeader(hash)
    if (!header.previousblockhash) break // genesis — nothing further back
    hash = header.previousblockhash
    height = height - 1
  }

  log.error(
    CTX,
    `reorg deeper than the tracked ring (ring floor height=${minHeight}) — treating lowest ring entry as ancestor; ` +
      'events for blocks below the ring are NOT replayed (documented bound)',
  )
  return { ancestorHeight: minHeight, disconnected: ring.filter((e) => e.height > minHeight) }
}

/**
 * Step 1 of the limbo model: move every maturing tx included above the ancestor into the
 * persisted limbo SET (record kept), truncate the ring above the ancestor and rewind the
 * tip to it. After this the replacement chain connects like an ordinary gap walk.
 */
export async function enterLimboAndRewind(deps: ReorgDeps, ancestorHeight: number): Promise<void> {
  const displaced = (await deps.store.maturingEntries()).filter((e) => e.height > ancestorHeight)

  if (displaced.length > 0) {
    const txids = displaced.map((e) => e.txid)
    await deps.store.addLimbo(txids)
    await deps.store.unindexMaturing(txids)
    log.warn(CTX, `${displaced.length} maturing tx(s) displaced by reorg → limbo: ${txids.join(', ')}`)
  }

  const ancestorHash = await deps.store.ringHashAt(ancestorHeight)
  if (ancestorHash === null) {
    // findForkPoint only returns heights that exist in the ring, so this is corruption.
    throw new Error(`[${CTX}] ring has no entry at ancestor height ${ancestorHeight} — cannot rewind`)
  }
  await deps.store.ringRemoveAbove(ancestorHeight)
  await deps.store.setTip({ hash: ancestorHash, height: ancestorHeight })
  log.info(CTX, `rewound tip to fork point ${ancestorHash}@${ancestorHeight}`)
}

/**
 * Step 3 of the limbo model: adjudicate whatever the new chain did NOT re-include.
 * Runs after the tip block finishes processing (and at boot when already reconciled).
 * Mempool probe is now trustworthy — bitcoind has fully switched to the new chain.
 */
export async function resolveLimbo(deps: ReorgDeps): Promise<void> {
  const leftover = await deps.store.limboTxids()
  if (leftover.length === 0) return
  const net = deps.cfg.network

  for (const txid of leftover) {
    const rec = await deps.store.readRecord(txid)
    if (rec === null) {
      log.warn(CTX, `limbo txid ${txid} has no record — removing dangling limbo entry`)
      await deps.store.removeLimbo(txid)
      continue
    }

    const mempoolEntry = await deps.rpc.getMempoolEntry(txid)
    if (mempoolEntry !== null) {
      // Demoted: back in the mempool. Block fields refer to the OLD (disconnected) block.
      const ev: TxEvent = {
        version: 1,
        event: 'demoted',
        network: net,
        txid,
        confs: 0,
        matched: rec.matched,
        blockHeight: rec.height,
        blockHash: rec.blockHash,
        hex: rec.hex,
        idempotencyKey: idem.demoted(net, txid, rec.blockHash),
        timestamp: Date.now(),
      }
      const ok = await deps.sink.deliver(ev)
      if (!ok) log.warn(CTX, `demoted delivery failed for ${txid} — best-effort one-shot, proceeding`)
      // One MULTI: record back to height 0, pending, evaluated (so the next reparse does not
      // re-fire `seen` — the tip prune forgot it when it was mined), out of limbo.
      await deps.store.demoteToPending(rec)
      log.info(CTX, `demoted ${txid} (was ${rec.blockHash}@${rec.height}) — back to pending`)
      continue
    }

    // Not re-included by the new chain, not in the mempool → conflicted. Terminal.
    const lastDepth = rec.fired.length > 0 ? Math.max(...rec.fired) : 0
    const ev: TxEvent = {
      version: 1,
      event: 'conflicted',
      network: net,
      txid,
      confs: lastDepth,
      matched: rec.matched,
      blockHeight: rec.height,
      blockHash: rec.blockHash,
      hex: rec.hex,
      idempotencyKey: idem.conflicted(net, txid),
      timestamp: Date.now(),
    }
    const ok = await deps.sink.deliver(ev)
    if (!ok) log.warn(CTX, `conflicted delivery failed for ${txid} — best-effort one-shot, proceeding`)
    await deps.store.removePending(txid)
    await deps.store.removeMaturing(txid) // deletes the record too
    await deps.store.removeLimbo(txid)
    log.info(CTX, `conflicted ${txid} (was ${rec.blockHash}@${rec.height}) — tracking ended`)
  }
}
