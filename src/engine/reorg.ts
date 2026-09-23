/**
 * Reorg handling — the limbo model. Spec: docs/DESIGN.md "src/engine/reorg.ts".
 *
 * INVARIANTS
 * - Nothing is adjudicated at detection time: a pruned/no-txindex node cannot answer
 *   "which block is txid X in now?". Displaced maturing txs go to the durable `limbo` SET
 *   (record kept), the ring/tip rewind to the fork point — ONE MULTI (`rewind`) — and the
 *   replacement chain then processes as a plain connected walk (one hash per height, no
 *   second fork search).
 * - Re-inclusion is discovered by the block pipeline's promotion step (no event; milestones
 *   re-fire under the new blockHash), and a PROVEN conflict (a new-chain tx spending one of
 *   a limbo tx's inputs) by its input scan — both leave limbo inside the block's MULTI.
 *   `resolveLimbo` runs only inside the tip path (a block that is the node's tip, or boot's
 *   settle step), and decides from the MEMPOOL SNAPSHOT that path validated against
 *   getbestblockhash — never from a live probe, which could describe a newer block: present
 *   in the snapshot → demoted, absent → conflicted by elimination (terminal). Each outcome
 *   is ONE MULTI that also enqueues its event — nothing here awaits delivery.
 * - Limbo is durable, so a crash between rewind and resolution re-resolves on the next
 *   block or boot.
 */
import type { Network, TxEvent } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import { idem } from '../store/keys'
import { log } from '../lib/log'

const CTX = 'reorg'

export type ReorgStore = Pick<
  Store,
  'ringHashAt' | 'ringAll' | 'rewind' | 'maturingEntries' | 'limboTxids' | 'removeLimbo' | 'demoteToPending' | 'conflict' | 'readRecord'
>

export type ReorgRpc = Pick<Rpc, 'getBlockHeader'>

export interface ForkPointDeps {
  store: Pick<ReorgStore, 'ringAll' | 'ringHashAt'>
  rpc: Pick<ReorgRpc, 'getBlockHeader'>
}

export interface ReorgDeps {
  cfg: { network: Network }
  store: ReorgStore
  rpc: ReorgRpc
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
 * Step 1 of the limbo model, ONE MULTI: every maturing tx included above the ancestor moves
 * to the persisted limbo SET (record kept), the ring is truncated above the ancestor and the
 * tip rewinds to it. After this the replacement chain connects like an ordinary gap walk.
 */
export async function enterLimboAndRewind(
  deps: { store: Pick<ReorgStore, 'maturingEntries' | 'ringHashAt' | 'rewind'> },
  ancestorHeight: number,
): Promise<void> {
  const displaced = (await deps.store.maturingEntries()).filter((e) => e.height > ancestorHeight).map((e) => e.txid)
  const ancestorHash = await deps.store.ringHashAt(ancestorHeight)
  if (ancestorHash === null) {
    // findForkPoint only returns heights that exist in the ring, so this is corruption.
    throw new Error(`[${CTX}] ring has no entry at ancestor height ${ancestorHeight} — cannot rewind`)
  }
  await deps.store.rewind({ hash: ancestorHash, height: ancestorHeight }, displaced)
  if (displaced.length > 0) log.warn(CTX, `${displaced.length} maturing tx(s) displaced by reorg → limbo: ${displaced.join(', ')}`)
  log.info(CTX, `rewound tip to fork point ${ancestorHash}@${ancestorHeight}`)
}

/**
 * Step 3 of the limbo model: adjudicate whatever the new chain did NOT re-include, from
 * `mempool` — the snapshot the tip path took and then validated (getbestblockhash still
 * equals the block it belongs to). A live re-probe here could see a block that landed after
 * the validation and call a just-re-mined tx `conflicted`.
 */
export async function resolveLimbo(deps: ReorgDeps, mempool: ReadonlySet<string>): Promise<void> {
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

    if (mempool.has(txid)) {
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
      await deps.store.demoteToPending(rec, ev)
      log.info(CTX, `demoted ${txid} (was ${rec.blockHash}@${rec.height}) — back to pending`)
      continue
    }

    // Not re-included by the new chain, not in the snapshot → conflicted BY ELIMINATION
    // (a PROVEN double-spend — the new chain spending one of its inputs — was already
    // adjudicated by the block pipeline's input scan and left limbo there). Terminal.
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
    await deps.store.conflict(rec, ev)
    log.info(CTX, `conflicted ${txid} (was ${rec.blockHash}@${rec.height}) — tracking ended`)
  }
}
