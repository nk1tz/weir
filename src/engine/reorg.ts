/**
 * Reorg handling — the limbo model.
 *
 * On weir's target deployment (pruned node, NO txindex) there is no reliable way to ask
 * "which block is txid X in now?" at reorg time: getrawtransaction without a blockhash
 * only answers for mempool txs. So weir never adjudicates a displaced tx's fate at
 * detection time. Instead:
 *
 *   1. `enterLimboAndRewind` — every maturing tx included above the fork point moves to
 *      the persisted `limbo` SET (record kept, maturing index entry removed), the ring is
 *      truncated to the ancestor and the tip rewound to it. The replacement chain then
 *      processes as a plain connected walk (no second fork search, one hash per height).
 *   2. Re-inclusion is discovered NATURALLY: the block pipeline's promotion step matches
 *      each new block's txs; a limbo txid found in a new block re-enters maturing with a
 *      fresh height/blockHash and an empty fired list — milestones re-fire on the next
 *      sweep with new-blockhash idempotency keys. No event at re-inclusion itself.
 *   3. `resolveLimbo` — runs after the TIP block finishes (and at boot when there is
 *      nothing to catch up): whatever is still in limbo was NOT re-included, so probe the
 *      mempool: present → `demoted` (back to pending); absent → `conflicted` (terminal).
 *
 * The limbo SET is durable redis state, so a crash between rewind and resolution is safe:
 * the next boot's reconcile + resolveLimbo finishes the job.
 *
 * Spec: docs/DESIGN.md "src/engine/reorg.ts".
 */
import { MaturingRecord, Network, TxEvent, WeirEvent } from '../lib/types'
import { idem } from '../store/keys'

export interface Log {
  info(ctx: string, msg: string): void
  warn(ctx: string, msg: string): void
  error(ctx: string, msg: string): void
}

const consoleLog: Log = {
  info: (ctx, msg) => console.log(`[info] [${ctx}] ${msg}`),
  warn: (ctx, msg) => console.warn(`[warn] [${ctx}] ${msg}`),
  error: (ctx, msg) => console.error(`[error] [${ctx}] ${msg}`),
}

const CTX = 'reorg'

export interface ReorgStore {
  ringHashAt(height: number): Promise<string | null>
  ringAbove(height: number): Promise<Array<{ height: number; hash: string }>>
  ringRemoveAbove(height: number): Promise<void>
  setTip(tip: { hash: string; height: number }): Promise<void>
  maturingEntries(): Promise<Array<{ txid: string; height: number }>>
  unindexMaturing(txid: string): Promise<void>
  removeMaturing(txid: string): Promise<void>
  addLimbo(txids: string[]): Promise<void>
  limboTxids(): Promise<string[]>
  removeLimbo(txid: string): Promise<void>
  addPending(txid: string): Promise<void>
  removePending(txid: string): Promise<void>
  /** hash-only record ops: pending/maturing tx records live in maturing:{txid} from seen-time */
  putRecord(rec: MaturingRecord): Promise<void>
  readRecord(txid: string): Promise<MaturingRecord | null>
  deleteRecord(txid: string): Promise<void>
}

export interface ReorgRpc {
  getBlockHeader(hash: string): Promise<{ height: number; previousblockhash?: string; time: number }>
  getMempoolEntry(txid: string): Promise<object | null>
}

export interface ForkPointDeps {
  store: Pick<ReorgStore, 'ringAbove' | 'ringHashAt'>
  rpc: Pick<ReorgRpc, 'getBlockHeader'>
  log?: Log
}

export interface ReorgDeps {
  cfg: { network: Network }
  store: ReorgStore
  rpc: ReorgRpc
  sink: { deliver(event: WeirEvent): Promise<boolean> }
  log?: Log
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
  const log = deps.log ?? consoleLog
  // Snapshot the whole ring once: gives us per-height hashes plus the lower bound of the walk.
  const ring = (await deps.store.ringAbove(0)).slice().sort((a, b) => a.height - b.height)
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
    const nextHeight = Math.min(height - 1, header.height - 1) // guarantee progress
    hash = header.previousblockhash
    height = nextHeight
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
  const log = deps.log ?? consoleLog
  const displaced = (await deps.store.maturingEntries()).filter((e) => e.height > ancestorHeight)

  if (displaced.length > 0) {
    await deps.store.addLimbo(displaced.map((e) => e.txid))
    for (const { txid } of displaced) {
      await deps.store.unindexMaturing(txid)
    }
    log.warn(CTX, `${displaced.length} maturing tx(s) displaced by reorg → limbo: ${displaced.map((e) => e.txid).join(', ')}`)
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
  const log = deps.log ?? consoleLog
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
      // Return the tx to the pending pool; keep its record available (height 0 = unmined).
      await deps.store.putRecord({ ...rec, height: 0, blockHash: '', fired: [] })
      await deps.store.addPending(txid)
      await deps.store.removeLimbo(txid)
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
    await deps.store.removeMaturing(txid)
    await deps.store.deleteRecord(txid)
    await deps.store.removeLimbo(txid)
    log.info(CTX, `conflicted ${txid} (was ${rec.blockHash}@${rec.height}) — tracking ended`)
  }
}
