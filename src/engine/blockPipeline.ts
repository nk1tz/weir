/**
 * Block pipeline: the ordered per-block sequence
 *   connectivity (gap/reorg) → mined-watch promotion → milestone sweep → dropped check
 *   → TTL sweep → prune/tip/ring, then limbo resolution once the TIP block is done.
 *
 * Spec: docs/DESIGN.md "src/engine/blockPipeline.ts".
 *
 * Two invariants that earlier revisions got wrong (see the review findings):
 *
 * - REORGS use the limbo model (src/engine/reorg.ts): displaced maturing txs move to the
 *   persisted `limbo` SET and the tip/ring REWIND to the fork point, so the replacement
 *   chain processes as a plain connected walk — one hash per height in the ring, no
 *   second fork search, and re-inclusion is discovered by the promotion step rather than
 *   by txid lookups that a pruned/no-txindex node cannot answer.
 *
 * - The dropped check, TTL sweep and evaluated-prune compare against the node's LIVE
 *   mempool, which is only meaningful at the chain tip — during a multi-block catch-up a
 *   pending tx mined in a LATER missed block would otherwise be falsely reported dropped.
 *   They run only when `isTip` (the outermost block), never for historical catch-up blocks.
 *
 * Record lifecycle note: a tx's `maturing:{txid}` hash record exists from SEEN-time onward
 * (the tx pipeline stores it at height 0 when the tx enters `pending`), so dropped/mined
 * transitions always have hex+matched at hand. The `maturing` ZSET only indexes MINED txs.
 */
import { DecodedBlock, DecodedTx, ExpiredEvent, MatchedOutput, MaturingRecord, Network, Tip, TxEvent, WeirEvent } from '../lib/types'
import { idem } from '../store/keys'
import { enterLimboAndRewind, findForkPoint, Log, ReorgRpc, ReorgStore, resolveLimbo } from './reorg'

const consoleLog: Log = {
  info: (ctx, msg) => console.log(`[info] [${ctx}] ${msg}`),
  warn: (ctx, msg) => console.warn(`[warn] [${ctx}] ${msg}`),
  error: (ctx, msg) => console.error(`[error] [${ctx}] ${msg}`),
}

const CTX = 'blockPipeline'

export interface BlockStore extends ReorgStore {
  getTip(): Promise<Tip | null>
  ringPut(height: number, hash: string): Promise<void>
  ringPrune(keep: number): Promise<void>
  setBlockTxids(txids: string[]): Promise<void>
  pendingInBlock(): Promise<string[]>
  watchedSubset(addrs: string[]): Promise<string[]>
  isEvaluated(txid: string): Promise<boolean>
  markEvaluated(txid: string): Promise<void>
  unmarkEvaluated(txid: string): Promise<void>
  addMaturing(rec: MaturingRecord): Promise<void>
  getMaturing(txid: string): Promise<MaturingRecord | null>
  setMaturingFired(txid: string, fired: number[]): Promise<void>
  replacePostBlockMempool(txids: string[]): Promise<void>
  droppedPending(): Promise<string[]>
  pruneEvaluated(): Promise<void>
  dueExpiries(nowMs: number): Promise<Array<{ address: string; expiresAtMs: number }>>
  removeWatch(addr: string): Promise<boolean>
  clearExpiry(addr: string): Promise<void>
  /** prune-window guard: wipe all tx tracking, keep watches/tip/ring; returns lost txids */
  clearTracking(): Promise<string[]>
}

export interface BlockRpc extends ReorgRpc {
  getBlockHash(height: number): Promise<string>
  getBlockRaw(hash: string): Promise<Buffer>
  getRawMempool(): Promise<string[]>
  getBlockchainInfo(): Promise<{ chain: string; blocks: number; pruned: boolean; pruneheight?: number }>
}

export interface BlockPipelineDeps {
  cfg: { network: Network; confirmMilestones: number[]; maxMilestone: number; ringSize: number }
  store: BlockStore
  rpc: BlockRpc
  sink: { deliver(event: WeirEvent): Promise<boolean> }
  decodeBlock(raw: Buffer, network: Network): DecodedBlock
  log?: Log
}

/**
 * Same address-collection logic as src/engine/matcher.ts, inlined so this module has no
 * import on the matcher. An address matched by two outputs yields two entries.
 */
async function matchOutputs(
  tx: DecodedTx,
  store: { watchedSubset(addrs: string[]): Promise<string[]> },
): Promise<MatchedOutput[]> {
  const addrs = [...new Set(tx.outputs.map((o) => o.address).filter((a): a is string => a !== null))]
  if (addrs.length === 0) return []
  const watched = new Set(await store.watchedSubset(addrs))
  const matched: MatchedOutput[] = []
  for (const o of tx.outputs) {
    if (o.address !== null && watched.has(o.address)) {
      matched.push({ address: o.address, vout: o.vout, valueSats: o.valueSats })
    }
  }
  return matched
}

/**
 * Returns the block processor used by both the ZMQ path (via makeBlockHandler) and boot
 * catch-up (reconcile). Processes one raw block through the full pipeline sequence,
 * pulling any missed ancestor blocks over RPC first (gap/reorg handling), then resolves
 * whatever a reorg left in limbo.
 */
export function makeBlockProcessor(deps: BlockPipelineDeps): (raw: Buffer) => Promise<void> {
  const log = deps.log ?? consoleLog
  const { cfg, store, rpc, sink } = deps

  async function processConnected(block: DecodedBlock, height: number, isTip: boolean): Promise<void> {
    const net = cfg.network

    // ── 2. mined-watch promotion ─────────────────────────────────────────────────────
    // Check EVERY block tx against the matcher — not just pending ∩ block — so a payment
    // never seen in the mempool (missed ZMQ, direct-to-block) still confirms. Limbo txids
    // found in the block are reorg re-inclusions: fresh height/blockHash, fired resets,
    // milestones re-fire on the sweep below with new-blockhash idempotency keys.
    await store.setBlockTxids(block.txs.map((t) => t.txid))
    const pendingMined = new Set(await store.pendingInBlock())
    const limbo = new Set(await store.limboTxids())
    for (const tx of block.txs) {
      const matched = await matchOutputs(tx, store)
      if (matched.length === 0) {
        if (pendingMined.has(tx.txid) || limbo.has(tx.txid)) {
          // Was tracked but no longer matches (watch removed mid-flight): end tracking quietly.
          log.info(CTX, `tracked ${tx.txid} mined but no longer matches any watch — dropping tracking`)
          await store.removePending(tx.txid)
          await store.removeLimbo(tx.txid)
          await store.deleteRecord(tx.txid)
        }
        continue
      }
      // matched re-derived from the block's own decode (authoritative at mining time)
      const rec: MaturingRecord = {
        txid: tx.txid,
        height,
        blockHash: block.hash,
        matched,
        fired: [],
        hex: tx.hex,
      }
      if (limbo.has(tx.txid)) {
        await store.addMaturing(rec)
        await store.removeLimbo(tx.txid)
        log.info(CTX, `re-included ${tx.txid} after reorg → maturing at ${block.hash}@${height} (milestones re-fire)`)
      } else if (pendingMined.has(tx.txid)) {
        await store.removePending(tx.txid)
        await store.addMaturing(rec)
        log.info(CTX, `promoted pending ${tx.txid} → maturing at ${block.hash}@${height}`)
      } else {
        const existing = await store.getMaturing(tx.txid)
        if (existing && existing.height > 0) continue // already maturing (e.g. block replay)
        if (await store.isEvaluated(tx.txid)) continue
        await store.markEvaluated(tx.txid)
        await store.addMaturing(rec)
        log.info(CTX, `never-seen ${tx.txid} mined paying a watched address — maturing at ${block.hash}@${height}`)
      }
    }

    // ── 3. milestone sweep ───────────────────────────────────────────────────────────
    // Runs for EVERY block (including catch-up blocks) so confirmations count correctly.
    const blockTimeMs = block.time * 1000
    for (const entry of await store.maturingEntries()) {
      const rec = await store.getMaturing(entry.txid)
      if (!rec) {
        log.warn(CTX, `maturing entry ${entry.txid} has no record — removing dangling zset entry`)
        await store.removeMaturing(entry.txid)
        continue
      }
      const confs = height - rec.height + 1
      let fired = [...rec.fired]
      for (const m of cfg.confirmMilestones) {
        if (confs < m || fired.includes(m)) continue
        const ev: TxEvent = {
          version: 1,
          event: 'confirmed',
          network: net,
          txid: rec.txid,
          confs: m,
          matched: rec.matched,
          blockHeight: rec.height,
          blockHash: rec.blockHash,
          hex: rec.hex,
          idempotencyKey: idem.confirmed(net, rec.txid, m, rec.blockHash),
          timestamp: blockTimeMs,
        }
        const ok = await sink.deliver(ev)
        if (ok) {
          fired = [...fired, m].sort((a, b) => a - b)
          await store.setMaturingFired(rec.txid, fired)
        } else {
          // Do NOT add to fired — the milestone is retried on the next block's sweep.
          log.warn(CTX, `confirmed:${m} delivery failed for ${rec.txid} — will retry next block`)
        }
      }
      // Final removal only once confs ≥ maxMilestone AND every milestone has fired.
      if (confs >= cfg.maxMilestone && cfg.confirmMilestones.every((m) => fired.includes(m))) {
        await store.removeMaturing(rec.txid)
        await store.deleteRecord(rec.txid)
        log.info(CTX, `tracking ended for ${rec.txid} at ${confs} confs`)
      }
    }

    // ── 4./5. dropped check + TTL sweep — TIP BLOCKS ONLY ────────────────────────────
    // Both compare against live node state (mempool, wall clock), which is meaningless
    // for historical blocks during a catch-up walk: a pending tx mined in a LATER missed
    // block is absent from the live mempool and would be falsely reported dropped.
    if (isTip) {
      await store.replacePostBlockMempool(await rpc.getRawMempool())
      for (const txid of await store.droppedPending()) {
        const rec = await store.readRecord(txid)
        if (!rec) log.error(CTX, `dropped ${txid} has no seen-time record — emitting with empty matched/hex`)
        const ev: TxEvent = {
          version: 1,
          event: 'dropped',
          network: net,
          txid,
          confs: 0,
          matched: rec?.matched ?? [],
          blockHeight: null,
          blockHash: null,
          hex: rec?.hex ?? '',
          idempotencyKey: idem.dropped(net, txid, height),
          timestamp: Date.now(),
        }
        const ok = await sink.deliver(ev)
        if (!ok) log.warn(CTX, `dropped delivery failed for ${txid} — best-effort one-shot, proceeding`)
        await store.removePending(txid)
        await store.unmarkEvaluated(txid) // a rebroadcast can legitimately fire `seen` again
        await store.deleteRecord(txid)
        log.info(CTX, `dropped ${txid} — left the mempool without confirming`)
      }

      const now = Date.now()
      for (const { address, expiresAtMs } of await store.dueExpiries(now)) {
        const ev: ExpiredEvent = {
          version: 1,
          event: 'expired',
          network: net,
          address,
          idempotencyKey: idem.expired(net, address, expiresAtMs),
          timestamp: now,
        }
        const ok = await sink.deliver(ev)
        if (!ok) log.warn(CTX, `expired delivery failed for ${address} — best-effort one-shot, proceeding`)
        await store.removeWatch(address)
        await store.clearExpiry(address)
        log.info(CTX, `watch expired: ${address}`)
      }
    }

    // ── 6. prune / tip / ring ────────────────────────────────────────────────────────
    if (isTip) await store.pruneEvaluated()
    await store.setTip({ hash: block.hash, height })
    await store.ringPut(height, block.hash)
    await store.ringPrune(cfg.ringSize)
    log.info(CTX, `processed block ${block.hash}@${height} (${block.txs.length} txs)`)
  }

  async function processOne(raw: Buffer, isTip: boolean): Promise<void> {
    const block = deps.decodeBlock(raw, cfg.network)
    const header = await rpc.getBlockHeader(block.hash)
    const height = header.height

    // ── 1. connectivity ──────────────────────────────────────────────────────────────
    const tip = await store.getTip()
    if (tip !== null) {
      if (tip.hash === block.hash) {
        log.info(CTX, `duplicate block ${block.hash}@${height} (already tip) — skipping`)
        return
      }
      if ((await store.ringHashAt(height)) === block.hash) {
        log.info(CTX, `already-processed block ${block.hash}@${height} (in ring) — skipping`)
        return
      }
      if (block.prevHash !== tip.hash) {
        // Gap or reorg — findForkPoint distinguishes them (a pure gap yields no disconnected).
        const { ancestorHeight, disconnected } = await findForkPoint(deps, block.prevHash, height)
        if (disconnected.length > 0) {
          log.warn(
            CTX,
            `reorg detected at incoming ${block.hash}@${height}: fork point height=${ancestorHeight}, ` +
              `${disconnected.length} block(s) disconnected`,
          )
          // Limbo + rewind: after this, the replacement chain is a plain connected walk.
          await enterLimboAndRewind(deps, ancestorHeight)
        } else {
          log.info(CTX, `gap: tip ${tip.hash}@${tip.height}, incoming @${height} — catching up ${ancestorHeight + 1}..${height - 1}`)
        }

        const walkFrom = ancestorHeight + 1
        if (walkFrom < height) {
          // Prune-window guard: a pruned node cannot serve blocks below its prune height.
          // Without this, downtime longer than the prune window becomes a permanent
          // crash-loop (getblock error → crash → restart → same error).
          const info = await rpc.getBlockchainInfo()
          const pruneHeight = info.pruned ? (info.pruneheight ?? 0) : 0
          if (walkFrom < pruneHeight) {
            const lost = await store.clearTracking()
            log.error(
              CTX,
              `downtime exceeded the node's prune window (need block ${walkFrom}, node pruned below ${pruneHeight}) — ` +
                `catch-up is impossible. Re-initializing forward-only from ${block.hash}@${height}. ` +
                `TRACKING LOST for ${lost.length} in-flight tx(s)${lost.length > 0 ? `: ${lost.join(', ')}` : ''}. ` +
                'Watches are unaffected; still-unconfirmed txs re-fire `seen` via the mempool reparse.',
            )
            await store.ringPut(height - 1, block.prevHash)
            await store.setTip({ hash: block.prevHash, height: height - 1 })
          } else {
            for (let missed = walkFrom; missed < height; missed++) {
              const missedHash = await rpc.getBlockHash(missed)
              const missedRaw = await rpc.getBlockRaw(missedHash)
              await processOne(missedRaw, false)
            }
          }
        }
      }
    } else {
      log.info(CTX, `first run — processing ${block.hash}@${height} standalone`)
    }

    await processConnected(block, height, isTip)
  }

  return async function processBlock(raw: Buffer): Promise<void> {
    await processOne(raw, true)
    // Whatever a reorg displaced and the new chain did not re-include gets adjudicated
    // now (mempool → demoted, gone → conflicted). No-op when limbo is empty. Persisted
    // limbo means a crash anywhere above simply re-resolves on the next block or boot.
    await resolveLimbo(deps)
  }
}

/**
 * ZMQ-facing handler: the processor behind a serialization queue so blocks arriving
 * back-to-back are processed strictly in order. The returned promise still rejects on
 * failure (the ZMQ wrapper logs it); the internal chain is kept alive so one failed
 * block does not poison processing of the next.
 */
export function makeBlockHandler(deps: BlockPipelineDeps): (raw: Buffer) => Promise<void> {
  const processBlock = makeBlockProcessor(deps)
  let queue: Promise<void> = Promise.resolve()
  return (raw: Buffer) => {
    const run = queue.then(() => processBlock(raw))
    queue = run.catch(() => undefined) // error surfaces via `run`; chain stays usable
    return run
  }
}
