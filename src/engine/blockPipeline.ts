/**
 * Block pipeline: the ordered per-block sequence — connectivity (gap/reorg) → input scan
 * (replacements + proven conflicts) → mined-watch promotion → milestone sweep → dropped
 * check → TTL sweep → prune/tip/ring, then limbo resolution once the TIP block is done.
 * Spec: docs/DESIGN.md "src/engine/blockPipeline.ts" and "Outpoint tracking" rule 4.
 *
 * INVARIANTS
 * - Reorgs use the limbo model (src/engine/reorg.ts): rewind first, then the replacement
 *   chain is a plain connected walk; re-inclusion is discovered by the promotion step.
 * - INPUT SCAN, before promotion: every block input is resolved against its claimant SET
 *   (`outpointOwners`, pipelined SMEMBERS). EVERY claimant that is not the spender itself is
 *   a confirmed double-spend:
 *   a LIMBO owner → PROVEN `conflict` (reason `double-spend`, conflictingTxid = spender) —
 *   it leaves limbo here, so `resolveLimbo` never sees it (no second verdict); any other
 *   owner → `replacePending` (dropped, reason `replaced`, replacedBy = spender), a guarded
 *   Lua that decides for itself whether the owner is still pending and unmined — no decision
 *   is taken from the read here (the record is read only to build the payload); a MATURING
 *   owner outside limbo is impossible on a valid chain → error log, skipped. Running before
 *   promotion means a limbo tx can never be both re-included and conflicted.
 * - The dropped check, TTL sweep and evaluated-prune run ONLY on the tip block: they compare
 *   against the node's LIVE mempool / wall clock, which is meaningless for historical blocks
 *   during a catch-up walk (a pending tx mined in a LATER missed block would be falsely
 *   reported dropped).
 * - A tx's `maturing:{txid}` record exists from SEEN-time (height 0); the `maturing` ZSET
 *   only indexes MINED txs.
 * - There are NO delivery-failure branches: every event is enqueued in the same store
 *   transition as its state change (markFired, dropPending, expireWatch, demoteToPending,
 *   conflict) and the outbox drainer retries it. The pipeline never holds a sink.
 * - RETIREMENT WATERMARK: a drop/replacement records its exit time in `retired`; a duplicate
 *   evaluation that STARTED before that exit cannot resurrect the txid (recordSeen refuses
 *   it), while a later evaluation — a real rebroadcast — records normally. Pruned with the
 *   tombstones. Drops never tombstone: a dropped-then-mined tx still confirms here.
 * - TOMBSTONES: when tracking ENDS (final milestone → `finishMaturing`; `conflict`) the txid
 *   is tombstoned for TOMBSTONE_TTL_MS so a stale mempool evaluation (an RPC that read the
 *   tx before the block and returned after the cleanup) cannot resurrect it — the next block
 *   would otherwise report a false `dropped` for a payment that confirmed. The never-seen
 *   promotion branch skips tombstoned txids for the same reason. `dropPending` never
 *   tombstones: a rebroadcast may legitimately re-fire `seen`. Pruned on every tip block.
 */
import type { DecodedBlock, ExpiredEvent, MaturingRecord, Network, Outpoint, TxEvent } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import { idem, outpointField } from '../store/keys'
import { fatal, log } from '../lib/log'
import { matchAgainst } from './matcher'
import { enterLimboAndRewind, findForkPoint, type ReorgRpc, type ReorgStore, resolveLimbo } from './reorg'

const CTX = 'blockPipeline'

/** SMISMEMBER argument cap per round trip when resolving a block's distinct output addresses. */
const WATCHED_SUBSET_CHUNK = 1000

/**
 * How long an ended txid stays tombstoned. The stale window it guards against is one RPC
 * round trip (getrawtransaction issued before the block, answered after the cleanup); an
 * hour is generous. A constant, not config.
 */
export const TOMBSTONE_TTL_MS = 3_600_000

export type BlockStore = ReorgStore &
  Pick<
    Store,
    | 'getTip'
    | 'ringPut'
    | 'ringPrune'
    | 'setBlockTxids'
    | 'pendingInBlock'
    | 'watchedSubset'
    | 'outpointOwners'
    | 'replacePending'
    | 'promoteToMaturing'
    | 'markFired'
    | 'removeMaturing'
    | 'finishMaturing'
    | 'isTombstoned'
    | 'pruneTombstones'
    | 'pruneRetired'
    | 'replacePostBlockMempool'
    | 'droppedPending'
    | 'dropPending'
    | 'pruneEvaluated'
    | 'dueExpiries'
    | 'expireWatch'
    | 'endTracking'
    | 'clearTracking'
  >

export type BlockRpc = ReorgRpc & Pick<Rpc, 'getBlockHash' | 'getBlockRaw' | 'getRawMempool' | 'getBlockchainInfo'>

export interface BlockPipelineDeps {
  cfg: { network: Network; confirmMilestones: number[]; maxMilestone: number; ringSize: number }
  store: BlockStore
  rpc: BlockRpc
  decodeBlock(raw: Buffer, network: Network): DecodedBlock
}

/**
 * Returns the block processor used by both the ZMQ path (via makeBlockHandler) and boot
 * catch-up (reconcile). Processes one raw block through the full pipeline sequence,
 * pulling any missed ancestor blocks over RPC first (gap/reorg handling), then resolves
 * whatever a reorg left in limbo.
 */
export function makeBlockProcessor(deps: BlockPipelineDeps): (raw: Buffer) => Promise<void> {
  const { cfg, store, rpc } = deps

  /** The block's distinct output addresses that are watched — one round trip per chunk. */
  async function watchedInBlock(block: DecodedBlock): Promise<Set<string>> {
    const addrs = new Set<string>()
    for (const tx of block.txs) {
      for (const o of tx.outputs) if (o.address !== null) addrs.add(o.address)
    }
    const all = [...addrs]
    const watched = new Set<string>()
    for (let i = 0; i < all.length; i += WATCHED_SUBSET_CHUNK) {
      for (const a of await store.watchedSubset(all.slice(i, i + WATCHED_SUBSET_CHUNK))) watched.add(a)
    }
    return watched
  }

  /**
   * Rule 4, the block-wide input scan: every input of every block tx (coinbase has none)
   * resolved against its claimant SET (`outpointOwners`, chunked pipelining); EVERY claimant
   * ≠ its spender adjudicated once (the first spending tx wins when several inputs of one
   * claimant are spent). Returns the limbo txids that were proven conflicted so the caller's
   * snapshot forgets them.
   */
  async function scanInputs(block: DecodedBlock, height: number, limbo: ReadonlySet<string>): Promise<string[]> {
    const net = cfg.network
    const startedAtMs = Date.now() // the block path is serialized: the replacePending fence is formal here
    const blockTimeMs = block.time * 1000
    const inputs: Outpoint[] = []
    const spenderOf = new Map<string, string>()
    for (const tx of block.txs) {
      for (const o of tx.inputs) {
        const field = outpointField(o)
        if (spenderOf.has(field)) continue // a valid block never spends one prevout twice
        spenderOf.set(field, tx.txid)
        inputs.push(o)
      }
    }
    if (inputs.length === 0) return []
    const owners = await store.outpointOwners(inputs)
    // claimant → the block txid that spent (one of) its inputs
    const spentOwner = new Map<string, string>()
    for (const [field, claimants] of owners) {
      const spender = spenderOf.get(field)!
      for (const owner of claimants) if (spender !== owner && !spentOwner.has(owner)) spentOwner.set(owner, spender)
    }

    const conflicted: string[] = []
    for (const [owner, spender] of spentOwner) {
      // The record only feeds the payload; whether there is anything to replace is decided
      // atomically inside replacePending.
      const rec = await store.readRecord(owner)
      if (!rec) {
        log.info(CTX, `${spender} in ${block.hash} spends an input of ${owner}, whose record is gone (already replaced or dropped) — nothing to do`)
        continue
      }
      if (limbo.has(owner)) {
        // PROVEN conflict: the new chain spent one of a displaced tx's inputs. Terminal.
        const lastDepth = rec.fired.length > 0 ? Math.max(...rec.fired) : 0
        const ev: TxEvent = {
          version: 1,
          event: 'conflicted',
          network: net,
          txid: owner,
          confs: lastDepth,
          matched: rec.matched,
          blockHeight: rec.height,
          blockHash: rec.blockHash,
          hex: rec.hex,
          reason: 'double-spend',
          conflictingTxid: spender,
          idempotencyKey: idem.conflicted(net, owner),
          timestamp: blockTimeMs,
        }
        await store.conflict(owner, ev) // full cleanup + SREM limbo + enqueue, one Lua
        conflicted.push(owner)
        log.info(CTX, `conflicted ${owner} (was ${rec.blockHash}@${rec.height}) — input double-spent by ${spender} in ${block.hash}@${height}`)
        continue
      }
      if (rec.height > 0) {
        log.error(
          CTX,
          `${spender} in ${block.hash} spends an input of ${owner}, which is maturing at ${rec.blockHash}@${rec.height} and not in limbo — ` +
            'impossible on a valid chain; skipping',
        )
        continue
      }
      // A double-spend confirmed while ours sat in the mempool.
      const ev: TxEvent = {
        version: 1,
        event: 'dropped',
        network: net,
        txid: owner,
        confs: 0,
        matched: rec.matched,
        blockHeight: null,
        blockHash: null,
        hex: rec.hex,
        reason: 'replaced',
        replacedBy: spender,
        idempotencyKey: idem.replaced(net, owner, spender),
        timestamp: Date.now(),
      }
      const outcome = await store.replacePending(owner, ev, startedAtMs, spender)
      if (outcome === 'replaced') log.info(CTX, `dropped ${owner} — replaced by ${spender} confirmed in ${block.hash}@${height}`)
      else if (outcome === 'skipped') log.info(CTX, `${owner} was no longer pending when ${spender} in ${block.hash} was scanned (already replaced) — nothing to do`)
      else log.warn(CTX, `replacement of ${owner} by mined ${spender} refused (the spender is tombstoned within the TTL) — ${owner} is left to the tip-block dropped check`)
    }
    return conflicted
  }

  async function processConnected(block: DecodedBlock, height: number, isTip: boolean): Promise<void> {
    const net = cfg.network

    // ── 2. input scan, then mined-watch promotion ────────────────────────────────────
    // Check EVERY block tx against the watch set — not just pending ∩ block — so a payment
    // never seen in the mempool (missed ZMQ, direct-to-block) still confirms. Limbo txids
    // found in the block are reorg re-inclusions: fresh height/blockHash, fired resets,
    // milestones re-fire on the sweep below with new-blockhash idempotency keys.
    await store.setBlockTxids(block.txs.map((t) => t.txid))
    const pendingMined = new Set(await store.pendingInBlock())
    const limbo = new Set(await store.limboTxids())
    for (const gone of await scanInputs(block, height, limbo)) limbo.delete(gone)
    const watched = await watchedInBlock(block)
    for (const tx of block.txs) {
      const matched = matchAgainst(tx, watched)
      if (matched.length === 0) {
        if (pendingMined.has(tx.txid) || limbo.has(tx.txid)) {
          // Was tracked but no longer matches (watch removed mid-flight): end tracking quietly.
          log.info(CTX, `tracked ${tx.txid} mined but no longer matches any watch — dropping tracking`)
          await store.endTracking(tx.txid)
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
        inputs: tx.inputs,
      }
      // One code path for all three origins: promoteToMaturing is a single MULTI, so a
      // crash can never leave a tx half-promoted. The `evaluated` flag is deliberately NOT
      // a gate here: it only means "the mempool evaluator looked at it once" (possibly
      // before the address was watched, or before a crash lost the record).
      if (limbo.has(tx.txid)) {
        log.info(CTX, `re-included ${tx.txid} after reorg → maturing at ${block.hash}@${height} (milestones re-fire)`)
      } else if (pendingMined.has(tx.txid)) {
        log.info(CTX, `promoted pending ${tx.txid} → maturing at ${block.hash}@${height}`)
      } else {
        const existing = await store.readRecord(tx.txid)
        if (existing && existing.height > 0) continue // already maturing (e.g. block replay)
        if (await store.isTombstoned(tx.txid)) {
          // Tracking already ended for this txid (final milestone / conflicted) within the
          // tombstone TTL: a block replay, or a reorg at exactly max-milestone depth. Not a
          // new payment — do not restart tracking.
          log.info(CTX, `${tx.txid} mined at ${block.hash}@${height} but its tracking already ended — skipping`)
          continue
        }
        log.info(CTX, `never-seen ${tx.txid} mined paying a watched address — maturing at ${block.hash}@${height}`)
      }
      await store.promoteToMaturing(rec)
    }

    // ── 3. milestone sweep ───────────────────────────────────────────────────────────
    // Runs for EVERY block (including catch-up blocks) so confirmations count correctly.
    const blockTimeMs = block.time * 1000
    for (const entry of await store.maturingEntries()) {
      const rec = await store.readRecord(entry.txid)
      if (!rec) {
        log.warn(CTX, `maturing entry ${entry.txid} has no record — removing dangling zset entry`)
        await store.removeMaturing(entry.txid)
        continue
      }
      const confs = height - rec.height + 1
      const fired = [...rec.fired]
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
        // fired is recorded at ENQUEUE time — the outbox owns delivery retry. Kept sorted.
        fired.push(m)
        fired.sort((a, b) => a - b)
        await store.markFired(rec.txid, [...fired], ev)
      }
      // Final removal only once confs ≥ maxMilestone AND every milestone has been enqueued:
      // record + index gone, txid tombstoned (one MULTI).
      if (confs >= cfg.maxMilestone && cfg.confirmMilestones.every((m) => fired.includes(m))) {
        await store.finishMaturing(rec.txid, Date.now())
        log.info(CTX, `tracking ended for ${rec.txid} at ${confs} confs`)
      }
    }

    // ── 4./5. dropped check + TTL sweep — TIP BLOCKS ONLY ────────────────────────────
    // Both compare against live node state (mempool, wall clock), which is meaningless
    // for historical blocks during a catch-up walk: a pending tx mined in a LATER missed
    // block is absent from the live mempool and would be falsely reported dropped.
    // Replacements were caught by the input scan (mempool path or above), so what is left
    // here vanished for another reason: the residual verdict is `evicted`.
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
          reason: 'evicted',
          idempotencyKey: idem.dropped(net, txid, height),
          timestamp: Date.now(),
        }
        // One GUARDED Lua: pending, evaluated (a rebroadcast can legitimately fire `seen`
        // again), record, outpoints it still owns, + `dropped` enqueued — or nothing, when a
        // mempool replacement already handled the txid since droppedPending() was read.
        if (await store.dropPending(txid, ev)) log.info(CTX, `dropped ${txid} — left the mempool without confirming`)
        else log.info(CTX, `${txid} left pending while the dropped check ran (already replaced) — nothing to do`)
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
        await store.expireWatch(address, ev) // SREM addresses + ZREM expiries + `expired`, one MULTI
        log.info(CTX, `watch expired: ${address}`)
      }
    }

    // ── 6. prune / tip / ring ────────────────────────────────────────────────────────
    if (isTip) {
      await store.pruneEvaluated()
      await store.pruneTombstones(Date.now() - TOMBSTONE_TTL_MS)
      await store.pruneRetired(Date.now() - TOMBSTONE_TTL_MS)
    }
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
 * ZMQ-facing handler: the given processor behind a serialization queue so blocks arriving
 * back-to-back are processed strictly in order. A block that fails to process is an
 * UNEXPECTED error → `fatal` (the process exits; docker restarts it and boot reconciliation
 * replays the block). There is deliberately no "keep the queue alive" path: skipping a
 * failed block and processing the next would silently lose confirmations.
 * `onFatal` is injectable for tests only; production callers use the default.
 */
export function makeBlockHandler(
  processBlock: (raw: Buffer) => Promise<void>,
  onFatal: (ctx: string, err: unknown) => void = fatal,
): (raw: Buffer) => Promise<void> {
  let queue: Promise<void> = Promise.resolve()
  return (raw: Buffer) => {
    queue = queue.then(() => processBlock(raw)).catch((err: unknown) => onFatal(CTX, err))
    return queue
  }
}
