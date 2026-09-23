/**
 * Block pipeline: the ordered per-block sequence — connectivity (gap/reorg) → READS (pending,
 * limbo, claimants, watch set, maturing records) → every transition computed in memory →
 * ONE MULTI (`applyBlock`: promotions, milestones, finished, replaced, proven conflicts,
 * ended, tip, ring) → then, only when the block is the node's CURRENT tip, the tip-only work:
 * eviction check, TTL sweep, `evaluated` prune, limbo resolution.
 * Spec: docs/DESIGN.md "src/engine/blockPipeline.ts" and "Outpoint tracking" rule 4.
 *
 * INVARIANTS
 * - ONE WRITER: this runs inside the engine queue, so the reads at the top of a block are
 *   the state its MULTI acts on. No transition here re-checks anything.
 * - ONE MULTI PER BLOCK: a crash before exec leaves nothing of the block behind (tip
 *   included), and boot reconciliation replays it whole. A crash after exec means the tip
 *   moved with everything else, so a replay is a duplicate and is skipped.
 * - Reorgs use the limbo model (src/engine/reorg.ts): rewind first (one MULTI), then the
 *   replacement chain is a plain connected walk; re-inclusion is discovered by promotion.
 * - INPUT SCAN, before promotion: every block input is resolved against its claimant SET.
 *   EVERY claimant that is not the spender itself is a confirmed double-spend: a LIMBO
 *   owner → PROVEN `conflicted` (reason `double-spend`, conflictingTxid = spender), so
 *   `resolveLimbo` never sees it (no second verdict); a PENDING owner → `dropped` (reason
 *   `replaced`, replacedBy = spender); a MATURING owner outside limbo is impossible on a
 *   valid chain → error log, skipped. Running before promotion means a limbo tx can never be
 *   both re-included and conflicted.
 * - TIP-ONLY WORK compares against the node's LIVE state (mempool, wall clock), which is
 *   wrong for a block that is not the node's current tip: a catch-up block (a pending tx
 *   mined in a LATER missed block would be falsely evicted) or a queued burst during a
 *   reorg. So after the block's MULTI, `settleTip` snapshots the mempool, then asks
 *   getbestblockhash, and runs only when that is the stored tip — and every step, limbo
 *   resolution included, decides from THAT snapshot, never from a later probe. Boot calls
 *   the same `settleTip` after reconciling. A skipped run is picked up by the next tip
 *   block; each step is its own MULTI and safe to repeat.
 * - A tx's `maturing:{txid}` record exists from SEEN-time (height 0); the `maturing` ZSET
 *   only indexes MINED txs.
 * - There are NO delivery-failure branches: every event is enqueued in the same MULTI as
 *   its state change and the outbox drainer retries it. The pipeline never holds a sink.
 */
import type { DecodedBlock, ExpiredEvent, MaturingRecord, Network, Outpoint, TxEvent } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { BlockWrites, Store } from '../store/redis'
import { idem, outpointField } from '../store/keys'
import { log } from '../lib/log'
import { metrics } from '../lib/metrics'
import { matchAgainst } from './matcher'
import { enterLimboAndRewind, findForkPoint, type ReorgRpc, type ReorgStore, resolveLimbo } from './reorg'

const CTX = 'blockPipeline'

/** SMISMEMBER argument cap per round trip when resolving a block's distinct output addresses. */
const WATCHED_SUBSET_CHUNK = 1000

export type BlockStore = ReorgStore &
  Pick<
    Store,
    | 'getTip'
    | 'pendingTxids'
    | 'watchedSubset'
    | 'outpointOwners'
    | 'applyBlock'
    | 'dropPending'
    | 'evaluatedTxids'
    | 'forgetEvaluated'
    | 'dueExpiries'
    | 'expireWatch'
    | 'resetTracking'
  >

export type BlockRpc = ReorgRpc & Pick<Rpc, 'getBlockHash' | 'getBlockRaw' | 'getRawMempool' | 'getBlockchainInfo' | 'getBestBlockHash'>

export interface BlockPipelineDeps {
  cfg: { network: Network; confirmMilestones: number[]; maxMilestone: number; ringSize: number }
  store: BlockStore
  rpc: BlockRpc
  decodeBlock(raw: Buffer, network: Network): DecodedBlock
}

export interface BlockPipeline {
  /** one raw block through the full sequence, pulling any missed ancestor blocks over RPC first (gap/reorg handling) */
  processBlock(raw: Buffer): Promise<void>
  /**
   * The tip-only work for the STORED tip, if the node agrees it is the tip: snapshot the
   * mempool, then getbestblockhash; equal → eviction, TTL, evaluated prune, limbo
   * resolution from that snapshot, resolves true. Otherwise nothing is written and it
   * resolves false (the node moved on: a newer block will settle). No tip yet → true.
   */
  settleTip(): Promise<boolean>
}

/** The block processor alone — what the ZMQ path queues. */
export function makeBlockProcessor(deps: BlockPipelineDeps): (raw: Buffer) => Promise<void> {
  return makeBlockPipeline(deps).processBlock
}

export function makeBlockPipeline(deps: BlockPipelineDeps): BlockPipeline {
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
   * resolved against its claimant SET; EVERY claimant ≠ its spender adjudicated once (the
   * first spending tx wins when several inputs of one claimant are spent). Fills
   * `writes.conflicted` / `writes.dropped` and removes those txids from the local snapshots.
   */
  async function scanInputs(block: DecodedBlock, height: number, pending: Set<string>, limbo: Set<string>, writes: BlockWrites): Promise<void> {
    const net = cfg.network
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
    if (inputs.length === 0) return
    const owners = await store.outpointOwners(inputs)
    // claimant → the block txid that spent (one of) its inputs
    const spentOwner = new Map<string, string>()
    for (const [field, claimants] of owners) {
      const spender = spenderOf.get(field)!
      for (const owner of claimants) if (spender !== owner && !spentOwner.has(owner)) spentOwner.set(owner, spender)
    }

    for (const [owner, spender] of spentOwner) {
      const rec = await store.readRecord(owner)
      if (!rec) {
        log.info(CTX, `${spender} in ${block.hash} spends an input of ${owner}, whose record is gone (already replaced or dropped) — nothing to do`)
        continue
      }
      if (limbo.has(owner)) {
        // PROVEN conflict: the new chain spent one of a displaced tx's inputs. Terminal.
        const lastDepth = rec.fired.length > 0 ? Math.max(...rec.fired) : 0
        const event: TxEvent = {
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
        writes.conflicted.push({ rec, event })
        limbo.delete(owner)
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
      const event: TxEvent = {
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
      writes.dropped.push({ rec, event })
      pending.delete(owner)
      log.info(CTX, `dropped ${owner} — replaced by ${spender} confirmed in ${block.hash}@${height}`)
    }
  }

  /** The tip-only work at the stored tip `{hash, height}` — each step its own MULTI, each safe to repeat at the next tip block. */
  async function tipWork(hash: string, height: number, inMempool: ReadonlySet<string>): Promise<void> {
    const net = cfg.network

    // Eviction check: a pending tx absent from the live mempool vanished without being
    // mined. Replacements were caught by the input scans (mempool path or the block's), so
    // what is left here went for another reason: the residual verdict is `evicted`.
    for (const txid of await store.pendingTxids()) {
      if (inMempool.has(txid)) continue
      const rec = await store.readRecord(txid)
      if (!rec) log.error(CTX, `dropped ${txid} has no seen-time record — emitting with empty matched/hex`)
      const event: TxEvent = {
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
      await store.dropPending({ rec: { txid, inputs: rec?.inputs ?? [] }, event })
      log.info(CTX, `dropped ${txid} — left the mempool without confirming`)
    }

    // TTL sweep: a watch's lifetime ended.
    const now = Date.now()
    for (const { address, expiresAtMs } of await store.dueExpiries(now)) {
      const event: ExpiredEvent = {
        version: 1,
        event: 'expired',
        network: net,
        address,
        idempotencyKey: idem.expired(net, address, expiresAtMs),
        timestamp: now,
      }
      await store.expireWatch(address, event)
      log.info(CTX, `watch expired: ${address}`)
    }

    // The reparse dedupe list forgets what left the mempool (mined or gone): a new mempool epoch.
    await store.forgetEvaluated((await store.evaluatedTxids()).filter((t) => !inMempool.has(t)))

    // Whatever a reorg displaced and the new chain did not re-include gets adjudicated now,
    // from the same snapshot (present → demoted, absent → conflicted). No-op when limbo is empty.
    await resolveLimbo(deps, inMempool)
    log.info(CTX, `tip work done at ${hash}@${height}`)
  }

  async function settleTip(): Promise<boolean> {
    const tip = await store.getTip()
    if (tip === null) return true
    const mempool = new Set(await rpc.getRawMempool()) // snapshot BEFORE asking which block is best:
    const best = await rpc.getBestBlockHash() // if best is still the tip, the snapshot belongs to it
    if (best !== tip.hash) {
      log.info(CTX, `${tip.hash}@${tip.height} is not the node's tip (best ${best}) — tip work deferred to the next block`)
      return false
    }
    await tipWork(tip.hash, tip.height, mempool)
    return true
  }

  async function processConnected(block: DecodedBlock, height: number, isTip: boolean): Promise<void> {
    const net = cfg.network
    const blockTimeMs = block.time * 1000
    const writes: BlockWrites = {
      promoted: [],
      fired: [],
      finished: [],
      dropped: [],
      conflicted: [],
      ended: [],
      unindexed: [],
      tip: { hash: block.hash, height },
      ringKeep: cfg.ringSize,
    }

    // ── reads + the input scan ───────────────────────────────────────────────────────
    const pending = new Set(await store.pendingTxids())
    const limbo = new Set(await store.limboTxids())
    await scanInputs(block, height, pending, limbo, writes)

    // ── promotion ────────────────────────────────────────────────────────────────────
    // Check EVERY block tx against the watch set — not just pending ∩ block — so a payment
    // never seen in the mempool (missed ZMQ, direct-to-block) still confirms. Limbo txids
    // found in the block are reorg re-inclusions: fresh height/blockHash, fired resets,
    // milestones re-fire on the sweep below with new-blockhash idempotency keys.
    const watched = await watchedInBlock(block)
    for (const tx of block.txs) {
      const matched = matchAgainst(tx, watched)
      if (matched.length === 0) {
        if (pending.has(tx.txid) || limbo.has(tx.txid)) {
          // Was tracked but no longer matches (watch removed mid-flight): end tracking quietly.
          log.info(CTX, `tracked ${tx.txid} mined but no longer matches any watch — dropping tracking`)
          const rec = await store.readRecord(tx.txid)
          writes.ended.push({ txid: tx.txid, inputs: rec?.inputs ?? [] })
        }
        continue
      }
      // matched re-derived from the block's own decode (authoritative at mining time)
      const rec: MaturingRecord = { txid: tx.txid, height, blockHash: block.hash, matched, fired: [], hex: tx.hex, inputs: tx.inputs }
      if (limbo.has(tx.txid)) log.info(CTX, `re-included ${tx.txid} after reorg → maturing at ${block.hash}@${height} (milestones re-fire)`)
      else if (pending.has(tx.txid)) log.info(CTX, `promoted pending ${tx.txid} → maturing at ${block.hash}@${height}`)
      else log.info(CTX, `never-seen ${tx.txid} mined paying a watched address — maturing at ${block.hash}@${height}`)
      writes.promoted.push(rec)
    }

    // ── milestone sweep ──────────────────────────────────────────────────────────────
    // Every maturing record (the index, plus what this block promotes) — runs for EVERY
    // block, catch-up blocks included, so confirmations count correctly.
    const records = new Map<string, MaturingRecord>()
    for (const entry of await store.maturingEntries()) {
      const rec = await store.readRecord(entry.txid)
      if (!rec) {
        log.warn(CTX, `maturing entry ${entry.txid} has no record — removing dangling zset entry`)
        writes.unindexed.push(entry.txid)
        continue
      }
      records.set(entry.txid, rec)
    }
    for (const rec of writes.promoted) records.set(rec.txid, rec)
    for (const rec of records.values()) {
      const confs = height - rec.height + 1
      const fired = [...rec.fired]
      for (const m of cfg.confirmMilestones) {
        if (confs < m || fired.includes(m)) continue
        const event: TxEvent = {
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
        writes.fired.push({ txid: rec.txid, fired: [...fired], event })
      }
      // Final removal once confs ≥ maxMilestone AND every milestone has been enqueued.
      if (confs >= cfg.maxMilestone && cfg.confirmMilestones.every((m) => fired.includes(m))) {
        writes.finished.push(rec)
        log.info(CTX, `tracking ended for ${rec.txid} at ${confs} confs`)
      }
    }

    // ── the block's ONE MULTI ────────────────────────────────────────────────────────
    await store.applyBlock(writes)
    metrics.counters.inc('weir_blocks_processed_total')
    log.info(CTX, `processed block ${block.hash}@${height} (${block.txs.length} txs)`)

    // ── tip-only work, only at the node's real tip ───────────────────────────────────
    if (isTip) await settleTip()
  }

  async function processOne(raw: Buffer, isTip: boolean): Promise<void> {
    // A ZMQ duplicate or an old notification: the block is the tip or already in the ring.
    // (A stored tip that the node no longer has is boot's case: reconcile rewinds to the
    // node's chain BEFORE handing it a block — see src/boot/reconcile.ts.)
    const block = deps.decodeBlock(raw, cfg.network)
    const header = await rpc.getBlockHeader(block.hash)
    const height = header.height

    // ── connectivity ─────────────────────────────────────────────────────────────────
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
          metrics.counters.inc('weir_reorgs_total')
          log.warn(
            CTX,
            `reorg detected at incoming ${block.hash}@${height}: fork point height=${ancestorHeight}, ` +
              `${disconnected.length} block(s) disconnected`,
          )
          // Limbo + rewind (one MULTI): after this, the replacement chain is a plain connected walk.
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
            const lost = await store.resetTracking({ hash: block.prevHash, height: height - 1 })
            log.error(
              CTX,
              `downtime exceeded the node's prune window (need block ${walkFrom}, node pruned below ${pruneHeight}) — ` +
                `catch-up is impossible. Re-initializing forward-only from ${block.hash}@${height}. ` +
                `TRACKING LOST for ${lost.length} in-flight tx(s)${lost.length > 0 ? `: ${lost.join(', ')}` : ''}. ` +
                'Watches are unaffected; still-unconfirmed txs re-fire `seen` via the mempool reparse.',
            )
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

  return { processBlock: (raw: Buffer) => processOne(raw, true), settleTip }
}
