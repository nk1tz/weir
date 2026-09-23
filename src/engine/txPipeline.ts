import type { DecodedTx, MaturingRecord, Network, TxEvent } from '../lib/types'
import type { Drop, Store } from '../store/redis'
import { idem } from '../store/keys'
import { log } from '../lib/log'
import { matchTx } from './matcher'

const CTX = 'txPipeline'

/** Structural deps — tests pass in-memory fakes (tests/fakes.ts). No sink: events are enqueued. */
export interface TxEvaluatorDeps {
  store: Pick<Store, 'isEvaluated' | 'readRecord' | 'watchedSubset' | 'outpointOwners' | 'applyEvaluation'>
  cfg: { network: Network; seenEnabled: boolean }
}

export interface RawTxHandlerDeps extends TxEvaluatorDeps {
  decodeRawTx(raw: Buffer | string, network: Network): DecodedTx
}

/**
 * Evaluate one decoded transaction against the watch set — reads, then ONE MULTI
 * (`applyEvaluation`). Runs inside the engine queue, so nothing changes between the reads
 * and the write.
 *
 * - already evaluated → no-op (the reparse dedupe list).
 * - already tracked (a record exists: pending, maturing or limbo) → no-op. bitcoind
 *   re-publishes `rawtx` for every tx of a connected AND a disconnected block, so a tracked
 *   tx is sighted again; `seen` is the `unseen → pending` transition only.
 * - REPLACEMENT (DESIGN "Outpoint tracking" rule 3): every distinct claimant ≠ this txid of
 *   one of its inputs whose record is UNMINED was just replaced (RBF fee-bump, or a redirect
 *   to another address) → `dropped` with reason `replaced` / `replacedBy`, written in the
 *   same MULTI as this tx's own outcome. A MINED claimant is never touched from the mempool
 *   (bitcoind does not relay a conflict with a confirmed tx; during reorg lag the block path
 *   adjudicates it): warned, skipped.
 * - no matched outputs → marked evaluated only.
 * - matched → its seen-time record (height 0 / blockHash '' — a dropped tx cannot be
 *   re-fetched from a pruned node), pending, its claims, + `seen` (null when milestone 0 is
 *   not configured).
 */
export function makeTxEvaluator(deps: TxEvaluatorDeps): (tx: DecodedTx) => Promise<void> {
  const { store, cfg } = deps

  /** Rule 3: the `dropped` (replaced) transition for every unmined claimant of one of `tx`'s inputs. */
  async function replacedClaimants(tx: DecodedTx): Promise<Drop[]> {
    if (tx.inputs.length === 0) return []
    const owners = await store.outpointOwners(tx.inputs)
    const distinct = new Set<string>()
    for (const claimants of owners.values()) for (const owner of claimants) if (owner !== tx.txid) distinct.add(owner)
    const drops: Drop[] = []
    for (const owner of distinct) {
      const rec = await store.readRecord(owner)
      if (!rec) {
        log.info(CTX, `${tx.txid} spends an input of ${owner}, whose record is gone (already replaced or dropped) — nothing to replace`)
        continue
      }
      if (rec.height > 0) {
        log.warn(CTX, `${tx.txid} spends an input of mined tx ${owner} — a mempool tx cannot replace a confirmed one; ignoring`)
        continue
      }
      const event: TxEvent = {
        version: 1,
        event: 'dropped',
        network: cfg.network,
        txid: owner,
        confs: 0,
        matched: rec.matched,
        blockHeight: null,
        blockHash: null,
        hex: rec.hex,
        reason: 'replaced',
        replacedBy: tx.txid,
        idempotencyKey: idem.replaced(cfg.network, owner, tx.txid),
        timestamp: Date.now(),
      }
      drops.push({ rec, event })
    }
    return drops
  }

  return async function evaluateTx(tx: DecodedTx): Promise<void> {
    if (await store.isEvaluated(tx.txid)) return
    if ((await store.readRecord(tx.txid)) !== null) {
      log.info(CTX, `${tx.txid} is already tracked (mined, or displaced by a reorg) — a repeated sighting, nothing to do`)
      return
    }

    const dropped = await replacedClaimants(tx)
    const matched = await matchTx(tx, store)
    let seen: { rec: MaturingRecord; event: TxEvent | null } | null = null
    if (matched.length > 0) {
      const event: TxEvent | null = cfg.seenEnabled
        ? {
            version: 1,
            event: 'seen',
            network: cfg.network,
            txid: tx.txid,
            confs: 0,
            matched,
            idempotencyKey: idem.seen(cfg.network, tx.txid),
            timestamp: Date.now(),
            blockHeight: null,
            blockHash: null,
            hex: tx.hex,
          }
        : null
      seen = { rec: { txid: tx.txid, height: 0, blockHash: '', matched, fired: [], hex: tx.hex, inputs: tx.inputs }, event }
    }

    await store.applyEvaluation({ txid: tx.txid, dropped, seen })
    for (const d of dropped) log.info(CTX, `dropped ${d.rec.txid} — replaced by ${tx.txid} (input conflict)`)
    if (seen !== null) log.info(CTX, `seen ${tx.txid} paying ${matched.length} watched output(s)`)
  }
}

/** ZMQ rawtx path: decode → evaluate. */
export function makeRawTxHandler(deps: RawTxHandlerDeps): (raw: Buffer) => Promise<void> {
  const evaluate = makeTxEvaluator(deps)
  return async function handleRawTx(raw: Buffer): Promise<void> {
    await evaluate(deps.decodeRawTx(raw, deps.cfg.network))
  }
}
