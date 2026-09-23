import type { DecodedTx, Network, TxEvent } from '../lib/types'
import { MAX_EVALUATION_AGE_MS, type Store } from '../store/redis'
import { idem } from '../store/keys'
import { log } from '../lib/log'
import { matchTx } from './matcher'

const CTX = 'txPipeline'

/** Structural deps — tests pass in-memory fakes (tests/fakes.ts). No sink: events are enqueued. */
export interface TxEvaluatorDeps {
  store: Pick<Store, 'isEvaluated' | 'markEvaluated' | 'watchedSubset' | 'recordSeen' | 'outpointOwners' | 'readRecord' | 'replacePending'>
  cfg: { network: Network; seenEnabled: boolean }
}

export interface RawTxHandlerDeps extends TxEvaluatorDeps {
  decodeRawTx(raw: Buffer | string, network: Network): DecodedTx
}

/**
 * Evaluate one decoded transaction against the watch set — ONE pass (DESIGN "Outpoint
 * tracking" rule 3):
 *
 * - already evaluated → no-op. Then the FENCE: an evaluation older than
 *   MAX_EVALUATION_AGE_MS is refused before anything is mutated (warned, left un-evaluated).
 * - REPLACEMENT, before output matching: the tx's inputs are looked up (`outpointOwners`,
 *   one pipelined SMEMBERS per prevout); every distinct claimant ≠ this txid whose record is
 *   UNMINED was just replaced (RBF fee-bump, or a redirect to another address) →
 *   `replacePending` NOW: one fenced + guarded Lua that forgets it and enqueues `dropped`
 *   with reason `replaced`/`replacedBy` — or does nothing (false) when the claimant is no
 *   longer pending-unmined (the block pipeline mined it, or another caller replaced it). A
 *   MINED claimant (maturing/limbo) is never touched from the mempool (bitcoind does not
 *   relay a conflict with a confirmed tx; during reorg lag the block path adjudicates it):
 *   warned, skipped. Its record is read only to build the payload (gone → nothing to do).
 *   The Lua also refuses (`stale`) when the SPENDER itself is retired at an exit this
 *   evaluation predates, or tombstoned: an old evaluation of A must not replace the B that
 *   replaced A (rule 3).
 *   If ANY adjudication came back `stale` (the fence refused it inside the Lua), the whole
 *   evaluation stops WITHOUT marking the tx evaluated: the next reparse redoes it with a
 *   fresh view — otherwise an unwatched spender would be marked evaluated while the tx it
 *   replaced stays pending forever.
 * - no matched outputs → markEvaluated only, after the same age check (a view that aged past
 *   the fence during the pre-pass is not marked either).
 * - matched → `recordSeen(rec, event | null)`: ONE atomic store step that persists the
 *   seen-time record (height 0 / blockHash '' — a dropped tx cannot be re-fetched from a
 *   pruned node), marks the tx pending + evaluated, CLAIMS its inputs (SADD, never
 *   conflicts), and enqueues the `seen` event (null when milestone 0 is not configured).
 *   Nothing here awaits the network; the outbox delivers. `skipped` when a concurrent path
 *   (a block mining the tx, or a duplicate evaluation) got there first, `stale` past the
 *   fence — then there is nothing left to do. Documented imprecision: two spenders of one
 *   outpoint evaluated CONCURRENTLY can both see no claimant and both be recorded; the loser
 *   is reported `dropped`/`evicted` at the next tip check rather than `replaced`.
 *
 * `startedAtMs` is when this evaluation's view of the tx was taken: ZMQ receipt for the
 * rawtx path, the moment getrawtransaction was issued for the reparser. It is the fence's
 * clock — a response parked for longer than the tombstone TTL must never land.
 */
export function makeTxEvaluator(deps: TxEvaluatorDeps): (tx: DecodedTx, startedAtMs: number) => Promise<void> {
  const { store, cfg } = deps

  /**
   * Rule 3 for one claimant: build the `dropped` payload from its record and let the guarded
   * Lua decide. The record is gone (already replaced or dropped) or mined (warned: impossible
   * from the mempool) → nothing to do; otherwise `replacePending`. Returns whether the Lua
   * refused the adjudication as STALE — the caller then abandons the evaluation.
   */
  async function replaceOwner(owner: string, replacedBy: string, startedAtMs: number): Promise<{ stale: boolean }> {
    const rec = await store.readRecord(owner)
    if (!rec) {
      log.info(CTX, `${replacedBy} spends an input of ${owner}, whose record is gone (already replaced or dropped) — nothing to replace`)
      return { stale: false }
    }
    if (rec.height > 0) {
      log.warn(CTX, `${replacedBy} spends an input of mined tx ${owner} — a mempool tx cannot replace a confirmed one; ignoring`)
      return { stale: false }
    }
    const ev: TxEvent = {
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
      replacedBy,
      idempotencyKey: idem.replaced(cfg.network, owner, replacedBy),
      timestamp: Date.now(),
    }
    const outcome = await store.replacePending(owner, ev, startedAtMs, replacedBy)
    if (outcome === 'replaced') log.info(CTX, `dropped ${owner} — replaced by ${replacedBy} (input conflict)`)
    else if (outcome === 'skipped') log.info(CTX, `${owner} was no longer pending when ${replacedBy} tried to replace it (mined or already replaced) — nothing to do`)
    return { stale: outcome === 'stale' }
  }

  /** Rule 3: replace every unmined claimant of one of `tx`'s inputs. True when any adjudication was refused as stale. */
  async function replaceConflictingPending(tx: DecodedTx, startedAtMs: number): Promise<boolean> {
    if (tx.inputs.length === 0) return false
    const owners = await store.outpointOwners(tx.inputs)
    const distinct = new Set<string>()
    for (const claimants of owners.values()) for (const owner of claimants) if (owner !== tx.txid) distinct.add(owner)
    let anyStale = false
    for (const owner of distinct) if ((await replaceOwner(owner, tx.txid, startedAtMs)).stale) anyStale = true
    return anyStale
  }

  return async function evaluateTx(tx: DecodedTx, startedAtMs: number): Promise<void> {
    if (await store.isEvaluated(tx.txid)) return

    // The fence, applied before anything is mutated: a view of the mempool this old must
    // not replace a pending tx (replacePending and recordSeen re-check it atomically).
    const ageMs = Date.now() - startedAtMs
    if (ageMs > MAX_EVALUATION_AGE_MS) {
      log.warn(CTX, `refused stale evaluation of ${tx.txid}: started ${ageMs}ms ago (fence ${MAX_EVALUATION_AGE_MS}ms) — left un-evaluated for the next reparse`)
      return
    }

    if (await replaceConflictingPending(tx, startedAtMs)) {
      log.warn(CTX, `${tx.txid}: a replacement it implies was refused as stale — left un-evaluated for the next reparse`)
      return
    }

    const matched = await matchTx(tx, store)
    if (matched.length === 0) {
      // The view aged past the fence during the pre-pass: do not mark it — the reparse redoes it.
      if (Date.now() - startedAtMs > MAX_EVALUATION_AGE_MS) {
        log.warn(CTX, `${tx.txid} aged past the fence while being evaluated — left un-evaluated for the next reparse`)
        return
      }
      await store.markEvaluated(tx.txid)
      return
    }

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

    const outcome = await store.recordSeen(
      { txid: tx.txid, height: 0, blockHash: '', matched, fired: [], hex: tx.hex, inputs: tx.inputs },
      event,
      startedAtMs,
    )
    if (outcome !== 'recorded') {
      log.info(CTX, `${tx.txid} not recorded (tracked by a concurrent path, or a stale evaluation) — seen skipped`)
    }
  }
}

/** ZMQ rawtx path: decode → evaluate, with startedAtMs = receipt time. */
export function makeRawTxHandler(deps: RawTxHandlerDeps): (raw: Buffer) => Promise<void> {
  const evaluate = makeTxEvaluator(deps)
  return async function handleRawTx(raw: Buffer): Promise<void> {
    const startedAtMs = Date.now()
    const tx = deps.decodeRawTx(raw, deps.cfg.network)
    await evaluate(tx, startedAtMs)
  }
}
