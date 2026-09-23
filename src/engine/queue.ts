/**
 * The engine queue — weir's ONE writer. Spec: docs/DESIGN.md "Single writer".
 *
 * Every engine action (a ZMQ rawtx evaluation, a ZMQ rawblock, a whole mempool reparse,
 * boot reconciliation) is one item on this promise chain and runs to completion before the
 * next starts, in arrival order. Because nothing else mutates engine state, every action can
 * read, decide in memory and write one plain MULTI with no guard: nothing can have changed
 * in between. The queue IS the reparser's mutex and the block handler's serialisation.
 *
 * A rejection inside an item is an UNEXPECTED error → `fatal` (docker restarts the daemon
 * and boot reconciliation heals). There is deliberately no "skip the failed item and keep
 * the queue alive": a skipped block would silently lose confirmations. `onFatal` is
 * injectable for tests only.
 */
import { fatal } from '../lib/log'

const CTX = 'engine'

export interface EngineQueue {
  /** Append `fn` to the chain. Resolves when it has run; never rejects (a rejection is fatal). */
  run(fn: () => Promise<void>): Promise<void>
}

export function makeEngineQueue(onFatal: (ctx: string, err: unknown) => void = fatal): EngineQueue {
  let tail: Promise<void> = Promise.resolve()
  return {
    run(fn) {
      tail = tail.then(fn).catch((err: unknown) => onFatal(CTX, err))
      return tail
    },
  }
}
