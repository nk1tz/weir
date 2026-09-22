import type { DecodedTx, MatchedOutput } from '../lib/types'
import type { Store } from '../store/redis'

/**
 * Pure match of a decoded transaction's outputs against an already-resolved watched
 * set. Returns ALL matched outputs — an address paid by two outputs yields two entries.
 */
export function matchAgainst(tx: DecodedTx, watched: ReadonlySet<string>): MatchedOutput[] {
  if (watched.size === 0) return []
  const matched: MatchedOutput[] = []
  for (const out of tx.outputs) {
    if (out.address !== null && watched.has(out.address)) {
      matched.push({ address: out.address, vout: out.vout, valueSats: out.valueSats })
    }
  }
  return matched
}

/**
 * Match one transaction against the watch set: collect its distinct output addresses,
 * ask the store which of them are watched (one round trip), then `matchAgainst`.
 */
export async function matchTx(tx: DecodedTx, store: Pick<Store, 'watchedSubset'>): Promise<MatchedOutput[]> {
  const addresses = [
    ...new Set(tx.outputs.map((o) => o.address).filter((a): a is string => a !== null)),
  ]
  if (addresses.length === 0) return []
  return matchAgainst(tx, new Set(await store.watchedSubset(addresses)))
}
