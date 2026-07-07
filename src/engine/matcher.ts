import { DecodedTx, MatchedOutput } from '../lib/types'

/**
 * Match a decoded transaction's outputs against the watch set.
 *
 * Collects the distinct addresses present in the outputs, asks the store which
 * of them are watched (single pipelined round-trip), then returns ALL matched
 * outputs — an address paid by two outputs yields two entries.
 */
export async function matchTx(
  tx: DecodedTx,
  store: { watchedSubset(addresses: string[]): Promise<string[]> },
): Promise<MatchedOutput[]> {
  const addresses = [
    ...new Set(tx.outputs.map((o) => o.address).filter((a): a is string => a !== null)),
  ]
  if (addresses.length === 0) return []

  const watched = new Set(await store.watchedSubset(addresses))
  if (watched.size === 0) return []

  const matched: MatchedOutput[] = []
  for (const out of tx.outputs) {
    if (out.address !== null && watched.has(out.address)) {
      matched.push({ address: out.address, vout: out.vout, valueSats: out.valueSats })
    }
  }
  return matched
}
