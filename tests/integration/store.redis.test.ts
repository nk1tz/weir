/**
 * REAL-REDIS integration test for the Store's Lua scripts and MULTIs.
 *
 * The unit suite pins script TEXT (mocked client) and mirrors semantics in FakeStore, but
 * never EXECUTES the Lua (cjson, SADD/SREM claims, the pendingUnmined guards, the
 * conditional sweep). This file does, against a live redis. Skipped unless REDIS_TEST_URL
 * is set:
 *   docker run -d -p 6390:6379 redis:7-alpine && REDIS_TEST_URL=redis://localhost:6390 pnpm test
 * or `pnpm test:redis`.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { createClient } from 'redis'
import { MAX_EVALUATION_AGE_MS, Store } from '../../src/store/redis'
import { keysFor, outpointField } from '../../src/store/keys'
import type { MaturingRecord, Outpoint, TxEvent } from '../../src/lib/types'

const URL = process.env.REDIS_TEST_URL
const K = keysFor('regtest')

const X: Outpoint = { txid: 'ff'.repeat(32), vout: 0 }
const Y: Outpoint = { txid: 'ee'.repeat(32), vout: 1 }

function rec(txid: string, inputs: Outpoint[], height = 0, blockHash = ''): MaturingRecord {
  return { txid, height, blockHash, matched: [{ address: 'bcrt1qa', vout: 0, valueSats: 5000 }], fired: [], hex: `hex-${txid}`, inputs }
}
function ev(event: TxEvent['event'], txid: string, extra: Partial<TxEvent> = {}): TxEvent {
  return {
    version: 1, event, network: 'regtest', txid, confs: 0,
    matched: [{ address: 'bcrt1qa', vout: 0, valueSats: 5000 }],
    idempotencyKey: `regtest:${txid}:${event}`, timestamp: Date.now(),
    blockHeight: null, blockHash: null, hex: `hex-${txid}`, ...extra,
  }
}

describe.skipIf(!URL)('Store against a real redis (Lua executes for real)', () => {
  let store: Store
  let raw: ReturnType<typeof createClient>

  beforeAll(async () => {
    raw = createClient({ url: URL })
    await raw.connect()
    store = new Store(URL!, 'regtest')
    await store.connect()
  })
  afterAll(async () => {
    await store.quit()
    await raw.quit()
  })
  beforeEach(async () => {
    await raw.flushDb()
  })

  const now = () => Date.now()
  const claimants = async (o: Outpoint) => (await raw.sMembers(K.outpointKey(o))).sort()
  const outboxEvents = async () => {
    const ids = await raw.zRange(K.outbox, 0, -1)
    return Promise.all(ids.map(async (id) => JSON.parse((await raw.hGet(K.outboxRecord(id), 'payload'))!) as TxEvent))
  }

  it('recordSeen: writes record + pending + evaluated, claims its inputs (SADD), enqueues; then skipped; stale is refused', async () => {
    expect(await store.recordSeen(rec('A', [X, Y]), ev('seen', 'A'), now())).toBe('recorded')
    expect(await raw.hGetAll(K.maturingRecord('A'))).toMatchObject({ height: '0', blockHash: '', hex: 'hex-A', inputs: JSON.stringify([X, Y]) })
    expect(await raw.sIsMember(K.pending, 'A')).toBe(true)
    expect(await raw.sIsMember(K.evaluated, 'A')).toBe(true)
    expect(await claimants(X)).toEqual(['A'])
    expect(await claimants(Y)).toEqual(['A'])
    expect(await raw.zCard(K.outbox)).toBe(1)
    expect((await outboxEvents())[0]).toMatchObject({ event: 'seen', txid: 'A' })
    expect(await raw.zCard(K.outboxCreated)).toBe(1)

    expect(await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())).toBe('skipped')
    expect(await raw.zCard(K.outbox)).toBe(1)

    expect(await store.recordSeen(rec('Z', [X]), ev('seen', 'Z'), now() - MAX_EVALUATION_AGE_MS - 1)).toBe('stale')
    expect(await raw.exists(K.maturingRecord('Z'))).toBe(0)
    expect(await claimants(X)).toEqual(['A']) // nothing claimed by the refused evaluation
  })

  it('claims never conflict: two txs (mempool + mined) claim the same prevout; each release SREMs only itself', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    expect(await store.recordSeen(rec('B', [X, Y]), ev('seen', 'B'), now())).toBe('recorded') // no {conflicts}, no force
    await store.promoteToMaturing(rec('C', [X], 7, 'b7')) // a mined tx claims X too
    expect(await claimants(X)).toEqual(['A', 'B', 'C'])
    expect(await claimants(Y)).toEqual(['B'])

    expect(await store.replacePending('A', ev('dropped', 'A', { reason: 'replaced', replacedBy: 'D' }), now(), 'D')).toBe('replaced')
    expect(await claimants(X)).toEqual(['B', 'C']) // only A released
    expect(await raw.sIsMember(K.pending, 'A')).toBe(false)
    expect(await raw.sIsMember(K.evaluated, 'A')).toBe(false)
    expect(await raw.exists(K.maturingRecord('A'))).toBe(0)

    await store.finishMaturing('C', now())
    expect(await claimants(X)).toEqual(['B'])
    expect(await store.isTombstoned('C')).toBe(true)

    await store.dropPending('B', ev('dropped', 'B', { reason: 'evicted' }))
    expect(await raw.exists(K.outpointKey(X))).toBe(0) // the SET vanishes when empty
    expect(await raw.exists(K.outpointKey(Y))).toBe(0)
  })

  it('concurrent claims: two recordSeen of different txs SADD into the same SET without losing either', async () => {
    const [a, b] = await Promise.all([
      store.recordSeen(rec('A', [X]), ev('seen', 'A'), now()),
      store.recordSeen(rec('B', [X]), ev('seen', 'B'), now()),
    ])
    expect([a, b]).toEqual(['recorded', 'recorded'])
    expect(await claimants(X)).toEqual(['A', 'B'])
    const owners = await store.outpointOwners([X, Y])
    expect(owners.get(outpointField(X))!.sort()).toEqual(['A', 'B'])
    expect(owners.has(outpointField(Y))).toBe(false) // empty prevouts are omitted
  })

  it('outpointOwners: pipelined SMEMBERS, chunked at 1000, a hit beyond the first chunk is found', async () => {
    const many: Outpoint[] = Array.from({ length: 1200 }, (_, i) => ({ txid: 'ab'.repeat(32), vout: i }))
    await store.recordSeen(rec('A', [many[1150]!, many[3]!]), ev('seen', 'A'), now())
    const owners = await store.outpointOwners(many)
    expect([...owners.keys()].sort()).toEqual([outpointField(many[1150]!), outpointField(many[3]!)].sort())
    expect(owners.get(outpointField(many[1150]!))).toEqual(['A'])
  })

  it('multi-claimant adjudication end to end: mined A + pending B + pending C claim X; a block spender adjudicates every claimant via the guarded Luas', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    await store.promoteToMaturing(rec('A', [X], 5, 'b5'))
    await store.recordSeen(rec('B', [X]), ev('seen', 'B'), now())
    await store.recordSeen(rec('C', [X, Y]), ev('seen', 'C'), now())
    expect(await claimants(X)).toEqual(['A', 'B', 'C'])
    // the reorg puts A in limbo; the new block's spender D spends X
    await store.addLimbo(['A'])
    await store.unindexMaturing(['A'])
    const owners = await store.outpointOwners([X])
    expect(owners.get(outpointField(X))!.sort()).toEqual(['A', 'B', 'C'])

    await store.conflict('A', ev('conflicted', 'A', { reason: 'double-spend', conflictingTxid: 'D' }))
    expect(await store.replacePending('B', ev('dropped', 'B', { reason: 'replaced', replacedBy: 'D' }), now(), 'D')).toBe('replaced')
    expect(await store.replacePending('C', ev('dropped', 'C', { reason: 'replaced', replacedBy: 'D' }), now(), 'D')).toBe('replaced')

    expect(await raw.exists(K.outpointKey(X))).toBe(0)
    expect(await raw.exists(K.outpointKey(Y))).toBe(0) // C released its other claim too
    expect(await raw.sCard(K.pending)).toBe(0)
    expect(await raw.sIsMember(K.limbo, 'A')).toBe(false)
    expect(await store.isTombstoned('A')).toBe(true)
    const events = await outboxEvents()
    expect(events.map((e) => `${e.event}:${e.txid}`)).toEqual(['seen:A', 'seen:B', 'seen:C', 'conflicted:A', 'dropped:B', 'dropped:C'])
    expect(events[4]).toMatchObject({ reason: 'replaced', replacedBy: 'D' })
  })

  it('replacePending is a no-op once the owner is mined (guard inside the Lua)', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    await store.promoteToMaturing(rec('A', [X], 5, 'b5'))
    expect(await store.replacePending('A', ev('dropped', 'A', { reason: 'replaced', replacedBy: 'B' }), now(), 'B')).toBe('skipped')
    expect(await raw.hGet(K.maturingRecord('A'), 'height')).toBe('5')
    expect(await claimants(X)).toEqual(['A'])
    expect(await raw.zCard(K.outbox)).toBe(1) // only the seen
    expect(await store.replacePending('A', ev('dropped', 'A'), now() - MAX_EVALUATION_AGE_MS - 1, 'B')).toBe('stale') // fence
  })

  it('replacePending: the height guard is reached independently of the pending guard (seeded: pending member whose record is mined)', async () => {
    // promoteToMaturing always SREMs pending, so this state is seeded directly.
    await raw.sAdd(K.pending, 'A')
    await raw.hSet(K.maturingRecord('A'), { height: '5', blockHash: 'b5', matched: '[]', fired: '[1]', hex: 'hex-A', inputs: JSON.stringify([X]) })
    await raw.sAdd(K.outpointKey(X), 'A')
    expect(await store.replacePending('A', ev('dropped', 'A', { reason: 'replaced', replacedBy: 'B' }), now(), 'B')).toBe('skipped')
    expect(await raw.sIsMember(K.pending, 'A')).toBe(true) // untouched
    expect(await raw.hGet(K.maturingRecord('A'), 'height')).toBe('5')
    expect(await claimants(X)).toEqual(['A'])
    expect(await raw.zCard(K.outbox)).toBe(0)
  })

  it('replacePending: a pending member with a MISSING record is a no-op; a STALE replacement of an otherwise eligible pending record is refused (fence first)', async () => {
    await raw.sAdd(K.pending, 'A') // no record
    expect(await store.replacePending('A', ev('dropped', 'A', { reason: 'replaced', replacedBy: 'B' }), now(), 'B')).toBe('skipped')
    expect(await raw.sIsMember(K.pending, 'A')).toBe(true)
    expect(await raw.zCard(K.outbox)).toBe(0)

    await store.recordSeen(rec('C', [Y]), ev('seen', 'C'), now()) // fully eligible
    expect(await store.replacePending('C', ev('dropped', 'C', { reason: 'replaced', replacedBy: 'B' }), now() - MAX_EVALUATION_AGE_MS - 1, 'B')).toBe('stale')
    expect(await raw.sIsMember(K.pending, 'C')).toBe(true)
    expect(await raw.exists(K.maturingRecord('C'))).toBe(1)
    expect(await claimants(Y)).toEqual(['C'])
    expect(await raw.zCard(K.outbox)).toBe(1) // only C's seen
    expect(await store.replacePending('C', ev('dropped', 'C', { reason: 'replaced', replacedBy: 'B' }), now(), 'B')).toBe('replaced') // and a fresh one succeeds
  })

  it('dropPending is GUARDED: not pending, or record missing/mined → false with nothing written or enqueued; a second caller gets no second verdict', async () => {
    const dropped = ev('dropped', 'A', { reason: 'evicted' })
    expect(await store.dropPending('A', dropped)).toBe(false) // unknown txid
    await raw.sAdd(K.pending, 'A')
    expect(await store.dropPending('A', dropped)).toBe(false) // pending, no record
    await raw.hSet(K.maturingRecord('A'), { height: '5', blockHash: 'b5', matched: '[]', fired: '[]', hex: 'hex-A', inputs: '[]' })
    expect(await store.dropPending('A', dropped)).toBe(false) // pending, record mined
    expect(await raw.zCard(K.outbox)).toBe(0)
    expect(await raw.sIsMember(K.pending, 'A')).toBe(true)

    await raw.del([K.pending, K.maturingRecord('A')])
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    expect(await store.dropPending('A', dropped)).toBe(true)
    expect(await store.dropPending('A', dropped)).toBe(false)
    expect(await raw.zCard(K.outbox)).toBe(2) // seen + ONE dropped
    expect(await raw.sIsMember(K.evaluated, 'A')).toBe(false) // a rebroadcast may re-fire seen
    expect(await store.isTombstoned('A')).toBe(false)
    await new Promise((r) => setTimeout(r, 3)) // a rebroadcast: an evaluation started AFTER the exit
    expect(await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())).toBe('recorded')
  })

  it('RETIREMENT WATERMARK: dropPending/replacePending ZADD retired; an evaluation that started at or before the exit is refused (stale, nothing written), a later one records; drops never tombstone', async () => {
    expect(await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())).toBe('recorded')
    const older = now() // a duplicate evaluation of A that is still in flight
    await new Promise((r) => setTimeout(r, 3))
    expect(await store.replacePending('A', ev('dropped', 'A', { reason: 'replaced', replacedBy: 'B' }), now(), 'B')).toBe('replaced')
    const exitAt = (await raw.zScore(K.retired, 'A'))!
    expect(exitAt).toBeGreaterThan(older)
    expect(await store.isTombstoned('A')).toBe(false)

    expect(await store.recordSeen(rec('A', [X]), ev('seen', 'A'), older)).toBe('stale') // predates the exit
    expect(await raw.exists(K.maturingRecord('A'))).toBe(0)
    expect(await raw.sIsMember(K.pending, 'A')).toBe(false)
    expect(await raw.exists(K.outpointKey(X))).toBe(0)
    expect(await raw.zCard(K.outbox)).toBe(2) // seen + ONE dropped — no resurrection
    expect(await store.recordSeen(rec('A', [X]), ev('seen', 'A'), exitAt)).toBe('stale') // same ms: still predates

    expect(await store.recordSeen(rec('A', [X]), ev('seen', 'A'), exitAt + 1)).toBe('recorded') // a real rebroadcast
    expect(await raw.zScore(K.retired, 'A')).toBe(exitAt) // the watermark stays until the next exit / the prune
    expect(await claimants(X)).toEqual(['A'])

    // dropPending sets it too, and a later drop overwrites it
    await new Promise((r) => setTimeout(r, 3))
    expect(await store.dropPending('A', ev('dropped', 'A', { reason: 'evicted' }))).toBe(true)
    expect((await raw.zScore(K.retired, 'A'))!).toBeGreaterThan(exitAt)
    expect(await store.isTombstoned('A')).toBe(false)

    // dropped-then-mined without a mempool sighting: the block path's never-seen promotion is not blocked
    await store.promoteToMaturing(rec('A', [X], 9, 'b9'))
    expect(await raw.zScore(K.maturing, 'A')).toBe(9)
    expect(await claimants(X)).toEqual(['A'])

    // pruned by TTL like the tombstones
    await store.pruneRetired(now())
    expect(await raw.zScore(K.retired, 'A')).toBeNull()
  })

  it('SPENDER GUARD: replacePending refuses (stale, nothing written) when the spender is retired at an exit ≥ startedAt, or tombstoned; a later evaluation of the same spender passes', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    const olderA = now()
    await new Promise((r) => setTimeout(r, 3))
    expect(await store.replacePending('A', ev('dropped', 'A', { reason: 'replaced', replacedBy: 'B' }), now(), 'B')).toBe('replaced')
    await store.recordSeen(rec('B', [X]), ev('seen', 'B'), now())
    // the old evaluation of A tries to replace B: A is retired after olderA → refused
    expect(await store.replacePending('B', ev('dropped', 'B', { reason: 'replaced', replacedBy: 'A' }), olderA, 'A')).toBe('stale')
    expect(await raw.sIsMember(K.pending, 'B')).toBe(true)
    expect(await claimants(X)).toEqual(['B'])
    expect(await raw.zCard(K.outbox)).toBe(3) // seen A, dropped A, seen B — nothing more
    // a NEW evaluation of A (after its exit) may act
    const exitA = (await raw.zScore(K.retired, 'A'))!
    expect(await store.replacePending('B', ev('dropped', 'B', { reason: 'replaced', replacedBy: 'A' }), exitA + 1, 'A')).toBe('replaced')
    // a tombstoned spender may never act
    await store.recordSeen(rec('C', [Y]), ev('seen', 'C'), now())
    await raw.zAdd(K.tombstones, { score: now(), value: 'T' })
    expect(await store.replacePending('C', ev('dropped', 'C', { reason: 'replaced', replacedBy: 'T' }), now(), 'T')).toBe('stale')
    expect(await raw.sIsMember(K.pending, 'C')).toBe(true)
  })

  it('conflict(): full cleanup, own-claim release, tombstone, enqueue', async () => {
    await store.recordSeen(rec('B', [Y]), ev('seen', 'B'), now())
    await store.promoteToMaturing(rec('B', [Y], 3, 'b3'))
    await store.recordSeen(rec('E', [Y]), ev('seen', 'E'), now()) // a co-claimant that must survive
    await store.addLimbo(['B'])
    await store.unindexMaturing(['B'])
    await store.conflict('B', ev('conflicted', 'B', { reason: 'double-spend', conflictingTxid: 'D' }))
    expect(await raw.exists(K.maturingRecord('B'))).toBe(0)
    expect(await raw.sIsMember(K.limbo, 'B')).toBe(false)
    expect(await claimants(Y)).toEqual(['E'])
    expect(await store.isTombstoned('B')).toBe(true)
    expect((await outboxEvents()).map((e) => e.event)).toEqual(['seen', 'seen', 'conflicted'])
  })

  it('demoteToPending: back to height 0, pending + evaluated, out of limbo, claims kept', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    await store.promoteToMaturing(rec('A', [X], 5, 'b5'))
    await store.addLimbo(['A'])
    await store.unindexMaturing(['A'])
    await store.demoteToPending(rec('A', [X], 5, 'b5'), ev('demoted', 'A', { blockHeight: 5, blockHash: 'b5' }))
    expect(await raw.hGetAll(K.maturingRecord('A'))).toMatchObject({ height: '0', blockHash: '', fired: '[]' })
    expect(await raw.sIsMember(K.pending, 'A')).toBe(true)
    expect(await raw.sIsMember(K.evaluated, 'A')).toBe(true)
    expect(await raw.sIsMember(K.limbo, 'A')).toBe(false)
    expect(await claimants(X)).toEqual(['A'])
    // and the demoted tx is a live pending claimant again: a spender's replacePending applies
    expect(await store.replacePending('A', ev('dropped', 'A', { reason: 'replaced', replacedBy: 'D' }), now(), 'D')).toBe('replaced')
    expect(await raw.exists(K.outpointKey(X))).toBe(0)
  })

  it('endTracking: SREM pending + limbo, own-claim release, record gone, nothing enqueued', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    await store.recordSeen(rec('B', [X]), ev('seen', 'B'), now())
    await store.addLimbo(['A'])
    await store.endTracking('A')
    expect(await raw.sIsMember(K.pending, 'A')).toBe(false)
    expect(await raw.sIsMember(K.limbo, 'A')).toBe(false)
    expect(await raw.exists(K.maturingRecord('A'))).toBe(0)
    expect(await claimants(X)).toEqual(['B'])
    expect(await raw.zCard(K.outbox)).toBe(2) // only the two seen
  })

  it('a release never touches another claimant, even when the releaser is not in the SET at all', async () => {
    await store.recordSeen(rec('A', [X, Y]), ev('seen', 'A'), now())
    await store.promoteToMaturing(rec('A', [X, Y], 5, 'b5'))
    await raw.sRem(K.outpointKey(X), 'A') // A's claim on X vanished by some other path
    await raw.sAdd(K.outpointKey(X), 'Z')
    await store.finishMaturing('A', now())
    expect(await claimants(X)).toEqual(['Z']) // intact
    expect(await raw.exists(K.outpointKey(Y))).toBe(0) // A's own claim released
  })

  it('clearTracking: one script wipes tracking, removes every outpoint:* SET, preserves watches + outbox, returns the lost txids', async () => {
    await raw.sAdd(K.addresses, 'bcrt1qa')
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    await store.recordSeen(rec('B', [Y]), ev('seen', 'B'), now())
    await store.promoteToMaturing(rec('B', [Y], 2, 'b2'))
    const lost = (await store.clearTracking()).sort()
    expect(lost).toEqual(['A', 'B'])
    for (const k of [K.pending, K.evaluated, K.maturing, K.limbo, K.maturingRecord('A'), K.maturingRecord('B'), K.outpointKey(X), K.outpointKey(Y)]) {
      expect(await raw.exists(k)).toBe(0)
    }
    expect(await raw.keys(`${K.outpointPrefix}*`)).toEqual([])
    expect(await raw.sCard(K.addresses)).toBe(1)
    expect(await raw.zCard(K.outbox)).toBe(2) // queued events are still owed
  })

  it('clearTracking ORDER: claims are DELeted before the tracking Lua and the sweep after it — a recordSeen landing after the Lua keeps its record, memberships AND claims', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    await store.promoteToMaturing(rec('B', [Y], 2, 'b2'))
    const sweep = store.sweepOrphanRecords.bind(store)
    store.sweepOrphanRecords = async () => {
      // lands between the tracking Lua and the orphan sweep
      expect(await store.recordSeen(rec('fresh', [X, Y]), ev('seen', 'fresh'), now())).toBe('recorded')
      return sweep()
    }
    try {
      expect((await store.clearTracking()).sort()).toEqual(['A', 'B'])
    } finally {
      store.sweepOrphanRecords = sweep
    }
    for (const k of [K.maturingRecord('A'), K.maturingRecord('B'), K.maturing, K.limbo]) expect(await raw.exists(k)).toBe(0)
    expect(await raw.exists(K.maturingRecord('fresh'))).toBe(1) // the conditional sweep left it
    expect(await raw.sMembers(K.pending)).toEqual(['fresh'])
    expect(await raw.sMembers(K.evaluated)).toEqual(['fresh'])
    expect(await claimants(X)).toEqual(['fresh']) // A's claim wiped first; fresh's claim survived
    expect(await claimants(Y)).toEqual(['fresh'])
  })

  it('clearTracking ORPHAN CLAIMS: a recordSeen landing between the bulk outpoint DEL and the tracking Lua leaves NO claim behind — the Lua releases each collected record\'s own claims; the sweep does the same for orphans', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    const pendingTxids = store.pendingTxids.bind(store)
    store.pendingTxids = async () => {
      // lands after the bulk SCAN-DEL (which runs before the collection) and before the Lua
      expect(await store.recordSeen(rec('C', [X, Y]), ev('seen', 'C'), now())).toBe('recorded')
      return pendingTxids() // C is collected → the Lua releases its claims and deletes its record
    }
    try {
      expect((await store.clearTracking()).sort()).toEqual(['A', 'C'])
    } finally {
      store.pendingTxids = pendingTxids
    }
    expect(await raw.exists(K.maturingRecord('C'))).toBe(0)
    expect(await raw.sCard(K.pending)).toBe(0)
    expect(await raw.sCard(K.evaluated)).toBe(0)
    expect(await raw.keys(`${K.outpointPrefix}*`)).toEqual([]) // C's claims on X and Y released by the Lua

    // the orphan sweep releases too: an orphan record with a stray claim
    await raw.hSet(K.maturingRecord('O'), { height: '0', blockHash: '', matched: '[]', fired: '[]', hex: 'x', inputs: JSON.stringify([Y]) })
    await raw.sAdd(K.outpointKey(Y), 'O')
    expect(await store.sweepOrphanRecords()).toBe(1)
    expect(await raw.exists(K.outpointKey(Y))).toBe(0)
  })

  it('sweepOrphanRecords deletes a record ONLY while it has no live membership (a concurrent recordSeen survives)', async () => {
    await raw.hSet(K.maturingRecord('orphan'), { height: '0', blockHash: '', matched: '[]', fired: '[]', hex: 'x', inputs: '[]' })
    await store.recordSeen(rec('live', [X]), ev('seen', 'live'), now()) // pending
    await store.promoteToMaturing(rec('mined', [Y], 4, 'b4')) // maturing index
    await raw.hSet(K.maturingRecord('limboed'), { height: '3', blockHash: 'b3', matched: '[]', fired: '[]', hex: 'x', inputs: '[]' })
    await store.addLimbo(['limboed'])

    expect(await store.sweepOrphanRecords()).toBe(1)

    expect(await raw.exists(K.maturingRecord('orphan'))).toBe(0)
    expect(await raw.exists(K.maturingRecord('live'))).toBe(1)
    expect(await raw.exists(K.maturingRecord('mined'))).toBe(1)
    expect(await raw.exists(K.maturingRecord('limboed'))).toBe(1)
  })

  it('outbox: due/read/ack, retry reschedules, dead-letter caps and deletes hashes, stats are exact', async () => {
    await store.recordSeen(rec('A', [X]), ev('seen', 'A'), now())
    await store.markFired('A', [1], ev('confirmed', 'A', { confs: 1 }))
    const due = await store.outboxDue(now() + 1, 50)
    expect(due).toHaveLength(2)
    const first = (await store.outboxRead(due[0]!))!
    expect(first.event.event).toBe('seen') // enqueue order preserved on equal-ish scores
    expect(await store.outboxRetry(due[0]!, now() + 60_000, 1, 'HTTP 500')).toBe(true)
    expect(await store.outboxDue(now() + 1, 50)).toEqual([due[1]])
    expect((await store.outboxRead(due[0]!))!.attempts).toBe(1)
    expect(await store.outboxRetry('no-such-id', now(), 1, 'x')).toBe(false)

    expect(await store.outboxDead(due[1]!, now(), 3, 'HTTP 500', 1)).toBe(true)
    expect(await store.outboxDead(due[0]!, now() + 1, 4, 'HTTP 500', 1)).toBe(true) // cap 1 → the older dead is dropped
    expect(await raw.zCard(K.outboxDead)).toBe(1)
    expect(await raw.exists(K.outboxRecord(due[1]!))).toBe(0) // trimmed hash deleted
    expect(await raw.exists(K.outboxRecord(due[0]!))).toBe(1)
    const stats = await store.outboxStats()
    expect(stats).toEqual({ depth: 0, oldestCreatedAt: null, dead: 1 })

    await store.recordSeen(rec('B', [Y]), ev('seen', 'B'), now())
    const [b] = await store.outboxDue(now() + 1, 50)
    await store.outboxAck(b!)
    expect(await raw.exists(K.outboxRecord(b!))).toBe(0)
    expect(await raw.zCard(K.outboxCreated)).toBe(0)
  })
})
