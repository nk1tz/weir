/**
 * REAL-REDIS integration test for the Store's MULTIs.
 *
 * The unit suite mirrors the Store's semantics in FakeStore but never sends a command. This
 * file executes the three MULTIs that carry weir's state — one evaluation, one block, the
 * outbox's — against a live redis and checks what landed. Skipped unless REDIS_TEST_URL is
 * set:
 *   docker run -d -p 6390:6379 redis:7-alpine && REDIS_TEST_URL=redis://localhost:6390 pnpm test
 * or `pnpm test:redis`.
 */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { createClient } from 'redis'
import { Store } from '../../src/store/redis'
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

describe.skipIf(!URL)('Store against a real redis (the MULTIs execute for real)', () => {
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

  it('applyEvaluation: ONE MULTI marks the tx evaluated, drops the claimant it replaced (its own claim only) and records the new tx with its claims + seen', async () => {
    await store.applyEvaluation({ txid: 'A', dropped: [], seen: { rec: rec('A', [X, Y]), event: ev('seen', 'A') } })
    await store.applyEvaluation({ txid: 'C', dropped: [], seen: { rec: rec('C', [X]), event: null } }) // seen disabled: no event
    expect(await raw.hGetAll(K.maturingRecord('A'))).toMatchObject({ height: '0', blockHash: '', hex: 'hex-A', inputs: JSON.stringify([X, Y]) })
    expect(await raw.sIsMember(K.pending, 'A')).toBe(true)
    expect(await raw.sIsMember(K.evaluated, 'A')).toBe(true)
    expect(await claimants(X)).toEqual(['A', 'C'])
    expect(await claimants(Y)).toEqual(['A'])
    expect((await outboxEvents()).map((e) => `${e.event}:${e.txid}`)).toEqual(['seen:A'])
    expect(await raw.zCard(K.outboxCreated)).toBe(1)

    // B spends X: A (pending) is replaced; C stays; B pays a watch → recorded. One exec.
    const a = (await store.readRecord('A'))!
    await store.applyEvaluation({
      txid: 'B',
      dropped: [{ rec: a, event: ev('dropped', 'A', { reason: 'replaced', replacedBy: 'B' }) }],
      seen: { rec: rec('B', [X]), event: ev('seen', 'B') },
    })
    expect(await raw.exists(K.maturingRecord('A'))).toBe(0)
    expect(await raw.sIsMember(K.pending, 'A')).toBe(false)
    expect(await raw.sIsMember(K.evaluated, 'A')).toBe(false) // a rebroadcast may re-fire seen
    expect(await raw.exists(K.outpointKey(Y))).toBe(0) // the SET vanishes when empty
    expect(await claimants(X)).toEqual(['B', 'C']) // only A released
    expect(await raw.sIsMember(K.pending, 'B')).toBe(true)
    expect((await outboxEvents()).map((e) => `${e.event}:${e.txid}`)).toEqual(['seen:A', 'dropped:A', 'seen:B'])

    // an unwatched tx: evaluated only
    await store.applyEvaluation({ txid: 'U', dropped: [], seen: null })
    expect(await raw.sIsMember(K.evaluated, 'U')).toBe(true)
    expect(await raw.exists(K.maturingRecord('U'))).toBe(0)
  })

  it('applyBlock: ONE MULTI promotes, fires, finishes, drops, conflicts, ends, unindexes and moves tip + ring — all landed together', async () => {
    // seed: P pending (promoted by this block), F maturing since 1 (finishes here), D pending
    // (replaced by a block spender), L in limbo (proven conflict), E pending but no longer
    // watched (ends quietly), G a dangling maturing index entry, ring of 13 entries to prune
    await store.applyEvaluation({ txid: 'P', dropped: [], seen: { rec: rec('P', [X]), event: null } })
    await store.applyEvaluation({ txid: 'D', dropped: [], seen: { rec: rec('D', [Y]), event: null } })
    await store.applyEvaluation({ txid: 'E', dropped: [], seen: { rec: rec('E', []), event: null } })
    await raw.hSet(K.maturingRecord('F'), { height: '1', blockHash: 'b1', matched: '[]', fired: '[1]', hex: 'hex-F', inputs: JSON.stringify([{ txid: 'aa'.repeat(32), vout: 0 }]) })
    await raw.zAdd(K.maturing, { score: 1, value: 'F' })
    await raw.sAdd(K.outpointKey({ txid: 'aa'.repeat(32), vout: 0 }), 'F')
    await raw.hSet(K.maturingRecord('L'), { height: '2', blockHash: 'b2', matched: '[]', fired: '[1]', hex: 'hex-L', inputs: JSON.stringify([{ txid: 'bb'.repeat(32), vout: 0 }]) })
    await raw.sAdd(K.limbo, 'L')
    await raw.sAdd(K.outpointKey({ txid: 'bb'.repeat(32), vout: 0 }), 'L')
    await raw.zAdd(K.maturing, { score: 2, value: 'G' })
    for (let h = 0; h <= 2; h++) await raw.zAdd(K.blocks, { score: h, value: `b${h}` })
    await raw.hSet(K.tip, { hash: 'b2', height: '2' })

    const p = rec('P', [X], 3, 'b3')
    const f = (await store.readRecord('F'))!
    const l = (await store.readRecord('L'))!
    await store.applyBlock({
      promoted: [p],
      fired: [
        { txid: 'P', fired: [1], event: ev('confirmed', 'P', { confs: 1, blockHeight: 3, blockHash: 'b3' }) },
        { txid: 'F', fired: [1, 3], event: ev('confirmed', 'F', { confs: 3, blockHeight: 1, blockHash: 'b1' }) },
      ],
      finished: [f],
      dropped: [{ rec: (await store.readRecord('D'))!, event: ev('dropped', 'D', { reason: 'replaced', replacedBy: 'S' }) }],
      conflicted: [{ rec: l, event: ev('conflicted', 'L', { reason: 'double-spend', conflictingTxid: 'S' }) }],
      ended: [(await store.readRecord('E'))!],
      unindexed: ['G'],
      tip: { hash: 'b3', height: 3 },
      ringKeep: 3,
    })

    // promoted P: record at the block, indexed, out of pending, claim kept, fired [1]
    expect(await raw.hGetAll(K.maturingRecord('P'))).toMatchObject({ height: '3', blockHash: 'b3', fired: '[1]' })
    expect(await raw.zScore(K.maturing, 'P')).toBe(3)
    expect(await raw.sIsMember(K.pending, 'P')).toBe(false)
    expect(await claimants(X)).toEqual(['P'])
    // finished F: gone, claim released, its final confirmed still enqueued
    expect(await raw.exists(K.maturingRecord('F'))).toBe(0)
    expect(await raw.zScore(K.maturing, 'F')).toBeNull()
    expect(await raw.exists(K.outpointKey({ txid: 'aa'.repeat(32), vout: 0 }))).toBe(0)
    // dropped D: gone, un-evaluated, claim released
    expect(await raw.exists(K.maturingRecord('D'))).toBe(0)
    expect(await raw.sIsMember(K.pending, 'D')).toBe(false)
    expect(await raw.sIsMember(K.evaluated, 'D')).toBe(false)
    expect(await raw.exists(K.outpointKey(Y))).toBe(0)
    // conflicted L: gone from limbo and everything else
    expect(await raw.exists(K.maturingRecord('L'))).toBe(0)
    expect(await raw.sIsMember(K.limbo, 'L')).toBe(false)
    expect(await raw.exists(K.outpointKey({ txid: 'bb'.repeat(32), vout: 0 }))).toBe(0)
    // ended E: gone, no event
    expect(await raw.exists(K.maturingRecord('E'))).toBe(0)
    expect(await raw.sIsMember(K.pending, 'E')).toBe(false)
    // dangling G unindexed
    expect(await raw.zScore(K.maturing, 'G')).toBeNull()
    // tip + ring (put b3, pruned to the 3 highest)
    expect(await raw.hGetAll(K.tip)).toEqual({ hash: 'b3', height: '3' })
    expect(await raw.zRange(K.blocks, 0, -1)).toEqual(['b1', 'b2', 'b3'])
    // exactly the events of the block, in MULTI order
    expect((await outboxEvents()).map((e) => `${e.event}:${e.txid}`)).toEqual(['confirmed:P', 'confirmed:F', 'dropped:D', 'conflicted:L'])
  })

  it('rewind, demoteToPending, conflict, dropPending and resetTracking: each one MULTI', async () => {
    await raw.hSet(K.maturingRecord('M'), { height: '5', blockHash: 'b5', matched: '[]', fired: '[1]', hex: 'hex-M', inputs: JSON.stringify([X]) })
    await raw.zAdd(K.maturing, { score: 5, value: 'M' })
    await raw.sAdd(K.outpointKey(X), 'M')
    for (let h = 3; h <= 6; h++) await raw.zAdd(K.blocks, { score: h, value: `b${h}` })
    await raw.hSet(K.tip, { hash: 'b6', height: '6' })

    await store.rewind({ hash: 'b4', height: 4 }, ['M'])
    expect(await raw.sIsMember(K.limbo, 'M')).toBe(true)
    expect(await raw.zScore(K.maturing, 'M')).toBeNull()
    expect(await raw.exists(K.maturingRecord('M'))).toBe(1) // record kept
    expect(await raw.zRange(K.blocks, 0, -1)).toEqual(['b3', 'b4'])
    expect(await raw.hGetAll(K.tip)).toEqual({ hash: 'b4', height: '4' })

    const m = (await store.readRecord('M'))!
    await store.demoteToPending(m, ev('demoted', 'M', { blockHeight: 5, blockHash: 'b5' }))
    expect(await raw.hGetAll(K.maturingRecord('M'))).toMatchObject({ height: '0', blockHash: '', fired: '[]' })
    expect(await raw.sIsMember(K.pending, 'M')).toBe(true)
    expect(await raw.sIsMember(K.evaluated, 'M')).toBe(true)
    expect(await raw.sIsMember(K.limbo, 'M')).toBe(false)
    expect(await claimants(X)).toEqual(['M']) // claims kept

    await store.dropPending({ rec: (await store.readRecord('M'))!, event: ev('dropped', 'M', { reason: 'evicted' }) })
    expect(await raw.exists(K.maturingRecord('M'))).toBe(0)
    expect(await raw.sIsMember(K.pending, 'M')).toBe(false)
    expect(await raw.exists(K.outpointKey(X))).toBe(0)

    await raw.hSet(K.maturingRecord('N'), { height: '5', blockHash: 'b5', matched: '[]', fired: '[]', hex: 'hex-N', inputs: JSON.stringify([Y]) })
    await raw.sAdd(K.limbo, 'N')
    await raw.sAdd(K.outpointKey(Y), 'N')
    await store.conflict((await store.readRecord('N'))!, ev('conflicted', 'N'))
    expect(await raw.exists(K.maturingRecord('N'))).toBe(0)
    expect(await raw.sIsMember(K.limbo, 'N')).toBe(false)
    expect(await raw.exists(K.outpointKey(Y))).toBe(0)
    expect((await outboxEvents()).map((e) => `${e.event}:${e.txid}`)).toEqual(['demoted:M', 'dropped:M', 'conflicted:N'])

    // resetTracking wipes records, claims and the tracking sets; keeps watches + outbox; jumps the tip
    await store.applyEvaluation({ txid: 'R', dropped: [], seen: { rec: rec('R', [X]), event: null } })
    await raw.sAdd(K.addresses, 'bcrt1qa')
    expect((await store.resetTracking({ hash: 'b9', height: 9 })).sort()).toEqual(['R'])
    expect(await raw.keys(`${K.outpointPrefix}*`)).toEqual([])
    expect(await raw.keys(K.maturingRecord('*'))).toEqual([])
    expect(await raw.sCard(K.pending)).toBe(0)
    expect(await raw.sCard(K.evaluated)).toBe(0)
    expect(await raw.sIsMember(K.addresses, 'bcrt1qa')).toBe(true)
    expect(await raw.zCard(K.outbox)).toBe(3)
    expect(await raw.hGetAll(K.tip)).toEqual({ hash: 'b9', height: '9' })
    expect(await raw.zScore(K.blocks, 'b9')).toBe(9)
  })

  it('outbox: due/read/ack, retry reschedules, dead-letter caps and deletes hashes, stats are exact', async () => {
    await store.applyEvaluation({ txid: 'A', dropped: [], seen: { rec: rec('A', [X]), event: ev('seen', 'A') } })
    await store.applyBlock({ promoted: [], fired: [{ txid: 'A', fired: [1], event: ev('confirmed', 'A', { confs: 1 }) }], finished: [], dropped: [], conflicted: [], ended: [], unindexed: [], tip: { hash: 'b1', height: 1 }, ringKeep: 12 })
    const due = await store.outboxDue(now() + 1, 50)
    expect(due).toHaveLength(2)
    const first = (await store.outboxRead(due[0]!))!
    expect(first.event.event).toBe('seen') // enqueue order preserved on equal-ish scores
    await store.outboxRetry(due[0]!, now() + 60_000, 1, 'HTTP 500')
    expect(await store.outboxDue(now() + 1, 50)).toEqual([due[1]])
    expect((await store.outboxRead(due[0]!))!).toMatchObject({ attempts: 1, lastError: 'HTTP 500' })

    await store.outboxDead(due[1]!, now(), 3, 'HTTP 500', 1)
    await store.outboxDead(due[0]!, now() + 1, 4, 'HTTP 500', 1) // cap 1 → the older dead is dropped
    expect(await raw.zCard(K.outboxDead)).toBe(1)
    expect(await raw.exists(K.outboxRecord(due[1]!))).toBe(0) // trimmed hash deleted
    expect(await raw.exists(K.outboxRecord(due[0]!))).toBe(1)
    const stats = await store.outboxStats()
    expect(stats).toEqual({ depth: 0, oldestCreatedAt: null, dead: 1 })

    await store.applyEvaluation({ txid: 'B', dropped: [], seen: { rec: rec('B', [Y]), event: ev('seen', 'B') } })
    const [b] = await store.outboxDue(now() + 1, 50)
    await store.outboxAck(b!)
    expect(await raw.exists(K.outboxRecord(b!))).toBe(0)
    expect(await raw.zCard(K.outboxCreated)).toBe(0)
  })
})
