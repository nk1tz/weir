import type { EventEmitter } from 'node:events'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ExpiredEvent, TxEvent } from '../src/lib/types'

/**
 * Store connection-lifecycle tests against a module-mocked `redis` client: the real
 * `createClient` is replaced by an EventEmitter with the few methods these paths use, so
 * the reconnect-exhaustion and 'end' handling — plus CONFIG GET error classification —
 * are exercised without a redis server. Command semantics (MULTI etc.) are E2E territory.
 */
interface FakeClient extends EventEmitter {
  options: { socket: { reconnectStrategy: (retries: number, cause: Error) => number | Error } }
  connect: ReturnType<typeof vi.fn>
  quit: ReturnType<typeof vi.fn>
  configGet: ReturnType<typeof vi.fn>
  eval: ReturnType<typeof vi.fn>
  zRange: ReturnType<typeof vi.fn>
  zRangeByScore: ReturnType<typeof vi.fn>
  zCard: ReturnType<typeof vi.fn>
  hGetAll: ReturnType<typeof vi.fn>
  zScore: ReturnType<typeof vi.fn>
  zRemRangeByScore: ReturnType<typeof vi.fn>
  zRangeWithScores: ReturnType<typeof vi.fn>
  /** every MULTI chain that reached exec(), in order, as recorded command sequences */
  multiCalls: Array<Array<{ cmd: string; args: unknown[] }>>
  /** what the next exec() replies with (a MULTI used as a read pipeline) */
  execReplies: unknown[]
}

const holder = vi.hoisted(() => ({ clients: [] as unknown[] }))

vi.mock('redis', async () => {
  const { EventEmitter } = await import('node:events')
  class MockClient extends EventEmitter {
    options: unknown
    connect = vi.fn(async () => {})
    quit = vi.fn(async () => {
      this.emit('end') // node-redis emits 'end' after a deliberate quit/disconnect
    })
    configGet = vi.fn(async () => ({}))
    eval = vi.fn(async () => 1)
    zRange = vi.fn(async () => [])
    zRangeByScore = vi.fn(async () => [])
    zCard = vi.fn(async () => 0)
    hGetAll = vi.fn(async () => ({}))
    zScore = vi.fn(async () => null)
    zRemRangeByScore = vi.fn(async () => 0)
    zRangeWithScores = vi.fn(async () => [])
    multiCalls: Array<Array<{ cmd: string; args: unknown[] }>> = []
    execReplies: unknown[] = []
    /** chainable recorder: exec() commits the recorded ops as ONE transaction */
    multi = () => {
      const ops: Array<{ cmd: string; args: unknown[] }> = []
      const rec: Record<string, unknown> = {
        exec: async () => {
          this.multiCalls.push(ops)
          return this.execReplies
        },
      }
      for (const cmd of ['hSet', 'hGet', 'zAdd', 'sAdd', 'sRem', 'del', 'zRem', 'sDiffStore', 'sInterStore']) {
        rec[cmd] = (...args: unknown[]) => {
          ops.push({ cmd, args })
          return rec
        }
      }
      return rec
    }
    constructor(options: unknown) {
      super()
      this.options = options
    }
  }
  return {
    createClient: (options: unknown) => {
      const c = new MockClient(options)
      holder.clients.push(c)
      return c
    },
  }
})

import { Store } from '../src/store/redis'

function mkStore(): { store: Store; client: FakeClient } {
  const store = new Store('redis://redis:6379', 'regtest')
  const client = holder.clients[holder.clients.length - 1] as FakeClient
  return { store, client }
}

describe('Store connection lifecycle', () => {
  let exit: ReturnType<typeof vi.spyOn>
  let errorLog: ReturnType<typeof vi.spyOn>
  let warnLog: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    holder.clients.length = 0
    exit = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never)
    errorLog = vi.spyOn(console, 'error').mockImplementation(() => {})
    warnLog = vi.spyOn(console, 'warn').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('reconnect strategy: bounded exponential backoff, capped at 2s', () => {
    const { client } = mkStore()
    const strategy = client.options.socket.reconnectStrategy
    const cause = new Error('connect ECONNREFUSED')
    expect([0, 1, 2, 3, 4].map((r) => strategy(r, cause))).toEqual([250, 500, 1000, 2000, 2000])
    expect(exit).not.toHaveBeenCalled()
  })

  it('exhaustion BEFORE connect() resolved returns an Error (boot fails fast through connect), no exit', () => {
    const { client } = mkStore()
    const result = client.options.socket.reconnectStrategy(5, new Error('connect ECONNREFUSED 10.0.0.9:6379'))

    expect(result).toBeInstanceOf(Error)
    expect((result as Error).message).toMatch(/redis unreachable after 5 connection attempts/)
    expect((result as Error).message).toContain('ECONNREFUSED 10.0.0.9:6379')
    expect(exit).not.toHaveBeenCalled()
  })

  it('exhaustion at RUNTIME is fatal — node-redis would otherwise leave a closed client with nobody waiting', async () => {
    const { store, client } = mkStore()
    await store.connect()

    client.options.socket.reconnectStrategy(5, new Error('connect ECONNREFUSED'))

    expect(exit).toHaveBeenCalledWith(1)
    expect(errorLog.mock.calls.some((c) => /\[store\] fatal: .*redis unreachable after 5 connection attempts/.test(String(c[0])))).toBe(true)
  })

  it("an 'end' the Store did not ask for is fatal", async () => {
    const { store, client } = mkStore()
    await store.connect()

    client.emit('end')

    expect(exit).toHaveBeenCalledWith(1)
    expect(errorLog.mock.calls.some((c) => /redis connection closed unexpectedly/.test(String(c[0])))).toBe(true)
  })

  it("the 'end' that follows quit() is expected — no exit", async () => {
    const { store, client } = mkStore()
    await store.connect()

    await store.quit()

    expect(client.quit).toHaveBeenCalledTimes(1)
    expect(exit).not.toHaveBeenCalled()
  })

  it("client 'error' events are logged, not fatal (node-redis reconnects by itself)", async () => {
    const { store, client } = mkStore()
    await store.connect()

    client.emit('error', new Error('Socket closed unexpectedly'))

    expect(exit).not.toHaveBeenCalled()
    expect(errorLog.mock.calls.some((c) => /redis client error: Socket closed unexpectedly/.test(String(c[0])))).toBe(true)
  })

  describe('maxmemoryPolicy', () => {
    it('returns the configured policy', async () => {
      const { store, client } = mkStore()
      client.configGet.mockResolvedValue({ 'maxmemory-policy': 'noeviction' })
      expect(await store.maxmemoryPolicy()).toBe('noeviction')
      expect(warnLog).not.toHaveBeenCalled()
    })

    it.each([
      "ERR unknown command 'CONFIG', with args beginning with: 'GET' 'maxmemory-policy'",
      'NOPERM this user has no permissions to run the CONFIG command',
      'ERR This command is not allowed from this context',
      'ERR command disabled',
    ])('a command-access error (%s) means CONFIG is blocked: warn + null', async (message) => {
      const { store, client } = mkStore()
      client.configGet.mockRejectedValue(new Error(message))

      expect(await store.maxmemoryPolicy()).toBeNull()
      expect(warnLog.mock.calls.some((c) => /CONFIG GET maxmemory-policy blocked/.test(String(c[0])))).toBe(true)
    })

    it('any other failure (connection loss etc.) is rethrown, not misreported as "blocked"', async () => {
      const { store, client } = mkStore()
      client.configGet.mockRejectedValue(new Error('Socket closed unexpectedly'))

      await expect(store.maxmemoryPolicy()).rejects.toThrow('Socket closed unexpectedly')
      expect(warnLog).not.toHaveBeenCalled()
    })

    it('an AUTH failure is rethrown even when its text contains a "blocked"-looking word', async () => {
      // Upstash: "WRONGPASS invalid username-password pair or user is disabled"
      for (const msg of [
        'WRONGPASS invalid username-password pair or user is disabled',
        'NOAUTH Authentication required.',
      ]) {
        const { store, client } = mkStore()
        client.configGet.mockRejectedValueOnce(new Error(msg))
        await expect(store.maxmemoryPolicy()).rejects.toThrow(msg)
      }
      expect(warnLog).not.toHaveBeenCalled()
    })

    it('explicit command-access denials from managed redis are classified as blocked', async () => {
      for (const msg of [
        "ERR unknown command 'CONFIG', with args beginning with: 'GET' 'maxmemory-policy'",
        'NOPERM this user has no permissions to run the \'config|get\' command',
        'ERR CONFIG GET is not allowed',
      ]) {
        const { store, client } = mkStore()
        client.configGet.mockRejectedValueOnce(new Error(msg))
        expect(await store.maxmemoryPolicy()).toBeNull()
      }
    })
  })

  // The engine tests run against tests/fakes.ts, so a regression in the REAL Store's MULTI
  // (e.g. dropping SADD evaluated, or the outbox enqueue) is invisible to them. These pin
  // the exact command chain each atomic transition sends, and that it is ONE transaction
  // that carries the event with it.
  describe('atomic transitions send exactly one MULTI with the contracted commands', () => {
    const T = 1_700_000_000_000
    const rec = {
      txid: 'tx1',
      height: 101,
      blockHash: 'b101',
      matched: [{ address: 'bcrt1qaddr', vout: 0, valueSats: 5000 }],
      fired: [1],
      hex: 'hex-tx1',
    }
    const K = (name: string) => `weir:regtest:${name}`
    /** makeEventId: <nowMs padded 15>-<seq padded 8>-<8 hex> — monotonic so equal-score ties sort in enqueue order */
    const EVENT_ID = /^\d{15}-\d{8}-[0-9a-f]{8}$/
    const txEvent = (event: TxEvent['event'], key: string): TxEvent => ({
      version: 1,
      event,
      network: 'regtest',
      txid: 'tx1',
      confs: event === 'confirmed' ? 1 : 0,
      matched: rec.matched,
      blockHeight: 101,
      blockHash: 'b101',
      hex: 'hex-tx1',
      idempotencyKey: key,
      timestamp: T,
    })
    const cmdKeys = (ops: Array<{ cmd: string; args: unknown[] }>) => ops.map((o) => [o.cmd, o.args[0]])

    /** The enqueue must be the LAST three ops: HSET outbox:{id} + ZADD outbox {now, id} + ZADD outbox:created {now, id}. */
    function expectEnqueued(ops: Array<{ cmd: string; args: unknown[] }>, ev: TxEvent | ExpiredEvent): string {
      const hset = ops[ops.length - 3]!
      const zadd = ops[ops.length - 2]!
      const created = ops[ops.length - 1]!
      expect(hset.cmd).toBe('hSet')
      const m = /^weir:regtest:outbox:(.+)$/.exec(String(hset.args[0]))
      expect(m, `outbox hash key, got ${String(hset.args[0])}`).not.toBeNull()
      const id = m![1]!
      expect(id).toMatch(EVENT_ID)
      expect(id.startsWith(String(T).padStart(15, '0'))).toBe(true)
      expect(hset.args[1]).toEqual({
        payload: JSON.stringify(ev),
        event: ev.event,
        idempotencyKey: ev.idempotencyKey,
        attempts: '0',
        createdAt: String(T),
      })
      expect(zadd.cmd).toBe('zAdd')
      expect(zadd.args[0]).toBe(K('outbox'))
      expect(zadd.args[1]).toEqual({ score: T, value: id })
      expect(created.cmd).toBe('zAdd')
      expect(created.args[0]).toBe(K('outbox:created'))
      expect(created.args[1]).toEqual({ score: T, value: id })
      return id
    }

    it('makeEventId: monotonic within a millisecond; the sequence restarts at 0 on the next millisecond', async () => {
      const { makeEventId } = await import('../src/store/redis')
      const ids = [makeEventId(T), makeEventId(T), makeEventId(T)]
      for (const id of ids) expect(id).toMatch(EVENT_ID)
      expect([...ids].sort()).toEqual(ids)
      expect(ids.map((id) => id.split('-')[1])).toEqual(['00000000', '00000001', '00000002'])
      const next = makeEventId(T + 1)
      expect(next > ids[2]!).toBe(true)
      expect(next.split('-')[1]).toBe('00000000')
      expect(makeEventId(T + 1).split('-')[1]).toBe('00000001')
    })

    beforeEach(() => {
      vi.spyOn(Date, 'now').mockReturnValue(T)
    })

    it('promoteToMaturing: HSET record, ZADD maturing, SREM pending, SREM limbo, SADD evaluated — no event', async () => {
      const { store, client } = mkStore()
      await store.promoteToMaturing(rec)
      expect(client.multiCalls).toHaveLength(1)
      const ops = client.multiCalls[0]!
      expect(ops.map((o) => [o.cmd, o.args[0]])).toEqual([
        ['hSet', K('maturing:tx1')],
        ['zAdd', K('maturing')],
        ['sRem', K('pending')],
        ['sRem', K('limbo')],
        ['sAdd', K('evaluated')],
      ])
      expect(ops[0]!.args[1]).toMatchObject({ height: '101', blockHash: 'b101', fired: '[1]', hex: 'hex-tx1' })
      expect(ops[1]!.args[1]).toEqual({ score: 101, value: 'tx1' })
    })

    it('demoteToPending: HSET record at height 0, SADD pending, SADD evaluated, SREM limbo + enqueue demoted', async () => {
      const { store, client } = mkStore()
      const ev = txEvent('demoted', 'regtest:tx1:demoted:b101')
      await store.demoteToPending(rec, ev)
      expect(client.multiCalls).toHaveLength(1)
      const ops = client.multiCalls[0]!
      expect(cmdKeys(ops.slice(0, 4))).toEqual([
        ['hSet', K('maturing:tx1')],
        ['sAdd', K('pending')],
        ['sAdd', K('evaluated')],
        ['sRem', K('limbo')],
      ])
      // demotion resets inclusion + fired but keeps matched/hex so a later dropped/re-mine has data
      expect(ops[0]!.args[1]).toMatchObject({ height: '0', blockHash: '', fired: '[]', hex: 'hex-tx1' })
      expect(ops).toHaveLength(7)
      expectEnqueued(ops, ev)
    })

    describe('recordSeen is ONE guarded Lua script (no MULTI)', () => {
      const seenRec = { ...rec, height: 0, blockHash: '', fired: [] }
      const seen: TxEvent = { ...txEvent('seen', 'regtest:tx1:seen'), blockHeight: null, blockHash: null }

      it('with an event: fence, guards, HSET record, SADD pending, SADD evaluated, enqueue — one EVAL', async () => {
        const { store, client } = mkStore()
        await expect(store.recordSeen(seenRec, seen, T - 5000)).resolves.toBe(true)

        expect(client.multiCalls).toHaveLength(0)
        expect(client.eval).toHaveBeenCalledTimes(1)
        const [script, opts] = client.eval.mock.calls[0] as [string, { keys: string[]; arguments: string[] }]
        // the fence comes first and is atomic with the guards: now − startedAt > maxAge → -1
        expect(script).toMatch(/^if tonumber\(ARGV\[11\]\) - tonumber\(ARGV\[12\]\) > tonumber\(ARGV\[13\]\) then return -1 end/)
        // the guards: an already-evaluated txid, or a record already mined (height > 0), is a no-op
        expect(script).toMatch(/SISMEMBER.*KEYS\[3\].*return 0/s)
        expect(script).toMatch(/HGET.*KEYS\[1\].*'height'.*tonumber\(h\) > 0 then return 0/s)
        expect(script).toMatch(/ZSCORE.*KEYS\[6\].*then return 0/s) // tombstoned → no resurrection
        expect(script).toMatch(/HSET.*KEYS\[1\]/)
        expect(script).toMatch(/SADD.*KEYS\[2\]/)
        expect(script).toMatch(/SADD.*KEYS\[3\]/)
        expect(script).toMatch(/ZADD.*KEYS\[4\]/)
        expect(script).toMatch(/HSET.*KEYS\[5\]/)
        expect(script).toMatch(/ZADD.*KEYS\[7\]/)
        expect(opts.keys).toHaveLength(7)
        expect(opts.keys.slice(0, 4)).toEqual([K('maturing:tx1'), K('pending'), K('evaluated'), K('outbox')])
        expect(opts.keys.slice(5)).toEqual([K('tombstones'), K('outbox:created')])
        const m = /^weir:regtest:outbox:(.+)$/.exec(opts.keys[4]!)
        expect(m).not.toBeNull()
        const id = m![1]!
        expect(id).toMatch(EVENT_ID)
        expect(opts.arguments).toEqual([
          'tx1',
          '0',
          '',
          JSON.stringify(rec.matched),
          '[]',
          'hex-tx1',
          id,
          JSON.stringify(seen),
          'seen',
          'regtest:tx1:seen',
          String(T),
          String(T - 5000),
          '600000',
        ])
      })

      it('a stale evaluation (script returns -1) resolves false and warns with the age', async () => {
        const { store, client } = mkStore()
        client.eval.mockResolvedValueOnce(-1)
        await expect(store.recordSeen(seenRec, seen, T - 700_000)).resolves.toBe(false)
        expect(warnLog.mock.calls.some((c) => /refused stale evaluation of tx1: started 700000ms ago \(fence 600000ms\)/.test(String(c[0])))).toBe(true)
      })

      it('without an event (seen disabled): no outbox key, empty event arguments', async () => {
        const { store, client } = mkStore()
        await store.recordSeen(seenRec, null, T)
        const [, opts] = client.eval.mock.calls[0] as [string, { keys: string[]; arguments: string[] }]
        expect(opts.keys).toEqual([K('maturing:tx1'), K('pending'), K('evaluated'), K('outbox'), K('outbox'), K('tombstones'), K('outbox:created')])
        expect(opts.arguments.slice(6, 10)).toEqual(['', '', '', ''])
        expect(opts.arguments[10]).toBe(String(T))
      })

      it('resolves false (no warn) when the script reports a guard fired', async () => {
        const { store, client } = mkStore()
        client.eval.mockResolvedValueOnce(0)
        await expect(store.recordSeen(seenRec, seen, T)).resolves.toBe(false)
        expect(warnLog).not.toHaveBeenCalled()
      })
    })

    it('markFired: HSET fired + enqueue confirmed', async () => {
      const { store, client } = mkStore()
      const ev = txEvent('confirmed', 'regtest:tx1:confirmed:1:b101')
      await store.markFired('tx1', [1, 3], ev)
      expect(client.multiCalls).toHaveLength(1)
      const ops = client.multiCalls[0]!
      expect(ops).toHaveLength(4)
      expect(cmdKeys(ops.slice(0, 1))).toEqual([['hSet', K('maturing:tx1')]])
      expect(ops[0]!.args[1]).toEqual({ fired: '[1,3]' })
      expectEnqueued(ops, ev)
    })

    it('dropPending: SREM pending, SREM evaluated, DEL record + enqueue dropped', async () => {
      const { store, client } = mkStore()
      const ev = txEvent('dropped', 'regtest:tx1:dropped:101')
      await store.dropPending('tx1', ev)
      expect(client.multiCalls).toHaveLength(1)
      const ops = client.multiCalls[0]!
      expect(ops).toHaveLength(6)
      expect(cmdKeys(ops.slice(0, 3))).toEqual([
        ['sRem', K('pending')],
        ['sRem', K('evaluated')],
        ['del', K('maturing:tx1')],
      ])
      expect(ops[0]!.args[1]).toBe('tx1')
      expect(ops.some((o) => o.args[0] === K('tombstones'))).toBe(false) // a rebroadcast may re-fire seen
      expectEnqueued(ops, ev)
    })

    it('conflict: SREM pending, ZREM maturing, DEL record, SREM limbo, ZADD tombstones + enqueue conflicted', async () => {
      const { store, client } = mkStore()
      const ev = txEvent('conflicted', 'regtest:tx1:conflicted')
      await store.conflict('tx1', ev)
      expect(client.multiCalls).toHaveLength(1)
      const ops = client.multiCalls[0]!
      expect(ops).toHaveLength(8)
      expect(cmdKeys(ops.slice(0, 5))).toEqual([
        ['sRem', K('pending')],
        ['zRem', K('maturing')],
        ['del', K('maturing:tx1')],
        ['sRem', K('limbo')],
        ['zAdd', K('tombstones')],
      ])
      expect(ops[4]!.args[1]).toEqual({ score: T, value: 'tx1' })
      expectEnqueued(ops, ev)
    })

    it('finishMaturing: DEL record, ZREM maturing, ZADD tombstones — one MULTI, no event', async () => {
      const { store, client } = mkStore()
      await store.finishMaturing('tx1', T)
      expect(client.multiCalls).toHaveLength(1)
      const ops = client.multiCalls[0]!
      expect(cmdKeys(ops)).toEqual([
        ['del', K('maturing:tx1')],
        ['zRem', K('maturing')],
        ['zAdd', K('tombstones')],
      ])
      expect(ops[2]!.args[1]).toEqual({ score: T, value: 'tx1' })
    })

    it('isTombstoned is ZSCORE non-nil; pruneTombstones is ZREMRANGEBYSCORE -inf beforeMs', async () => {
      const { store, client } = mkStore()
      await expect(store.isTombstoned('tx1')).resolves.toBe(false)
      client.zScore.mockResolvedValueOnce(T)
      await expect(store.isTombstoned('tx1')).resolves.toBe(true)
      expect(client.zScore).toHaveBeenCalledWith(K('tombstones'), 'tx1')
      await store.pruneTombstones(T - 3_600_000)
      expect(client.zRemRangeByScore).toHaveBeenCalledWith(K('tombstones'), '-inf', T - 3_600_000)
    })

    it('expireWatch: SREM addresses, ZREM expiries + enqueue expired', async () => {
      const { store, client } = mkStore()
      const ev: ExpiredEvent = {
        version: 1,
        event: 'expired',
        network: 'regtest',
        address: 'bcrt1qaddr',
        idempotencyKey: 'regtest:bcrt1qaddr:expired:1',
        timestamp: T,
      }
      await store.expireWatch('bcrt1qaddr', ev)
      expect(client.multiCalls).toHaveLength(1)
      const ops = client.multiCalls[0]!
      expect(ops).toHaveLength(5)
      expect(cmdKeys(ops.slice(0, 2))).toEqual([
        ['sRem', K('addresses')],
        ['zRem', K('expiries')],
      ])
      expect(ops[0]!.args[1]).toBe('bcrt1qaddr')
      expectEnqueued(ops, ev)
    })

    it('endTracking: SREM pending, SREM limbo, DEL record — nothing enqueued', async () => {
      const { store, client } = mkStore()
      await store.endTracking('tx1')
      expect(client.multiCalls).toHaveLength(1)
      expect(cmdKeys(client.multiCalls[0]!)).toEqual([
        ['sRem', K('pending')],
        ['sRem', K('limbo')],
        ['del', K('maturing:tx1')],
      ])
    })

    it('clearTracking never touches the outbox keys', async () => {
      const { store, client } = mkStore()
      const scanned: string[] = []
      ;(client as unknown as { scanIterator: unknown; del: unknown; sMembers: unknown; zRangeWithScores: unknown }).scanIterator = () => ({
        async *[Symbol.asyncIterator]() {
          yield K('maturing:tx1')
        },
      })
      ;(client as unknown as { del: (k: string | string[]) => Promise<number> }).del = async (k) => {
        scanned.push(...(Array.isArray(k) ? k : [k]))
        return 1
      }
      ;(client as unknown as { sMembers: () => Promise<string[]> }).sMembers = async () => []
      ;(client as unknown as { zRangeWithScores: () => Promise<unknown[]> }).zRangeWithScores = async () => []
      await store.clearTracking()
      expect(scanned).toContain(K('maturing:tx1'))
      expect(scanned.some((k) => k.includes('outbox'))).toBe(false)
    })
  })

  describe('outbox methods', () => {
    const T = 1_700_000_000_000
    const K = (name: string) => `weir:regtest:${name}`
    const cmdKeys = (ops: Array<{ cmd: string; args: unknown[] }>) => ops.map((o) => [o.cmd, o.args[0]])

    it('outboxDue: ZRANGEBYSCORE -inf now LIMIT 0 limit', async () => {
      const { store, client } = mkStore()
      client.zRangeByScore.mockResolvedValueOnce(['a', 'b'])
      await expect(store.outboxDue(T, 50)).resolves.toEqual(['a', 'b'])
      expect(client.zRangeByScore).toHaveBeenCalledWith(K('outbox'), '-inf', T, { LIMIT: { offset: 0, count: 50 } })
    })

    it('outboxRead: parses the hash; null when absent; corrupt (no payload) throws', async () => {
      const { store, client } = mkStore()
      const ev = { version: 1, event: 'expired', network: 'regtest', address: 'a', idempotencyKey: 'k', timestamp: T }
      client.hGetAll.mockResolvedValueOnce({ payload: JSON.stringify(ev), event: 'expired', idempotencyKey: 'k', attempts: '2', createdAt: String(T), lastError: 'HTTP 503' })
      await expect(store.outboxRead('id1')).resolves.toEqual({ event: ev, attempts: 2, createdAt: T, lastError: 'HTTP 503' })
      expect(client.hGetAll).toHaveBeenCalledWith(K('outbox:id1'))

      client.hGetAll.mockResolvedValueOnce({ payload: JSON.stringify(ev), event: 'expired', idempotencyKey: 'k', attempts: '0', createdAt: String(T) })
      await expect(store.outboxRead('id1')).resolves.toMatchObject({ attempts: 0, lastError: null })

      client.hGetAll.mockResolvedValueOnce({})
      await expect(store.outboxRead('gone')).resolves.toBeNull()

      client.hGetAll.mockResolvedValueOnce({ attempts: '1' })
      await expect(store.outboxRead('partial')).rejects.toThrow(/corrupt outbox record partial/)
    })

    it('outboxAck: DEL hash + ZREM queue + ZREM created, one MULTI', async () => {
      const { store, client } = mkStore()
      await store.outboxAck('id1')
      expect(client.multiCalls).toHaveLength(1)
      expect(cmdKeys(client.multiCalls[0]!)).toEqual([
        ['del', K('outbox:id1')],
        ['zRem', K('outbox')],
        ['zRem', K('outbox:created')],
      ])
      expect(client.multiCalls[0]![1]!.args[1]).toBe('id1')
    })

    it('outboxRetry: one Lua guarded by EXISTS — HSET attempts/lastError + ZADD next due; false when the hash is gone', async () => {
      const { store, client } = mkStore()
      await expect(store.outboxRetry('id1', T + 2000, 2, 'HTTP 503')).resolves.toBe(true)
      expect(client.multiCalls).toHaveLength(0)
      const [script, opts] = client.eval.mock.calls[0] as [string, { keys: string[]; arguments: string[] }]
      expect(script).toMatch(/^if redis\.call\('EXISTS', KEYS\[1\]\) == 0 then return 0 end/)
      expect(script).toMatch(/HSET.*KEYS\[1\].*'attempts', ARGV\[1\], 'lastError', ARGV\[2\]/)
      expect(script).toMatch(/ZADD.*KEYS\[2\], ARGV\[3\], ARGV\[4\]/)
      expect(opts.keys).toEqual([K('outbox:id1'), K('outbox')])
      expect(opts.arguments).toEqual(['2', 'HTTP 503', String(T + 2000), 'id1'])

      client.eval.mockResolvedValueOnce(0)
      await expect(store.outboxRetry('gone', T + 2000, 2, 'HTTP 503')).resolves.toBe(false)
    })

    it('outboxDead: ONE Lua — EXISTS guard, HSET, ZREM outbox + created, ZADD dead, then the cap (ZRANGE overflow, DEL hashes, ZREMRANGEBYRANK)', async () => {
      const { store, client } = mkStore()
      await expect(store.outboxDead('id1', T, 9, 'HTTP 503', 1000)).resolves.toBe(true)
      expect(client.multiCalls).toHaveLength(0)
      expect(client.zRange).not.toHaveBeenCalled()
      const [script, opts] = client.eval.mock.calls[0] as [string, { keys: string[]; arguments: string[] }]
      expect(script).toMatch(/^if redis\.call\('EXISTS', KEYS\[1\]\) == 0 then return 0 end/)
      expect(script).toMatch(/HSET.*KEYS\[1\].*'attempts', ARGV\[1\], 'lastError', ARGV\[2\]/)
      expect(script).toMatch(/ZREM.*KEYS\[2\], ARGV\[3\]/)
      expect(script).toMatch(/ZREM.*KEYS\[4\], ARGV\[3\]/)
      expect(script).toMatch(/ZADD.*KEYS\[3\], ARGV\[4\], ARGV\[3\]/)
      expect(script).toMatch(/ZCARD.*KEYS\[3\]/)
      expect(script).toMatch(/if n > max then.*ZRANGE.*KEYS\[3\], 0, n - max - 1.*DEL.*ARGV\[6\] \.\. dead.*ZREMRANGEBYRANK.*KEYS\[3\], 0, n - max - 1/s)
      expect(opts.keys).toEqual([K('outbox:id1'), K('outbox'), K('outbox:dead'), K('outbox:created')])
      expect(opts.arguments).toEqual(['9', 'HTTP 503', 'id1', String(T), '1000', K('outbox:')])

      client.eval.mockResolvedValueOnce(0)
      await expect(store.outboxDead('gone', T, 9, 'HTTP 503', 1000)).resolves.toBe(false)
    })

    it('outboxStats: ZCARD both sets; oldestCreatedAt = the lowest score of outbox:created (exact, O(1))', async () => {
      const { store, client } = mkStore()
      client.zCard.mockResolvedValueOnce(3).mockResolvedValueOnce(1)
      client.zRangeWithScores.mockResolvedValueOnce([{ value: 'a', score: T - 9000 }])
      await expect(store.outboxStats()).resolves.toEqual({ depth: 3, oldestCreatedAt: T - 9000, dead: 1 })
      expect(client.zRangeWithScores).toHaveBeenCalledWith(K('outbox:created'), 0, 0)
      expect(client.multiCalls).toHaveLength(0)
    })

    it('outboxStats on an empty outbox: depth 0, oldest null', async () => {
      const { store } = mkStore()
      await expect(store.outboxStats()).resolves.toEqual({ depth: 0, oldestCreatedAt: null, dead: 0 })
    })
  })
})
