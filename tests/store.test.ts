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
  hmGet: ReturnType<typeof vi.fn>
  sMembers: ReturnType<typeof vi.fn>
  del: ReturnType<typeof vi.fn>
  scanIterator: (opts: { MATCH: string; COUNT: number }) => AsyncIterable<string>
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
    hmGet = vi.fn(async (_key: string, fields: string[]) => fields.map(() => null))
    sMembers = vi.fn(async () => [])
    del = vi.fn(async () => 1)
    scanIterator = () => ({
      async *[Symbol.asyncIterator]() {},
    })
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
      for (const cmd of ['hSet', 'hGet', 'hDel', 'zAdd', 'sAdd', 'sRem', 'sMembers', 'del', 'zRem', 'sDiffStore', 'sInterStore']) {
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
  // (e.g. dropping SADD evaluated, the outbox enqueue, or the outpoint HDEL) is invisible to
  // them. These pin the exact command chain each atomic transition sends, and that it is ONE
  // transaction that carries the event with it.
  describe('atomic transitions send exactly one MULTI with the contracted commands', () => {
    const T = 1_700_000_000_000
    const INPUTS = [
      { txid: 'p1', vout: 0 },
      { txid: 'p2', vout: 3 },
    ]
    const FIELDS = ['p1:0', 'p2:3']
    const rec = {
      txid: 'tx1',
      height: 101,
      blockHash: 'b101',
      matched: [{ address: 'bcrt1qaddr', vout: 0, valueSats: 5000 }],
      fired: [1],
      hex: 'hex-tx1',
      inputs: INPUTS,
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

    it('promoteToMaturing: HSET record, ZADD maturing, SREM pending, SREM limbo, SADD evaluated, SADD into each prevout SET — no event', async () => {
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
        ['sAdd', K('outpoint:p1:0')],
        ['sAdd', K('outpoint:p2:3')],
      ])
      expect(ops[0]!.args[1]).toMatchObject({ height: '101', blockHash: 'b101', fired: '[1]', hex: 'hex-tx1', inputs: JSON.stringify(INPUTS) })
      expect(ops[1]!.args[1]).toEqual({ score: 101, value: 'tx1' })
      expect(ops[5]!.args[1]).toBe('tx1')
      expect(ops[6]!.args[1]).toBe('tx1')
    })

    it('promoteToMaturing of a tx with no inputs (coinbase-like) sends no claim', async () => {
      const { store, client } = mkStore()
      await store.promoteToMaturing({ ...rec, inputs: [] })
      expect(cmdKeys(client.multiCalls[0]!)).toHaveLength(5)
      expect(client.multiCalls[0]!.some((o) => String(o.args[0]).includes('outpoint'))).toBe(false)
    })

    /** The shared Lua prelude every releasing/enqueuing script starts with. */
    const LIVE_STATE_SNIPPET =
      /local function pendingUnmined\(pending, recordPrefix, txid\)\s+if redis\.call\('SISMEMBER', pending, txid\) == 0 then return false end\s+local h = redis\.call\('HGET', recordPrefix \.\. txid, 'height'\)\s+return h and tonumber\(h\) == 0\s+end/
    const RELEASE_SNIPPET =
      /local function release\(record, outpointPrefix, txid\)\s+local inputs = redis\.call\('HGET', record, 'inputs'\)\s+if not inputs then return end\s+for _, o in ipairs\(cjson\.decode\(inputs\)\) do redis\.call\('SREM', outpointPrefix \.\. o\.txid \.\. ':' \.\. o\.vout, txid\) end\s+end/
    const ENQUEUE_SNIPPET =
      /local function enqueue\(outbox, created, rec, id, payload, event, key, now\)\s+redis\.call\('HSET', rec, 'payload', payload, 'event', event, 'idempotencyKey', key, 'attempts', '0', 'createdAt', now\)\s+redis\.call\('ZADD', outbox, now, id\)\s+redis\.call\('ZADD', created, now, id\)/

    type Eval = [string, { keys: string[]; arguments: string[] }]
    /** the one EVAL a transition sent: no MULTI at all */
    function onlyEval(client: FakeClient): Eval {
      expect(client.multiCalls).toHaveLength(0)
      expect(client.eval).toHaveBeenCalledTimes(1)
      return client.eval.mock.calls[0] as Eval
    }
    /** an enqueuing script: the outbox-record key carries a fresh monotonic id, and the ARGV tail is [id, payload, event, idempotencyKey] at `at` */
    function expectLuaEnqueued([, opts]: Eval, recordKeyIdx: number, at: number, ev: TxEvent | ExpiredEvent): string {
      const m = /^weir:regtest:outbox:(.+)$/.exec(opts.keys[recordKeyIdx]!)
      expect(m, `outbox hash key at KEYS[${recordKeyIdx + 1}], got ${opts.keys[recordKeyIdx]}`).not.toBeNull()
      const id = m![1]!
      expect(id).toMatch(EVENT_ID)
      expect(id.startsWith(String(T).padStart(15, '0'))).toBe(true)
      expect(opts.arguments.slice(at, at + 4)).toEqual([id, JSON.stringify(ev), ev.event, ev.idempotencyKey])
      return id
    }
    /** claims are SADD, releases SREM of self: no script owns, forces, waits or hands over anything */
    function expectSetModelOnly(script: string): void {
      expect(script).not.toMatch(/HSETNX|HDEL|waitFor|waiting|handed|force/)
    }

    describe('recordSeen is ONE guarded Lua script (no MULTI)', () => {
      const seenRec = { ...rec, height: 0, blockHash: '', fired: [] }
      const seen: TxEvent = { ...txEvent('seen', 'regtest:tx1:seen'), blockHeight: null, blockHash: null }

      it('with an event: fence, guards, HSET record, SADD pending, SADD evaluated, SADD claims, enqueue — one EVAL', async () => {
        const { store, client } = mkStore()
        await expect(store.recordSeen(seenRec, seen, T - 5000)).resolves.toBe('recorded')

        const [script, opts] = onlyEval(client)
        expect(script).toMatch(LIVE_STATE_SNIPPET)
        expect(script).toMatch(RELEASE_SNIPPET)
        expect(script).toMatch(ENQUEUE_SNIPPET)
        expectSetModelOnly(script)
        // the fence comes first and is atomic with the guards: now − startedAt > maxAge → -1
        expect(script).toMatch(/if tonumber\(ARGV\[11\]\) - tonumber\(ARGV\[12\]\) > tonumber\(ARGV\[13\]\) then return -1 end/)
        // the guards: an already-evaluated txid, or a record already mined (height > 0), is a no-op
        expect(script).toMatch(/SISMEMBER.*KEYS\[3\].*return 0/s)
        expect(script).toMatch(/HGET.*KEYS\[1\].*'height'.*tonumber\(h\) > 0 then return 0/s)
        expect(script).toMatch(/ZSCORE.*KEYS\[6\].*then return 0/s) // tombstoned → no resurrection
        // the retirement watermark: an evaluation that started at or before the txid's last drop/replacement → -3 (stale)
        expect(script).toMatch(/local exitAt = redis\.call\('ZSCORE', KEYS\[8\], ARGV\[1\]\)\s+if exitAt and tonumber\(ARGV\[12\]\) <= tonumber\(exitAt\) then return -3 end/)
        expect(script).toMatch(/HSET.*KEYS\[1\].*'inputs', ARGV\[14\]/)
        expect(script).toMatch(/SADD.*KEYS\[2\]/)
        expect(script).toMatch(/SADD.*KEYS\[3\]/)
        // claims: SADD this txid into each prevout's SET, key built from the prefix (single-instance redis)
        expect(script).toMatch(/for field in string\.gmatch\(ARGV\[15\], '\[\^,\]\+'\) do redis\.call\('SADD', ARGV\[16\] \.\. field, ARGV\[1\]\) end/)
        expect(script).toMatch(/if ARGV\[7\] ~= '' then enqueue\(KEYS\[4\], KEYS\[7\], KEYS\[5\], ARGV\[7\], ARGV\[8\], ARGV\[9\], ARGV\[10\], ARGV\[11\]\) end/)
        expect(opts.keys).toHaveLength(8)
        expect(opts.keys.slice(0, 4)).toEqual([K('maturing:tx1'), K('pending'), K('evaluated'), K('outbox')])
        expect(opts.keys.slice(5)).toEqual([K('tombstones'), K('outbox:created'), K('retired')])
        const id = expectLuaEnqueued([script, opts], 4, 6, seen)
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
          JSON.stringify(INPUTS),
          'p1:0,p2:3',
          K('outpoint:'),
        ])
      })

      it('a tx with no inputs passes an empty field list (the Lua loop is a no-op)', async () => {
        const { store, client } = mkStore()
        await store.recordSeen({ ...seenRec, inputs: [] }, seen, T)
        const [, opts] = client.eval.mock.calls[0] as Eval
        expect(opts.arguments.slice(13)).toEqual(['[]', '', K('outpoint:')])
      })

      it('a stale evaluation (script returns -1) resolves `stale` and warns with the age', async () => {
        const { store, client } = mkStore()
        client.eval.mockResolvedValueOnce(-1)
        await expect(store.recordSeen(seenRec, seen, T - 700_000)).resolves.toBe('stale')
        expect(warnLog.mock.calls.some((c) => /refused stale evaluation of tx1: started 700000ms ago \(fence 600000ms\)/.test(String(c[0])))).toBe(true)
      })

      it('an evaluation that predates the txid\'s retirement (script returns -3) resolves `stale` with its own warn', async () => {
        const { store, client } = mkStore()
        client.eval.mockResolvedValueOnce(-3)
        await expect(store.recordSeen(seenRec, seen, T - 50)).resolves.toBe('stale')
        expect(warnLog.mock.calls.some((c) => /refused evaluation of tx1 that predates its last drop\/replacement \(started 50ms ago\)/.test(String(c[0])))).toBe(true)
      })

      it('without an event (seen disabled): no outbox key, empty event arguments', async () => {
        const { store, client } = mkStore()
        await store.recordSeen(seenRec, null, T)
        const [, opts] = client.eval.mock.calls[0] as Eval
        expect(opts.keys).toEqual([K('maturing:tx1'), K('pending'), K('evaluated'), K('outbox'), K('outbox'), K('tombstones'), K('outbox:created'), K('retired')])
        expect(opts.arguments.slice(6, 10)).toEqual(['', '', '', ''])
        expect(opts.arguments[10]).toBe(String(T))
        expect(opts.arguments.slice(13)).toEqual([JSON.stringify(INPUTS), 'p1:0,p2:3', K('outpoint:')])
      })

      it('resolves `skipped` (no warn) when the script reports a guard fired', async () => {
        const { store, client } = mkStore()
        client.eval.mockResolvedValueOnce(0)
        await expect(store.recordSeen(seenRec, seen, T)).resolves.toBe('skipped')
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

    it('dropPending: ONE GUARDED Lua — pending-unmined else 0 (nothing written); SREM pending, SREM evaluated, release own claims, DEL record, ZADD retired, enqueue dropped; no tombstone; boolean', async () => {
      const { store, client } = mkStore()
      const ev = { ...txEvent('dropped', 'regtest:tx1:dropped:101'), reason: 'evicted' as const }
      await expect(store.dropPending('tx1', ev)).resolves.toBe(true)
      const [script, opts] = onlyEval(client)
      expect(script).toMatch(LIVE_STATE_SNIPPET)
      expect(script).toMatch(RELEASE_SNIPPET)
      expectSetModelOnly(script)
      expect(script).toMatch(
        /^local function.*\nif not pendingUnmined\(KEYS\[2\], ARGV\[7\], ARGV\[1\]\) then return 0 end\s+redis\.call\('SREM', KEYS\[2\], ARGV\[1\]\)\s+redis\.call\('SREM', KEYS\[3\], ARGV\[1\]\)\s+release\(KEYS\[1\], ARGV\[8\], ARGV\[1\]\)\s+redis\.call\('DEL', KEYS\[1\]\)\s+redis\.call\('ZADD', KEYS\[7\], ARGV\[6\], ARGV\[1\]\)\s+enqueue\(KEYS\[4\], KEYS\[6\], KEYS\[5\], ARGV\[2\], ARGV\[3\], ARGV\[4\], ARGV\[5\], ARGV\[6\]\)\s+return 1$/s,
      )
      expect(script).not.toMatch(/tonumber\(ARGV\[6\]\) - tonumber/) // no fence: the block path is serialized
      expect(opts.keys).toHaveLength(7)
      expect(opts.keys.some((k) => k === K('tombstones'))).toBe(false) // a rebroadcast may re-fire seen
      expect(opts.keys.slice(0, 4)).toEqual([K('maturing:tx1'), K('pending'), K('evaluated'), K('outbox')])
      expect(opts.keys.slice(5)).toEqual([K('outbox:created'), K('retired')]) // the watermark, not a tombstone
      expectLuaEnqueued([script, opts], 4, 1, ev)
      expect(opts.arguments[0]).toBe('tx1')
      expect(opts.arguments.slice(5)).toEqual([String(T), K('maturing:'), K('outpoint:')])

      client.eval.mockResolvedValueOnce(0) // already replaced meanwhile: nothing written, false
      await expect(store.dropPending('tx1', ev)).resolves.toBe(false)
    })

    it('replacePending: ONE Lua — the SPENDER\'s own retirement/tombstone first (-3 → stale), then the fence (-1 → stale), then the pending-unmined guard (record must EXIST with height 0, else 0 → skipped), then the dropPending mutation + ZADD retired + enqueue → replaced', async () => {
      const { store, client } = mkStore()
      const ev = { ...txEvent('dropped', 'regtest:tx1:dropped:replaced:tx2'), reason: 'replaced' as const, replacedBy: 'tx2' }
      await expect(store.replacePending('tx1', ev, T - 5000, 'tx2')).resolves.toBe('replaced')
      const [script, opts] = onlyEval(client)
      expect(script).toMatch(LIVE_STATE_SNIPPET)
      expect(script).toMatch(RELEASE_SNIPPET)
      expectSetModelOnly(script)
      // the destructive step carries its own fence, atomically, before any read is trusted
      expect(script).toMatch(
        /^local function.*\nlocal spenderExit = redis\.call\('ZSCORE', KEYS\[7\], ARGV\[11\]\)\s+if spenderExit and tonumber\(ARGV\[7\]\) <= tonumber\(spenderExit\) then return -3 end\s+if redis\.call\('ZSCORE', KEYS\[8\], ARGV\[11\]\) then return -3 end\s+if tonumber\(ARGV\[6\]\) - tonumber\(ARGV\[7\]\) > tonumber\(ARGV\[8\]\) then return -1 end\s+if not pendingUnmined\(KEYS\[2\], ARGV\[9\], ARGV\[1\]\) then return 0 end\s+redis\.call\('SREM', KEYS\[2\], ARGV\[1\]\)\s+redis\.call\('SREM', KEYS\[3\], ARGV\[1\]\)\s+release\(KEYS\[1\], ARGV\[10\], ARGV\[1\]\)\s+redis\.call\('DEL', KEYS\[1\]\)\s+redis\.call\('ZADD', KEYS\[7\], ARGV\[6\], ARGV\[1\]\)\s+enqueue\(KEYS\[4\], KEYS\[6\], KEYS\[5\], ARGV\[2\], ARGV\[3\], ARGV\[4\], ARGV\[5\], ARGV\[6\]\)\s+return 1$/s,
      )
      expect(opts.keys).toHaveLength(8)
      expect(opts.keys.slice(0, 4)).toEqual([K('maturing:tx1'), K('pending'), K('evaluated'), K('outbox')])
      expect(opts.keys.slice(5)).toEqual([K('outbox:created'), K('retired'), K('tombstones')]) // tombstones only READ (the spender's)
      expectLuaEnqueued([script, opts], 4, 1, ev)
      expect(opts.arguments.slice(5)).toEqual([String(T), String(T - 5000), '600000', K('maturing:'), K('outpoint:'), 'tx2'])
      const hash = JSON.parse(opts.arguments[2]!) as Record<string, unknown>
      expect(hash).toMatchObject({ event: 'dropped', reason: 'replaced', replacedBy: 'tx2' })

      client.eval.mockResolvedValueOnce(0) // no longer pending / already mined: nothing written, no warn
      await expect(store.replacePending('tx1', ev, T, 'tx2')).resolves.toBe('skipped')
      expect(warnLog).not.toHaveBeenCalled()

      client.eval.mockResolvedValueOnce(-1) // stale: nothing written, warned
      await expect(store.replacePending('tx1', ev, T - 700_000, 'tx2')).resolves.toBe('stale')
      expect(warnLog.mock.calls.some((c) => /refused stale replacement of tx1 by tx2: evaluation started 700000ms ago/.test(String(c[0])))).toBe(true)

      client.eval.mockResolvedValueOnce(-3) // the spender's own evaluation predates its exit (or it is tombstoned): stale, warned
      await expect(store.replacePending('tx1', ev, T - 50, 'tx2')).resolves.toBe('stale')
      expect(warnLog.mock.calls.some((c) => /refused replacement of tx1 by tx2: the evaluation of tx2 \(started 50ms ago\) predates its own drop\/replacement, or it is tombstoned/.test(String(c[0])))).toBe(true)
    })

    it('conflict: ONE Lua — SREM pending, ZREM maturing, release own claims, DEL record, SREM limbo, ZADD tombstones, enqueue conflicted', async () => {
      const { store, client } = mkStore()
      const ev = txEvent('conflicted', 'regtest:tx1:conflicted')
      await store.conflict('tx1', ev)
      const [script, opts] = onlyEval(client)
      expect(script).toMatch(RELEASE_SNIPPET)
      expectSetModelOnly(script)
      expect(script).toMatch(
        /redis\.call\('SREM', KEYS\[2\], ARGV\[1\]\)\s+redis\.call\('ZREM', KEYS\[3\], ARGV\[1\]\)\s+release\(KEYS\[1\], ARGV\[7\], ARGV\[1\]\)\s+redis\.call\('DEL', KEYS\[1\]\)\s+redis\.call\('SREM', KEYS\[4\], ARGV\[1\]\)\s+redis\.call\('ZADD', KEYS\[5\], ARGV\[6\], ARGV\[1\]\)\s+enqueue\(KEYS\[6\], KEYS\[8\], KEYS\[7\], ARGV\[2\], ARGV\[3\], ARGV\[4\], ARGV\[5\], ARGV\[6\]\)\s+return 1$/,
      )
      expect(opts.keys.slice(0, 6)).toEqual([K('maturing:tx1'), K('pending'), K('maturing'), K('limbo'), K('tombstones'), K('outbox')])
      expect(opts.keys[7]).toBe(K('outbox:created'))
      expect(opts.keys).toHaveLength(8)
      expectLuaEnqueued([script, opts], 6, 1, ev)
      expect(opts.arguments[0]).toBe('tx1')
      expect(opts.arguments.slice(5)).toEqual([String(T), K('outpoint:')]) // the tombstone score, the claim prefix
    })

    it('finishMaturing: ONE Lua — release own claims, DEL record, ZREM maturing, ZADD tombstones — no event', async () => {
      const { store, client } = mkStore()
      await store.finishMaturing('tx1', T)
      const [script, opts] = onlyEval(client)
      expect(script).toMatch(RELEASE_SNIPPET)
      expect(script).toMatch(
        /release\(KEYS\[1\], ARGV\[3\], ARGV\[1\]\)\s+redis\.call\('DEL', KEYS\[1\]\)\s+redis\.call\('ZREM', KEYS\[2\], ARGV\[1\]\)\s+redis\.call\('ZADD', KEYS\[3\], ARGV\[2\], ARGV\[1\]\)\s+return 1$/,
      )
      expect(script).not.toMatch(/enqueue\(KEYS/)
      expect(opts.keys).toEqual([K('maturing:tx1'), K('maturing'), K('tombstones')])
      expect(opts.arguments).toEqual(['tx1', String(T), K('outpoint:')])
    })

    it('endTracking: ONE Lua — SREM pending, SREM limbo, release own claims, DEL record — nothing enqueued', async () => {
      const { store, client } = mkStore()
      await store.endTracking('tx1')
      const [script, opts] = onlyEval(client)
      expect(script).toMatch(RELEASE_SNIPPET)
      expect(script).toMatch(
        /redis\.call\('SREM', KEYS\[2\], ARGV\[1\]\)\s+redis\.call\('SREM', KEYS\[3\], ARGV\[1\]\)\s+release\(KEYS\[1\], ARGV\[2\], ARGV\[1\]\)\s+redis\.call\('DEL', KEYS\[1\]\)\s+return 1$/,
      )
      expect(script).not.toMatch(/enqueue\(KEYS/)
      expect(opts.keys).toEqual([K('maturing:tx1'), K('pending'), K('limbo')])
      expect(opts.arguments).toEqual(['tx1', K('outpoint:')])
    })

    it('outpointOwners: pipelined SMEMBERS per prevout chunked at 1000; prevouts with claimants only — including one beyond the first chunk; no round trip for []', async () => {
      const { store, client } = mkStore()
      await expect(store.outpointOwners([])).resolves.toEqual(new Map())
      expect(client.multiCalls).toHaveLength(0)

      // the mock returns the same replies for every exec: position 1 of each chunk has claimants
      client.execReplies = Array.from({ length: 1000 }, (_, i) => (i === 1 ? ['A', 'B'] : []))
      const many = Array.from({ length: 1500 }, (_, i) => ({ txid: `x${i}`, vout: i }))
      const owners = await store.outpointOwners(many)

      expect(client.multiCalls).toHaveLength(2)
      expect(client.multiCalls[0]).toHaveLength(1000)
      expect(client.multiCalls[1]).toHaveLength(500)
      expect(client.multiCalls[0]!.every((o) => o.cmd === 'sMembers')).toBe(true)
      expect(client.multiCalls[0]![1]!.args[0]).toBe(K('outpoint:x1:1'))
      expect(client.multiCalls[1]![1]!.args[0]).toBe(K('outpoint:x1001:1001'))
      expect(owners).toEqual(new Map([['x1:1', ['A', 'B']], ['x1001:1001', ['A', 'B']]])) // served by the SECOND pipeline
    })

    it('readRecord: a record written before outpoint tracking (no `inputs` field) reads with inputs []', async () => {
      const { store, client } = mkStore()
      client.hGetAll.mockResolvedValueOnce({ height: '101', blockHash: 'b101', matched: '[]', fired: '[1]', hex: 'hex-tx1' })
      await expect(store.readRecord('tx1')).resolves.toEqual({ txid: 'tx1', height: 101, blockHash: 'b101', matched: [], fired: [1], hex: 'hex-tx1', inputs: [] })
      client.hGetAll.mockResolvedValueOnce({ height: '101', blockHash: 'b101', matched: '[]', fired: '[1]', hex: 'hex-tx1', inputs: JSON.stringify(INPUTS) })
      await expect(store.readRecord('tx1')).resolves.toMatchObject({ inputs: INPUTS })
    })

    it('isTombstoned is ZSCORE non-nil; pruneTombstones is ZREMRANGEBYSCORE -inf beforeMs', async () => {
      const { store, client } = mkStore()
      await expect(store.isTombstoned('tx1')).resolves.toBe(false)
      client.zScore.mockResolvedValueOnce(T)
      await expect(store.isTombstoned('tx1')).resolves.toBe(true)
      expect(client.zScore).toHaveBeenCalledWith(K('tombstones'), 'tx1')
      await store.pruneTombstones(T - 3_600_000)
      expect(client.zRemRangeByScore).toHaveBeenCalledWith(K('tombstones'), '-inf', T - 3_600_000)
      await store.pruneRetired(T - 3_600_000)
      expect(client.zRemRangeByScore).toHaveBeenCalledWith(K('retired'), '-inf', T - 3_600_000)
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

    it('clearTracking: every outpoint:* SET is SCAN-DELeted FIRST, then the lost txids are collected and ONE Lua DELs their records AND every tracking key, then the CONDITIONAL orphan sweep; the outbox is never touched', async () => {
      const { store, client } = mkStore()
      client.sMembers.mockResolvedValueOnce(['limbo1']).mockResolvedValueOnce(['pend1', 'limbo1'])
      client.zRangeWithScores.mockResolvedValueOnce([{ value: 'mat1', score: 100 }])
      const scans: string[] = []
      client.scanIterator = (opts: { MATCH: string }) => {
        scans.push(opts.MATCH)
        return {
          async *[Symbol.asyncIterator]() {
            if (opts.MATCH === K('maturing:*')) {
              yield K('maturing:orphan')
              yield K('maturing:fresh')
            } else {
              yield K('outpoint:p1:0')
              yield K('outpoint:p2:3')
            }
          },
        }
      }
      const order: string[] = []
      client.del.mockImplementation(async (k: string) => {
        order.push(`del:${k}`)
        return 1
      })
      client.eval.mockImplementation(async (script: string) => {
        const tracking = script.includes("redis.call('DEL', unpack(KEYS))")
        order.push(tracking ? 'lua:tracking' : 'lua:sweep')
        return tracking ? 3 : 0
      })

      await expect(store.clearTracking()).resolves.toEqual(['limbo1', 'pend1', 'mat1'])

      expect(client.multiCalls).toHaveLength(0)
      expect(client.eval).toHaveBeenCalledTimes(3)
      // the ORDER: claims gone before the Lua, sweep after it — a recordSeen landing after the Lua keeps everything
      expect(order).toEqual([`del:${K('outpoint:p1:0')}`, `del:${K('outpoint:p2:3')}`, 'lua:tracking', 'lua:sweep', 'lua:sweep'])
      const [script, opts] = client.eval.mock.calls[0] as Eval
      // every collected txid: release its OWN claims (from its record) then DEL the record; then DEL the tracking keys
      expect(script).toMatch(RELEASE_SNIPPET)
      expect(script).toMatch(
        /for i = 3, #ARGV do\s+release\(ARGV\[1\] \.\. ARGV\[i\], ARGV\[2\], ARGV\[i\]\)\s+redis\.call\('DEL', ARGV\[1\] \.\. ARGV\[i\]\)\s+end\s+redis\.call\('DEL', unpack\(KEYS\)\)\s+return #ARGV - 2$/,
      )
      expect(opts.keys).toEqual([K('maturing'), K('pending'), K('limbo'), K('evaluated'), K('mempool:current'), K('mempool:postBlock'), K('block:txids')])
      expect(opts.arguments).toEqual([K('maturing:'), K('outpoint:'), 'limbo1', 'pend1', 'mat1'])
      expect(opts.keys.some((k) => k.includes('outbox'))).toBe(false)
      // the sweep: one Lua per scanned record that releases its own claims and deletes it ONLY while it has no live membership
      const SWEEP =
        /^local function.*\nif redis\.call\('SISMEMBER', KEYS\[2\], ARGV\[1\]\) == 1 then return 0 end\s+if redis\.call\('SISMEMBER', KEYS\[3\], ARGV\[1\]\) == 1 then return 0 end\s+if redis\.call\('ZSCORE', KEYS\[4\], ARGV\[1\]\) then return 0 end\s+release\(KEYS\[1\], ARGV\[2\], ARGV\[1\]\)\s+redis\.call\('DEL', KEYS\[1\]\)\s+return 1$/s
      const [s1, o1] = client.eval.mock.calls[1] as Eval
      expect(s1).toMatch(SWEEP)
      expect(s1).toMatch(RELEASE_SNIPPET)
      expect(o1).toEqual({ keys: [K('maturing:orphan'), K('pending'), K('limbo'), K('maturing')], arguments: ['orphan', K('outpoint:')] })
      const [s2, o2] = client.eval.mock.calls[2] as Eval
      expect(s2).toMatch(SWEEP)
      expect(o2).toEqual({ keys: [K('maturing:fresh'), K('pending'), K('limbo'), K('maturing')], arguments: ['fresh', K('outpoint:')] })
      // the claimant SETs were scanned BEFORE the records: one DEL each — never a record, never the outbox
      expect(scans).toEqual([K('outpoint:*'), K('maturing:*')])
      expect(client.del).toHaveBeenCalledTimes(2)
      expect(client.del).toHaveBeenNthCalledWith(1, K('outpoint:p1:0'))
      expect(client.del).toHaveBeenNthCalledWith(2, K('outpoint:p2:3'))
    })

    it('sweepOrphanRecords is callable on its own and returns the number deleted', async () => {
      const { store, client } = mkStore()
      client.scanIterator = () => ({
        async *[Symbol.asyncIterator]() {
          yield K('maturing:a')
          yield K('maturing:b')
        },
      })
      client.eval.mockResolvedValueOnce(1).mockResolvedValueOnce(0)
      await expect(store.sweepOrphanRecords()).resolves.toBe(1)
      expect(client.del).not.toHaveBeenCalled()
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
