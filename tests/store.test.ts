import type { EventEmitter } from 'node:events'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

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
  /** every MULTI chain that reached exec(), in order, as recorded command sequences */
  multiCalls: Array<Array<{ cmd: string; args: unknown[] }>>
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
    multiCalls: Array<Array<{ cmd: string; args: unknown[] }>> = []
    /** chainable recorder: exec() commits the recorded ops as ONE transaction */
    multi = () => {
      const ops: Array<{ cmd: string; args: unknown[] }> = []
      const rec: Record<string, unknown> = {
        exec: async () => {
          this.multiCalls.push(ops)
          return []
        },
      }
      for (const cmd of ['hSet', 'zAdd', 'sAdd', 'sRem', 'del', 'zRem', 'sDiffStore', 'sInterStore']) {
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
  // (e.g. dropping SADD evaluated) is invisible to them. These pin the exact command chain
  // each atomic transition sends, and that it is ONE transaction.
  describe('atomic transitions send exactly one MULTI with the contracted commands', () => {
    const rec = {
      txid: 'tx1',
      height: 101,
      blockHash: 'b101',
      matched: [{ address: 'bcrt1qaddr', vout: 0, valueSats: 5000 }],
      fired: [1],
      hex: 'hex-tx1',
    }
    const K = (name: string) => `weir:regtest:${name}`

    it('promoteToMaturing: HSET record, ZADD maturing, SREM pending, SREM limbo, SADD evaluated', async () => {
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

    it('demoteToPending: HSET record at height 0, SADD pending, SADD evaluated, SREM limbo', async () => {
      const { store, client } = mkStore()
      await store.demoteToPending(rec)
      expect(client.multiCalls).toHaveLength(1)
      const ops = client.multiCalls[0]!
      expect(ops.map((o) => [o.cmd, o.args[0]])).toEqual([
        ['hSet', K('maturing:tx1')],
        ['sAdd', K('pending')],
        ['sAdd', K('evaluated')],
        ['sRem', K('limbo')],
      ])
      // demotion resets inclusion + fired but keeps matched/hex so a later dropped/re-mine has data
      expect(ops[0]!.args[1]).toMatchObject({ height: '0', blockHash: '', fired: '[]', hex: 'hex-tx1' })
    })
  })
})
