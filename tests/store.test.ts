import type { EventEmitter } from 'node:events'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

/**
 * Store tests against a module-mocked `redis` client: the real `createClient` is replaced by
 * an EventEmitter with the few methods these paths use, so the reconnect-exhaustion and
 * 'end' handling, CONFIG GET error classification, and the pure parsing paths (records,
 * outbox hashes, event ids) are exercised without a redis server. Command semantics (the
 * MULTIs) are tests/integration/store.redis.test.ts and E2E territory.
 */
interface FakeClient extends EventEmitter {
  options: { socket: { reconnectStrategy: (retries: number, cause: Error) => number | Error } }
  connect: ReturnType<typeof vi.fn>
  quit: ReturnType<typeof vi.fn>
  configGet: ReturnType<typeof vi.fn>
  hGetAll: ReturnType<typeof vi.fn>
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
    hGetAll = vi.fn(async () => ({}))
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

import { makeEventId, Store } from '../src/store/redis'

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
        "NOPERM this user has no permissions to run the 'config|get' command",
        'ERR CONFIG GET is not allowed',
      ]) {
        const { store, client } = mkStore()
        client.configGet.mockRejectedValueOnce(new Error(msg))
        expect(await store.maxmemoryPolicy()).toBeNull()
      }
    })
  })

  describe('parsing', () => {
    const T = 1_700_000_000_000
    const INPUTS = [
      { txid: 'p1', vout: 0 },
      { txid: 'p2', vout: 3 },
    ]
    /** makeEventId: <nowMs padded 15>-<seq padded 8>-<8 hex> — monotonic so equal-score ties sort in enqueue order */
    const EVENT_ID = /^\d{15}-\d{8}-[0-9a-f]{8}$/

    it('makeEventId: monotonic within a millisecond; the sequence restarts at 0 on the next millisecond', () => {
      const ids = [makeEventId(T), makeEventId(T), makeEventId(T)]
      for (const id of ids) expect(id).toMatch(EVENT_ID)
      expect([...ids].sort()).toEqual(ids)
      expect(ids.map((id) => id.split('-')[1])).toEqual(['00000000', '00000001', '00000002'])
      const next = makeEventId(T + 1)
      expect(next > ids[2]!).toBe(true)
      expect(next.split('-')[1]).toBe('00000000')
      expect(makeEventId(T + 1).split('-')[1]).toBe('00000001')
    })

    it('readRecord: a record written before outpoint tracking (no `inputs` field) reads with inputs []; a partial hash throws', async () => {
      const { store, client } = mkStore()
      client.hGetAll.mockResolvedValueOnce({ height: '101', blockHash: 'b101', matched: '[]', fired: '[1]', hex: 'hex-tx1' })
      await expect(store.readRecord('tx1')).resolves.toEqual({ txid: 'tx1', height: 101, blockHash: 'b101', matched: [], fired: [1], hex: 'hex-tx1', inputs: [] })
      client.hGetAll.mockResolvedValueOnce({ height: '101', blockHash: 'b101', matched: '[]', fired: '[1]', hex: 'hex-tx1', inputs: JSON.stringify(INPUTS) })
      await expect(store.readRecord('tx1')).resolves.toMatchObject({ inputs: INPUTS })
      client.hGetAll.mockResolvedValueOnce({})
      await expect(store.readRecord('gone')).resolves.toBeNull()
      client.hGetAll.mockResolvedValueOnce({ fired: '[1]' })
      await expect(store.readRecord('partial')).rejects.toThrow(/corrupt maturing record for partial/)
    })

    it('outboxRead: parses the hash; null when absent; corrupt (no payload) throws', async () => {
      const { store, client } = mkStore()
      const ev = { version: 1, event: 'expired', network: 'regtest', address: 'a', idempotencyKey: 'k', timestamp: T }
      client.hGetAll.mockResolvedValueOnce({ payload: JSON.stringify(ev), event: 'expired', idempotencyKey: 'k', attempts: '2', createdAt: String(T), lastError: 'HTTP 503' })
      await expect(store.outboxRead('id1')).resolves.toEqual({ event: ev, attempts: 2, createdAt: T, lastError: 'HTTP 503' })
      expect(client.hGetAll).toHaveBeenCalledWith('weir:regtest:outbox:id1')

      client.hGetAll.mockResolvedValueOnce({ payload: JSON.stringify(ev), event: 'expired', idempotencyKey: 'k', attempts: '0', createdAt: String(T) })
      await expect(store.outboxRead('id1')).resolves.toMatchObject({ attempts: 0, lastError: null })

      client.hGetAll.mockResolvedValueOnce({})
      await expect(store.outboxRead('gone')).resolves.toBeNull()

      client.hGetAll.mockResolvedValueOnce({ attempts: '1' })
      await expect(store.outboxRead('partial')).rejects.toThrow(/corrupt outbox record partial/)
    })
  })
})
