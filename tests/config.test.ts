import { describe, expect, it } from 'vitest'
import { loadConfig } from '../src/config'

const FULL: NodeJS.ProcessEnv = {
  NETWORK: 'regtest',
  BITCOIN_RPC_URL: 'http://weir:pw@bitcoind:18443',
  BITCOIN_ZMQ_URL: 'tcp://bitcoind:28332',
  REDIS_URL: 'redis://redis:6379',
  WEBHOOK_URL: 'https://example.com/hook',
  WEBHOOK_SECRET: 's3cret',
}

const REQUIRED = ['NETWORK', 'BITCOIN_RPC_URL', 'BITCOIN_ZMQ_URL', 'REDIS_URL', 'WEBHOOK_URL', 'WEBHOOK_SECRET'] as const

function env(overrides: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  return { ...FULL, ...overrides }
}

describe('loadConfig', () => {
  it('loads a full valid env with the documented defaults', () => {
    const cfg = loadConfig(env())

    expect(cfg).toEqual({
      network: 'regtest',
      bitcoinRpcUrl: 'http://weir:pw@bitcoind:18443',
      bitcoinZmqUrl: 'tcp://bitcoind:28332',
      redisUrl: 'redis://redis:6379',
      webhookUrl: 'https://example.com/hook',
      webhookSecret: 's3cret',
      milestones: [0, 1, 3],
      confirmMilestones: [1, 3],
      seenEnabled: true,
      maxMilestone: 3,
      ringSize: 12,
      watchDefaultTtl: 0,
      heartbeatInterval: 0,
      adminToken: null,
      adminPort: 8787,
      webhookTimeoutMs: 10000,
      outboxMaxAgeSec: 259200,
      outboxDeadMax: 1000,
      readyMaxLag: 2,
    })
  })

  it('WEBHOOK_MAX_RETRIES no longer exists: retry is the outbox\'s job, bounded by OUTBOX_MAX_AGE', () => {
    const cfg = loadConfig(env({ WEBHOOK_MAX_RETRIES: '5' })) as unknown as Record<string, unknown>
    expect(cfg).not.toHaveProperty('webhookMaxRetries')
  })

  it('reads the optional vars', () => {
    const cfg = loadConfig(
      env({
        WATCH_DEFAULT_TTL: '86400',
        HEARTBEAT_INTERVAL: '60',
        ADMIN_TOKEN: 'tok',
        ADMIN_PORT: '0',
        WEBHOOK_TIMEOUT_MS: '2500',
        OUTBOX_MAX_AGE: '3600',
        OUTBOX_DEAD_MAX: '50',
        READY_MAX_LAG: '5',
      }),
    )
    expect(cfg).toMatchObject({
      watchDefaultTtl: 86400,
      heartbeatInterval: 60,
      adminToken: 'tok',
      adminPort: 0,
      webhookTimeoutMs: 2500,
      outboxMaxAgeSec: 3600,
      outboxDeadMax: 50,
      readyMaxLag: 5,
    })
  })

  it('READY_MAX_LAG must be a positive integer', () => {
    for (const bad of ['0', '-1', '1.5', 'two']) {
      expect(() => loadConfig(env({ READY_MAX_LAG: bad }))).toThrow(/READY_MAX_LAG must be a positive integer/)
    }
    expect(loadConfig(env({ READY_MAX_LAG: ' ' })).readyMaxLag).toBe(2) // blank = default
  })

  it.each(REQUIRED)('a missing %s throws naming it', (name) => {
    expect(() => loadConfig(env({ [name]: undefined }))).toThrow(`Missing required env var ${name}`)
    expect(() => loadConfig(env({ [name]: '   ' }))).toThrow(`Missing required env var ${name}`) // whitespace = missing
  })

  it('rejects a bad NETWORK', () => {
    expect(() => loadConfig(env({ NETWORK: 'litecoin' }))).toThrow(/NETWORK must be one of mainnet \| testnet \| signet \| regtest, got "litecoin"/)
  })

  it.each(['mainnet', 'testnet', 'signet', 'regtest'] as const)('accepts NETWORK=%s', (network) => {
    expect(loadConfig(env({ NETWORK: network })).network).toBe(network)
  })

  describe('CONFIRMATION_MILESTONES', () => {
    it('parses, dedupes and sorts', () => {
      const cfg = loadConfig(env({ CONFIRMATION_MILESTONES: ' 3, 1,1 ,0,,3 ' }))
      expect(cfg.milestones).toEqual([0, 1, 3])
      expect(cfg.confirmMilestones).toEqual([1, 3])
    })

    it('seenEnabled iff 0 is present', () => {
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '1,3' })).seenEnabled).toBe(false)
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '0,6' })).seenEnabled).toBe(true)
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '0' })).seenEnabled).toBe(true)
    })

    it('maxMilestone is the highest confirm milestone, 0 when there is none', () => {
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '0,1,6' })).maxMilestone).toBe(6)
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '2' })).maxMilestone).toBe(2)
      const seenOnly = loadConfig(env({ CONFIRMATION_MILESTONES: '0' }))
      expect(seenOnly.maxMilestone).toBe(0)
      expect(seenOnly.confirmMilestones).toEqual([])
    })

    it('ringSize = max(2 * maxMilestone, 12)', () => {
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '0,1,3' })).ringSize).toBe(12)
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '0' })).ringSize).toBe(12)
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '1,10' })).ringSize).toBe(20)
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '100' })).ringSize).toBe(200)
    })

    it('an empty or blank value falls back to the default 0,1,3', () => {
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '' })).milestones).toEqual([0, 1, 3])
      expect(loadConfig(env({ CONFIRMATION_MILESTONES: '   ' })).milestones).toEqual([0, 1, 3])
    })

    it.each(['x', '-1', '101', '1.5', '1,abc'])('rejects entry "%s"', (raw) => {
      expect(() => loadConfig(env({ CONFIRMATION_MILESTONES: raw }))).toThrow(/CONFIRMATION_MILESTONES entries must be integers 0-100/)
    })

    it('rejects a value with only separators', () => {
      expect(() => loadConfig(env({ CONFIRMATION_MILESTONES: ',,' }))).toThrow('CONFIRMATION_MILESTONES must not be empty')
    })
  })

  describe('WEBHOOK_URL', () => {
    it('rejects embedded credentials (user:pass@)', () => {
      expect(() => loadConfig(env({ WEBHOOK_URL: 'https://user:pass@example.com/hook' }))).toThrow(/must not embed credentials/)
      expect(() => loadConfig(env({ WEBHOOK_URL: 'https://user@example.com/hook' }))).toThrow(/must not embed credentials/)
    })

    it('rejects non-http(s) schemes', () => {
      expect(() => loadConfig(env({ WEBHOOK_URL: 'ftp://example.com/hook' }))).toThrow(/WEBHOOK_URL must be http\(s\), got "ftp:"/)
      expect(() => loadConfig(env({ WEBHOOK_URL: 'ws://example.com/hook' }))).toThrow(/must be http\(s\)/)
    })

    it('rejects an unparseable URL', () => {
      expect(() => loadConfig(env({ WEBHOOK_URL: 'not a url' }))).toThrow(/WEBHOOK_URL is not a valid URL/)
    })

    it('accepts plain http and https', () => {
      expect(loadConfig(env({ WEBHOOK_URL: 'http://localhost:9090/' })).webhookUrl).toBe('http://localhost:9090/')
      expect(loadConfig(env({ WEBHOOK_URL: 'https://example.com/hook?x=1' })).webhookUrl).toBe('https://example.com/hook?x=1')
    })
  })

  it('ADMIN_TOKEN unset or blank → null (no admin server)', () => {
    expect(loadConfig(env()).adminToken).toBeNull()
    expect(loadConfig(env({ ADMIN_TOKEN: '' })).adminToken).toBeNull()
    expect(loadConfig(env({ ADMIN_TOKEN: '   ' })).adminToken).toBeNull()
    expect(loadConfig(env({ ADMIN_TOKEN: ' tok ' })).adminToken).toBe('tok')
  })

  it.each(['WATCH_DEFAULT_TTL', 'HEARTBEAT_INTERVAL', 'ADMIN_PORT', 'WEBHOOK_TIMEOUT_MS'])(
    '%s must be a non-negative integer',
    (name) => {
      expect(() => loadConfig(env({ [name]: 'abc' }))).toThrow(`${name} must be a non-negative integer, got "abc"`)
      expect(() => loadConfig(env({ [name]: '-1' }))).toThrow(`${name} must be a non-negative integer`)
      expect(() => loadConfig(env({ [name]: '1.5' }))).toThrow(`${name} must be a non-negative integer`)
    },
  )

  it.each(['OUTBOX_MAX_AGE', 'OUTBOX_DEAD_MAX'])('%s must be a POSITIVE integer (0 would mean "dead-letter immediately" / "keep nothing")', (name) => {
    expect(() => loadConfig(env({ [name]: '0' }))).toThrow(`${name} must be a positive integer, got "0"`)
    expect(() => loadConfig(env({ [name]: '-1' }))).toThrow(`${name} must be a positive integer`)
    expect(() => loadConfig(env({ [name]: 'abc' }))).toThrow(`${name} must be a positive integer`)
    expect(() => loadConfig(env({ [name]: '1.5' }))).toThrow(`${name} must be a positive integer`)
    expect(loadConfig(env({ [name]: '1' }))).toMatchObject({ [name === 'OUTBOX_MAX_AGE' ? 'outboxMaxAgeSec' : 'outboxDeadMax']: 1 })
  })
})
