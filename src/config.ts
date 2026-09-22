import type { Network } from './lib/types'

export interface WeirConfig {
  network: Network
  bitcoinRpcUrl: string
  bitcoinZmqUrl: string
  redisUrl: string
  webhookUrl: string
  webhookSecret: string
  /** parsed, sorted, deduped CONFIRMATION_MILESTONES, e.g. [0, 1, 3] */
  milestones: number[]
  /** milestones > 0, i.e. the depths that fire `confirmed` events */
  confirmMilestones: number[]
  /** true when milestones include 0 — mempool `seen` events enabled */
  seenEnabled: boolean
  /** max confirm milestone; defines tracking window + reorg shield. 0 when no confirm milestones. */
  maxMilestone: number
  /** how many recent block hashes to keep in the ring */
  ringSize: number
  /** seconds; 0 = watch forever */
  watchDefaultTtl: number
  /** seconds; 0 = heartbeat off */
  heartbeatInterval: number
  /** null = admin HTTP server does not exist */
  adminToken: string | null
  adminPort: number
  webhookMaxRetries: number
  webhookTimeoutMs: number
}

const NETWORKS: Network[] = ['mainnet', 'testnet', 'signet', 'regtest']

function required(env: NodeJS.ProcessEnv, name: string): string {
  const v = env[name]?.trim()
  if (!v) throw new Error(`Missing required env var ${name}`)
  return v
}

function intOr(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name]?.trim()
  if (!raw) return fallback
  const n = Number(raw)
  if (!Number.isInteger(n) || n < 0) throw new Error(`${name} must be a non-negative integer, got "${raw}"`)
  return n
}

/**
 * WEBHOOK_URL must be plain http(s) with NO embedded credentials: consumer auth is the
 * HMAC signature, and a credentialed URL would (a) be rejected by fetch at delivery time
 * and (b) leak the password into logs via the fetch error message on every attempt.
 */
function validateWebhookUrl(raw: string): string {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    throw new Error(`WEBHOOK_URL is not a valid URL: "${raw}"`)
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`WEBHOOK_URL must be http(s), got "${url.protocol}"`)
  }
  if (url.username !== '' || url.password !== '') {
    throw new Error(
      'WEBHOOK_URL must not embed credentials (user:pass@) — webhook authenticity is proven by the HMAC signature (WEBHOOK_SECRET), not basic auth',
    )
  }
  return raw
}

function parseMilestones(raw: string): number[] {
  const parts = raw.split(',').map((s) => s.trim()).filter((s) => s.length > 0)
  if (parts.length === 0) throw new Error('CONFIRMATION_MILESTONES must not be empty')
  const nums = parts.map((p) => {
    const n = Number(p)
    if (!Number.isInteger(n) || n < 0 || n > 100) {
      throw new Error(`CONFIRMATION_MILESTONES entries must be integers 0-100, got "${p}"`)
    }
    return n
  })
  return [...new Set(nums)].sort((a, b) => a - b)
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): WeirConfig {
  const network = required(env, 'NETWORK') as Network
  if (!NETWORKS.includes(network)) {
    throw new Error(`NETWORK must be one of ${NETWORKS.join(' | ')}, got "${network}"`)
  }

  const milestones = parseMilestones(env['CONFIRMATION_MILESTONES']?.trim() || '0,1,3')
  const confirmMilestones = milestones.filter((m) => m > 0)
  const maxMilestone = confirmMilestones.length > 0 ? confirmMilestones[confirmMilestones.length - 1]! : 0

  const adminToken = env['ADMIN_TOKEN']?.trim() || null

  return {
    network,
    bitcoinRpcUrl: required(env, 'BITCOIN_RPC_URL'),
    bitcoinZmqUrl: required(env, 'BITCOIN_ZMQ_URL'),
    redisUrl: required(env, 'REDIS_URL'),
    webhookUrl: validateWebhookUrl(required(env, 'WEBHOOK_URL')),
    webhookSecret: required(env, 'WEBHOOK_SECRET'),
    milestones,
    confirmMilestones,
    seenEnabled: milestones.includes(0),
    maxMilestone,
    ringSize: Math.max(maxMilestone * 2, 12),
    watchDefaultTtl: intOr(env, 'WATCH_DEFAULT_TTL', 0),
    heartbeatInterval: intOr(env, 'HEARTBEAT_INTERVAL', 0),
    adminToken,
    adminPort: intOr(env, 'ADMIN_PORT', 8787),
    webhookMaxRetries: intOr(env, 'WEBHOOK_MAX_RETRIES', 3),
    webhookTimeoutMs: intOr(env, 'WEBHOOK_TIMEOUT_MS', 10000),
  }
}
