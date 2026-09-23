import { describeError, log } from '../lib/log'

const CTX = 'rpc'

/** Total attempts for a single logical call when the failure is network-level. */
const MAX_ATTEMPTS = 3
/** First retry delay; doubles per attempt. */
const BACKOFF_BASE_MS = 250
/**
 * Per-attempt deadline covering the request AND reading the response body. Generous: the
 * block path fetches whole blocks (`getblock` verbosity 0 on mainnet is a few MB). A call's
 * total budget is bounded at MAX_ATTEMPTS × timeout (+ backoff).
 */
export const RPC_TIMEOUT_MS = 30_000
/** The probe/heartbeat call (`getblockcount`) answers in microseconds or not at all — short deadline. */
export const RPC_PROBE_TIMEOUT_MS = 5_000

interface JsonRpcErrorShape {
  code: number
  message: string
}

interface JsonRpcResponse {
  result: unknown
  error: JsonRpcErrorShape | null
  id: unknown
}

/**
 * Internal error carrying the JSON-RPC error code so methods can map specific
 * "not found" codes (-5) to null. Not exported — the module's public surface
 * is exactly `class Rpc` per DESIGN.md.
 */
class RpcError extends Error {
  readonly code: number

  constructor(code: number, message: string) {
    super(message)
    this.name = 'RpcError'
    this.code = code
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/** the subset of bitcoind's getmempoolentry result weir uses */
export interface MempoolEntry {
  /** vbytes of the tx and its unconfirmed ancestors */
  ancestorsize: number
  fees: {
    /** BTC paid by the tx and its unconfirmed ancestors */
    ancestor: number
  }
}

export class Rpc {
  /** URL with credentials stripped — the actual fetch target. */
  private readonly endpoint: string
  private readonly authHeader: string
  private nextId = 0

  constructor(url: string) {
    const parsed = new URL(url)
    const user = decodeURIComponent(parsed.username)
    const pass = decodeURIComponent(parsed.password)
    this.authHeader = `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}`
    parsed.username = ''
    parsed.password = ''
    this.endpoint = parsed.toString()
  }

  /**
   * One attempt: POST and read the whole body under ONE AbortController deadline — a
   * stalled node (connection accepted, no bytes, or a body that never ends) is a rejection
   * at `timeoutMs`, never a hang. Throws on fetch rejection, body abort or deadline.
   */
  private async attempt(body: string, timeoutMs: number): Promise<{ status: number; ok: boolean; text: string }> {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    try {
      const res = await fetch(this.endpoint, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: this.authHeader,
        },
        body,
        signal: controller.signal,
      })
      const text = await res.text()
      return { status: res.status, ok: res.ok, text }
    } catch (err) {
      if (controller.signal.aborted) throw new Error(`timeout after ${timeoutMs}ms`, { cause: err })
      throw err
    } finally {
      clearTimeout(timer)
    }
  }

  /**
   * POST with retry (MAX_ATTEMPTS, 250ms base, x2 backoff) ONLY on network-level failures —
   * a fetch rejection, a body that aborted, or the per-attempt deadline; an HTTP response
   * of any status is never retried. Total budget ≤ MAX_ATTEMPTS × timeoutMs + backoff.
   */
  private async fetchWithRetry(method: string, body: string, timeoutMs: number): Promise<{ status: number; ok: boolean; text: string }> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.attempt(body, timeoutMs)
      } catch (err) {
        if (attempt >= MAX_ATTEMPTS) {
          log.error(CTX, `${method}: network failure after ${MAX_ATTEMPTS} attempts: ${describeError(err)}`)
          throw new Error(`rpc ${method} failed after ${MAX_ATTEMPTS} attempts: ${describeError(err)}`, { cause: err })
        }
        const delayMs = BACKOFF_BASE_MS * 2 ** (attempt - 1)
        log.warn(
          CTX,
          `${method}: network failure (attempt ${attempt}/${MAX_ATTEMPTS}), retrying in ${delayMs}ms: ${describeError(err)}`,
        )
        await sleep(delayMs)
      }
    }
  }

  /**
   * One JSON-RPC 1.0 call under a per-attempt deadline (RPC_TIMEOUT_MS unless the caller
   * passes a tighter one). Never retries 401/403 (throws 'Unauthorized' immediately) and
   * never retries JSON-RPC error responses (throws RpcError with the node's code/message).
   */
  private async call(method: string, params: unknown[] = [], timeoutMs: number = RPC_TIMEOUT_MS): Promise<unknown> {
    const id = ++this.nextId
    const body = JSON.stringify({ jsonrpc: '1.0', id, method, params })
    const response = await this.fetchWithRetry(method, body, timeoutMs)

    if (response.status === 401 || response.status === 403) {
      log.error(CTX, `${method}: HTTP ${response.status} from node — bad RPC credentials`)
      throw new Error('Unauthorized')
    }

    const text = response.text
    let parsed: JsonRpcResponse
    try {
      parsed = JSON.parse(text) as JsonRpcResponse
    } catch (err) {
      log.error(CTX, `${method}: HTTP ${response.status} with unparseable body: ${describeError(err)}`)
      throw new Error(`${method}: HTTP ${response.status} with non-JSON body`)
    }

    // bitcoind reports RPC-level errors with a non-null `error` member (often
    // alongside a non-2xx HTTP status). These are never retried.
    if (parsed.error !== null && parsed.error !== undefined) {
      throw new RpcError(parsed.error.code, parsed.error.message)
    }

    if (!response.ok) {
      log.error(CTX, `${method}: HTTP ${response.status} without JSON-RPC error member`)
      throw new Error(`${method}: HTTP ${response.status}`)
    }

    return parsed.result
  }

  /** The probe/heartbeat call: short deadline so /ready, /metrics and a heartbeat tick never wait on a stalled node. */
  async getBlockCount(): Promise<number> {
    return (await this.call('getblockcount', [], RPC_PROBE_TIMEOUT_MS)) as number
  }

  async getBestBlockHash(): Promise<string> {
    return (await this.call('getbestblockhash')) as string
  }

  async getBlockHash(height: number): Promise<string> {
    return (await this.call('getblockhash', [height])) as string
  }

  /** `confirmations` is -1 when the block is not on the node's active chain (Core semantics). */
  async getBlockHeader(hash: string): Promise<{ height: number; previousblockhash?: string; time: number; confirmations: number }> {
    return (await this.call('getblockheader', [hash])) as {
      height: number
      previousblockhash?: string
      time: number
      confirmations: number
    }
  }

  /** getblockheader, or null when the node does not know the hash (RPC error code -5) — boot's ring rebuild. */
  async getBlockHeaderIfKnown(hash: string): Promise<{ height: number; previousblockhash?: string; time: number; confirmations: number } | null> {
    try {
      return await this.getBlockHeader(hash)
    } catch (err) {
      if (err instanceof RpcError && err.code === -5) return null
      throw err
    }
  }

  /** getblock verbosity 0 — raw serialized block. */
  async getBlockRaw(hash: string): Promise<Buffer> {
    const hex = (await this.call('getblock', [hash, 0])) as string
    return Buffer.from(hex, 'hex')
  }

  async getRawMempool(): Promise<string[]> {
    return (await this.call('getrawmempool')) as string[]
  }

  /** null when the node does not know the tx (RPC error code -5). */
  async getRawTransactionVerbose(txid: string): Promise<{ blockhash?: string; hex: string } | null> {
    try {
      return (await this.call('getrawtransaction', [txid, true])) as { blockhash?: string; hex: string }
    } catch (err) {
      if (err instanceof RpcError && err.code === -5) return null
      throw err
    }
  }

  /**
   * The mempool entry (null when the tx is not in the mempool: RPC error -5). Typed to what
   * weir reads: the ancestor package size (vbytes) and its total fee (BTC), for `seen`'s
   * `feeRateSatVb`.
   */
  async getMempoolEntry(txid: string): Promise<MempoolEntry | null> {
    try {
      return (await this.call('getmempoolentry', [txid])) as MempoolEntry
    } catch (err) {
      if (err instanceof RpcError && err.code === -5) return null
      throw err
    }
  }

  async getBlockchainInfo(): Promise<{ chain: string; blocks: number; pruned: boolean; pruneheight?: number }> {
    return (await this.call('getblockchaininfo')) as {
      chain: string
      blocks: number
      pruned: boolean
      pruneheight?: number
    }
  }

  async getZmqNotifications(): Promise<Array<{ type: string; address: string }>> {
    return (await this.call('getzmqnotifications')) as Array<{ type: string; address: string }>
  }
}
