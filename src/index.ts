/**
 * weir daemon entrypoint. Boot sequence per DESIGN.md "src/index.ts":
 *   loadConfig → Store.connect → preflight → reconcile → startZmq (rawtx → txHandler,
 *   rawblock → blockHandler, gap → reparser) → initial mempool reparse (async) →
 *   startHeartbeat → admin server when ADMIN_TOKEN is set.
 * SIGINT/SIGTERM → close zmq, stop heartbeat, close admin, store.quit, exit 0.
 *
 * The startup banner never prints secrets: webhook target HOST only, and the RPC URL
 * never appears anywhere (it carries credentials).
 */
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { loadConfig } from './config'
import { log } from './lib/log'
import { Store } from './store/redis'
import { Rpc } from './bitcoin/rpc'
import { startZmq } from './bitcoin/zmq'
import { decodeBlock, decodeRawTx, isValidAddress } from './bitcoin/decoder'
import { WebhookSink } from './delivery/webhook'
import { makeRawTxHandler, makeTxEvaluator } from './engine/txPipeline'
import { makeMempoolReparser } from './engine/mempool'
import { makeBlockHandler, makeBlockProcessor } from './engine/blockPipeline'
import { resolveLimbo } from './engine/reorg'
import { startHeartbeat } from './engine/heartbeat'
import { preflight } from './boot/preflight'
import { reconcile } from './boot/reconcile'
import { startAdminServer } from './admin/server'

const CTX = 'index'

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

/** Mask credentials in a URL for log output. Handles passwords containing '@' (WHATWG
 *  parse, blank the userinfo) with a regex fallback for strings URL cannot parse. */
function maskUrl(raw: string): string {
  try {
    const url = new URL(raw)
    if (url.username !== '' || url.password !== '') {
      url.username = '***'
      url.password = ''
    }
    return url.toString()
  } catch {
    return raw.replace(/\/\/.*@/, '//***@')
  }
}

/** Works from both src/ (tsx) and dist/ (node): ../package.json is the project root. */
function packageVersion(): string {
  const pkg = JSON.parse(readFileSync(join(__dirname, '..', 'package.json'), 'utf8')) as {
    version?: string
  }
  return pkg.version ?? '0.0.0'
}

async function main(): Promise<void> {
  const cfg = loadConfig()

  // Startup banner — secrets stay out: webhook HOST only, no WEBHOOK_SECRET, no RPC URL.
  const webhookHost = new URL(cfg.webhookUrl).host
  log.info(CTX, `weir v${packageVersion()} starting`)
  log.info(
    CTX,
    `network=${cfg.network} milestones=[${cfg.milestones.join(',')}] ` +
      `webhook=${webhookHost} admin=${cfg.adminToken !== null ? `on :${cfg.adminPort}` : 'off'}`,
  )

  const store = new Store(cfg.redisUrl, cfg.network)
  const rpc = new Rpc(cfg.bitcoinRpcUrl)
  const sink = new WebhookSink({
    url: cfg.webhookUrl,
    secret: cfg.webhookSecret,
    maxRetries: cfg.webhookMaxRetries,
    timeoutMs: cfg.webhookTimeoutMs,
  })

  try {
    await store.connect()
  } catch (err) {
    throw new Error(`redis connection failed (${maskUrl(cfg.redisUrl)}): ${errMsg(err)}`)
  }
  log.info(CTX, 'redis connected')

  await preflight({ cfg, store, rpc, log })

  const processBlock = makeBlockProcessor({ cfg, store, rpc, sink, decodeBlock, log })
  await reconcile({
    store,
    rpc,
    processBlock,
    // crash-recovery: adjudicate reorg-displaced txs even when there's nothing to catch up
    resolveLimbo: () => resolveLimbo({ cfg, store, rpc, sink, log }),
    log,
  })

  const evaluate = makeTxEvaluator({ store, sink, cfg })
  const handleRawTx = makeRawTxHandler({ store, sink, cfg, decodeRawTx })
  const reparse = makeMempoolReparser({ rpc, store, cfg, decodeRawTx, evaluate })
  const handleBlock = makeBlockHandler({ cfg, store, rpc, sink, decodeBlock, log })

  /** unix ms of the last block weir fully processed — feeds /health secondsSinceLastBlock */
  let lastBlockAt: number | null = null

  // zmq's safeInvoke wraps every handler: returned promise rejections are logged, never unhandled.
  const zmq = await startZmq({
    url: cfg.bitcoinZmqUrl,
    onRawTx: (buf) => handleRawTx(buf),
    onRawBlock: (buf) =>
      handleBlock(buf).then(() => {
        lastBlockAt = Date.now()
      }),
    onTxGap: () => reparse(),
  })

  // Initial mempool reparse — deliberately not awaited (spec: async). A failure here is an
  // unexpected internal error: log and crash, per DESIGN's error policy.
  reparse().catch((err: unknown) => {
    log.error(CTX, `initial mempool reparse failed: ${errMsg(err)}`)
    process.exit(1)
  })

  const heartbeat = startHeartbeat({ cfg, store, sink, log })

  let admin: { close(): Promise<void> } | null = null
  if (cfg.adminToken !== null) {
    admin = startAdminServer({
      store,
      isValidAddress: (address: string) => isValidAddress(address, cfg.network),
      rpcPing: async () => {
        await rpc.getBlockCount()
      },
      lastBlockAtMs: () => lastBlockAt,
      config: cfg,
    })
  }

  let shuttingDown = false
  function shutdown(signal: string): void {
    if (shuttingDown) return
    shuttingDown = true
    log.info(CTX, `${signal} received — shutting down`)
    ;(async () => {
      await zmq.close()
      heartbeat.stop()
      if (admin !== null) await admin.close()
      await store.quit()
      log.info(CTX, 'shutdown complete')
      process.exit(0)
    })().catch((err: unknown) => {
      log.error(CTX, `shutdown failed: ${errMsg(err)}`)
      process.exit(1)
    })
  }
  process.on('SIGINT', () => shutdown('SIGINT'))
  process.on('SIGTERM', () => shutdown('SIGTERM'))

  log.info(CTX, 'weir is running')
}

main().catch((err: unknown) => {
  log.error(CTX, `fatal: ${errMsg(err)}`)
  process.exit(1)
})
