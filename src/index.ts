/**
 * weir daemon entrypoint. Boot sequence per DESIGN.md "src/index.ts":
 *   loadConfig → Store.connect → preflight → admin server when ADMIN_TOKEN is set →
 *   startOutboxDrainer → ONE awaited drainOnce → the engine queue's first item: reconcile
 *   + resolveLimbo (then `runtime.reconciled = true`) → startZmq (every rawtx, rawblock and
 *   gap-triggered reparse is queued) → initial mempool reparse (queued) → periodic reparse
 *   timer (queued) → startHeartbeat.
 * ONE WRITER (DESIGN "Single writer"): every engine action goes through `engine.run`, so
 * evaluations, blocks and reparses never interleave. The heartbeat and the admin server
 * only READ engine state (and the admin writes the watch set, which the engine only reads);
 * the outbox drainer touches outbox keys only. None of them is on the queue.
 * The admin server comes up BEFORE the boot drain and reconcile so the platform's probes
 * get answers during a long catch-up: `/live` 200, `/ready` 503 `{reconciled: false}` —
 * instead of a refused connection that looks like a dead process.
 * The drainer starts right after that and one pass is awaited BEFORE reconcile: events
 * queued before a crash go out first, and their acks free redis memory before reconcile
 * writes (a full redis would otherwise crash-loop at boot); if the endpoint is still down
 * the pass just reschedules. Engine deps get NO sink (they enqueue through the Store); the
 * sink goes only to the drainer and the heartbeat.
 * `runtime` (DESIGN "Health from chain lag") is the process state the probes read:
 * reconciled, shuttingDown (set FIRST in shutdown, so /live flips before anything closes),
 * and the last ZMQ rawtx/rawblock receipt times.
 * SIGINT/SIGTERM → close zmq, stop the reparse timer, stop heartbeat, close admin, await
 * drainer.stop(), store.quit, exit 0.
 *
 * The startup banner never prints secrets: webhook target HOST only, and the RPC URL
 * never appears anywhere (it carries credentials).
 */
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { loadConfig } from './config'
import type { Runtime } from './lib/types'
import { describeError, fatal, log } from './lib/log'
import { Store } from './store/redis'
import { Rpc } from './bitcoin/rpc'
import { startZmq } from './bitcoin/zmq'
import { decodeBlock, decodeRawTx, isValidAddress } from './bitcoin/decoder'
import { WebhookSink } from './delivery/webhook'
import { startOutboxDrainer } from './delivery/outbox'
import { makeEngineQueue } from './engine/queue'
import { makeRawTxHandler, makeTxEvaluator } from './engine/txPipeline'
import { MEMPOOL_REPARSE_INTERVAL_MS, makeMempoolReparser } from './engine/mempool'
import { makeBlockProcessor } from './engine/blockPipeline'
import { resolveLimbo } from './engine/reorg'
import { startHeartbeat } from './engine/heartbeat'
import { preflight } from './boot/preflight'
import { reconcile } from './boot/reconcile'
import { startAdminServer } from './admin/server'

const CTX = 'index'

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
    timeoutMs: cfg.webhookTimeoutMs,
  })

  try {
    await store.connect()
  } catch (err) {
    throw new Error(`redis connection failed (${new URL(cfg.redisUrl).host}): ${describeError(err)}`)
  }
  log.info(CTX, 'redis connected')

  await preflight({ cfg, store, rpc })

  const runtime: Runtime = { reconciled: false, shuttingDown: false, lastZmqTxAt: null, lastZmqBlockAt: null }

  // Probes answer from here on: /live 200, /ready 503 until reconcile is done.
  let admin: { close(): Promise<void> } | null = null
  if (cfg.adminToken !== null) {
    admin = startAdminServer({
      store,
      rpc,
      runtime,
      isValidAddress: (address: string) => isValidAddress(address, cfg.network),
      config: cfg,
    })
  }

  // Events still owed from before a restart go out now, ahead of the (possibly long) reconcile;
  // their acks free redis memory before reconcile writes. A store failure here is fatal (boot).
  const drainer = startOutboxDrainer({ cfg, store, sink })
  const bootDelivered = await drainer.drainOnce()
  log.info(CTX, `boot drain: ${bootDelivered} queued event(s) delivered before reconcile`)

  // The one writer. Boot reconciliation is its first item; ZMQ opens only after it finishes,
  // so nothing can be queued ahead of or during it.
  const engine = makeEngineQueue()
  const processBlock = makeBlockProcessor({ cfg, store, rpc, decodeBlock })
  await engine.run(async () => {
    await reconcile({ store, rpc, processBlock })
    // Crash-recovery: adjudicate reorg-displaced txs even when there was nothing to catch up
    // (a crash between limbo-rewind and resolution). No-op when limbo is empty.
    await resolveLimbo({ cfg, store, rpc })
  })
  runtime.reconciled = true

  const evaluate = makeTxEvaluator({ store, cfg })
  const handleRawTx = makeRawTxHandler({ store, cfg, decodeRawTx })
  const reparse = makeMempoolReparser({ rpc, store, cfg, decodeRawTx, evaluate })

  // Every handler is one engine-queue item; a rejection inside the queue is fatal (never
  // unhandled, never swallowed). Receipt times are stamped on the way in (before decoding)
  // — /ready reports them as ages.
  const zmq = await startZmq({
    url: cfg.bitcoinZmqUrl,
    onRawTx: (buf) => {
      runtime.lastZmqTxAt = Date.now()
      return engine.run(() => handleRawTx(buf))
    },
    onRawBlock: (buf) => {
      runtime.lastZmqBlockAt = Date.now()
      return engine.run(() => processBlock(buf))
    },
    onTxGap: () => engine.run(reparse),
  })

  // Initial mempool reparse — queued, not awaited (spec: async).
  void engine.run(reparse)

  // Periodic reparse — the retry safety net (DESIGN "Outpoint tracking" rule 7): picks up
  // txs ZMQ missed without a sequence gap. Queued like everything else, so it cannot overlap.
  const reparseTimer = setInterval(() => void engine.run(reparse), MEMPOOL_REPARSE_INTERVAL_MS)
  reparseTimer.unref()
  log.info(CTX, `periodic mempool reparse every ${MEMPOOL_REPARSE_INTERVAL_MS / 1000}s`)

  const heartbeat = startHeartbeat({ cfg, store, rpc, sink })

  function shutdown(signal: string): void {
    if (runtime.shuttingDown) return
    runtime.shuttingDown = true // FIRST: /live answers 503 from this instant
    log.info(CTX, `${signal} received — shutting down`)
    ;(async () => {
      await zmq.close()
      clearInterval(reparseTimer)
      heartbeat.stop()
      if (admin !== null) await admin.close()
      await drainer.stop() // waits for an in-flight delivery pass
      await store.quit()
      log.info(CTX, 'shutdown complete')
      process.exit(0)
    })().catch((err: unknown) => {
      log.error(CTX, 'shutdown failed')
      fatal(CTX, err)
    })
  }
  process.on('SIGINT', () => shutdown('SIGINT'))
  process.on('SIGTERM', () => shutdown('SIGTERM'))

  log.info(CTX, 'weir is running')
}

main().catch((err: unknown) => fatal(CTX, err))
