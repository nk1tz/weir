import { Subscriber } from 'zeromq'
import { fatal, log } from '../lib/log'
import { metrics } from '../lib/metrics'

const CTX = 'zmq'

const TOPIC_RAWTX = 'rawtx'
const TOPIC_RAWBLOCK = 'rawblock'

/** bitcoind's per-topic ZMQ sequence counter is a uint32 — wraps at 2^32. */
const UINT32_WRAP = 0x1_0000_0000

/**
 * Fire-and-forget handler invocation: never awaited, but a sync throw or an async
 * rejection is an UNEXPECTED error (the handlers already absorb delivery failures as
 * booleans) → fatal. Never unhandled, never swallowed.
 */
function safeInvoke(name: string, fn: () => unknown): void {
  void Promise.resolve()
    .then(() => fn())
    .catch((err: unknown) => {
      log.error(CTX, `${name} handler failed`)
      fatal(CTX, err)
    })
}

export async function startZmq(opts: {
  url: string
  onRawTx: (buf: Buffer) => void
  onRawBlock: (buf: Buffer) => void
  onTxGap: () => void
}): Promise<{ close(): Promise<void> }> {
  const sock = new Subscriber()
  sock.connect(opts.url)
  sock.subscribe(TOPIC_RAWTX)
  sock.subscribe(TOPIC_RAWBLOCK)
  log.info(CTX, `subscribed to ${TOPIC_RAWTX}+${TOPIC_RAWBLOCK} at ${opts.url}`)

  let closed = false
  // Per-topic sequence tracking (4-byte LE uint appended by bitcoind).
  // null until the first message on that topic — the first message never gaps.
  let lastTxSeq: number | null = null
  let lastBlockSeq: number | null = null

  async function run(): Promise<void> {
    // The iterator returns cleanly when sock.close() is called.
    for await (const parts of sock) {
      const [topicBuf, message, seqBuf] = parts
      if (topicBuf === undefined || message === undefined) {
        log.warn(CTX, `ignoring malformed message with ${parts.length} part(s)`)
        continue
      }
      const topic = topicBuf.toString('utf8')
      const seq = seqBuf !== undefined && seqBuf.length >= 4 ? seqBuf.readUInt32LE(0) : null

      if (topic === TOPIC_RAWTX) {
        // Last-message timestamps (unix seconds) for /metrics — a flat line means ZMQ is dead.
        metrics.gauges.set('weir_last_zmq_tx_timestamp_seconds', Math.floor(Date.now() / 1000))
        if (seq !== null) {
          if (lastTxSeq !== null) {
            const expected = (lastTxSeq + 1) % UINT32_WRAP
            if (seq !== expected) {
              // Fires once per gap: lastTxSeq resyncs to `seq` below, so the
              // next in-order message will not re-trigger.
              log.warn(CTX, `rawtx sequence gap: expected ${expected}, got ${seq}`)
              safeInvoke('onTxGap', () => opts.onTxGap())
            }
          }
          lastTxSeq = seq
        }
        safeInvoke('onRawTx', () => opts.onRawTx(message))
      } else if (topic === TOPIC_RAWBLOCK) {
        metrics.gauges.set('weir_last_zmq_block_timestamp_seconds', Math.floor(Date.now() / 1000))
        if (seq !== null) {
          if (lastBlockSeq !== null) {
            const expected = (lastBlockSeq + 1) % UINT32_WRAP
            if (seq !== expected) {
              // No dedicated handler: block gaps heal via the block pipeline's
              // own tip-connectivity gap walk. Log for visibility only.
              log.warn(CTX, `rawblock sequence gap: expected ${expected}, got ${seq}`)
            }
          }
          lastBlockSeq = seq
        }
        safeInvoke('onRawBlock', () => opts.onRawBlock(message))
      } else {
        log.warn(CTX, `ignoring message on unexpected topic "${topic}"`)
      }
    }
  }

  const loopDone = run().catch((err: unknown) => {
    if (closed) {
      // close() interrupted a pending receive — expected during shutdown.
      return
    }
    // Unexpected internal error: crash (docker restarts us; boot reconciliation heals).
    log.error(CTX, 'subscriber loop failed')
    fatal(CTX, err)
  })

  return {
    async close(): Promise<void> {
      closed = true
      sock.close()
      await loopDone
      log.info(CTX, 'subscriber closed')
    },
  }
}
