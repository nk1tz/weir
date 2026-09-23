/**
 * Minimal single-line logger: `[level] [ctx] msg`. No colors, no deps.
 * Per DESIGN.md "src/lib/log.ts".
 */

type Level = 'info' | 'warn' | 'error'

function emit(level: Level, ctx: string, msg: string): void {
  const line = `[${level}] [${ctx}] ${msg}`
  if (level === 'error') {
    console.error(line)
  } else if (level === 'warn') {
    console.warn(line)
  } else {
    console.log(line)
  }
}

export const log: {
  info(ctx: string, msg: string): void
  warn(ctx: string, msg: string): void
  error(ctx: string, msg: string): void
} = {
  info(ctx: string, msg: string): void {
    emit('info', ctx, msg)
  },
  warn(ctx: string, msg: string): void {
    emit('warn', ctx, msg)
  },
  error(ctx: string, msg: string): void {
    emit('error', ctx, msg)
  },
}

/**
 * THE fatal-error policy (DESIGN.md "Module map and contracts"): an UNEXPECTED error — a
 * redis/rpc failure inside an engine loop, a rejected ZMQ handler, a block that fails to
 * process — is logged with its stack and the process exits 1. Docker restarts the daemon
 * and boot reconciliation heals. Delivery failures are never routed here: the sink returns
 * `{ok: false}` and the outbox drainer retries.
 */
export function fatal(ctx: string, err: unknown): never {
  const stack = err instanceof Error && err.stack ? `\n${err.stack}` : ''
  emit('error', ctx, `fatal: ${describeError(err)}${stack}`)
  process.exit(1)
}

/**
 * One-line description of any thrown value for log output. AggregateError (e.g.
 * dual-stack ECONNREFUSED) has an empty .message — surface the inner ones; undici's
 * fetch wraps the real network error ("connect ECONNREFUSED ...") in .cause — follow it.
 */
export function describeError(err: unknown): string {
  if (err instanceof AggregateError) {
    const inner = err.errors.map((e) => (e instanceof Error ? e.message : String(e))).join('; ')
    return err.message ? `${err.message}: ${inner}` : inner || 'AggregateError (no detail)'
  }
  if (err instanceof Error) {
    return err.cause !== undefined ? `${err.message}: ${describeError(err.cause)}` : err.message
  }
  return String(err)
}
