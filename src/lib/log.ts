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
