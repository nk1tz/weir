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
