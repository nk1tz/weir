import { describe, expect, it, vi } from 'vitest'
import { makeEngineQueue } from '../src/engine/queue'

describe('engine queue (the one writer)', () => {
  it('runs items strictly one after the other, in arrival order, even when a later item is enqueued while an earlier one is parked', async () => {
    const queue = makeEngineQueue(vi.fn())
    const trace: string[] = []
    let release!: () => void
    const gate = new Promise<void>((r) => {
      release = r
    })

    const first = queue.run(async () => {
      trace.push('a:start')
      await gate
      trace.push('a:end')
    })
    const second = queue.run(async () => {
      trace.push('b')
    })
    const third = queue.run(async () => {
      trace.push('c')
    })
    await Promise.resolve()
    expect(trace).toEqual(['a:start']) // b and c wait behind the parked a

    release()
    await Promise.all([first, second, third])

    expect(trace).toEqual(['a:start', 'a:end', 'b', 'c'])
  })

  it('a rejected item is FATAL (injected onFatal called with the error, nothing swallowed) and the returned promise never rejects', async () => {
    // Policy: unexpected errors crash the process (docker restarts, boot reconciliation heals).
    // There is no "skip the failed item and keep going": a skipped block loses confirmations.
    const boom = new Error('redis went away')
    const onFatal = vi.fn()
    const queue = makeEngineQueue(onFatal)

    await expect(
      queue.run(async () => {
        throw boom
      }),
    ).resolves.toBeUndefined()

    expect(onFatal).toHaveBeenCalledTimes(1)
    expect(onFatal).toHaveBeenCalledWith('engine', boom)
  })
})
