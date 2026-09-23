import { beforeEach, describe, expect, it } from 'vitest'
import { MetricsRegistry, metrics } from '../src/lib/metrics'

describe('metrics registry', () => {
  let r: MetricsRegistry

  beforeEach(() => {
    r = new MetricsRegistry()
  })

  it('renders Prometheus text exposition 0.0.4: one # TYPE per family, families sorted, trailing newline', () => {
    r.counters.inc('weir_blocks_processed_total')
    r.gauges.set('weir_tip_height', 100)
    r.counters.inc('weir_events_enqueued_total', { event: 'seen' })
    r.counters.inc('weir_events_enqueued_total', { event: 'seen' })
    r.counters.inc('weir_events_enqueued_total', { event: 'confirmed' })

    expect(r.render()).toBe(
      [
        '# TYPE weir_blocks_processed_total counter',
        'weir_blocks_processed_total 1',
        '# TYPE weir_events_enqueued_total counter',
        'weir_events_enqueued_total{event="seen"} 2',
        'weir_events_enqueued_total{event="confirmed"} 1',
        '# TYPE weir_tip_height gauge',
        'weir_tip_height 100',
        '',
      ].join('\n'),
    )
  })

  it('an empty registry renders the empty string', () => {
    expect(r.render()).toBe('')
  })

  it('labels are sorted by key and their values escaped (backslash, quote, newline)', () => {
    r.gauges.set('weir_g', 1, { zeta: 'a\\b', alpha: 'say "hi"\nthere' })
    expect(r.render()).toBe('# TYPE weir_g gauge\nweir_g{alpha="say \\"hi\\"\\nthere",zeta="a\\\\b"} 1\n')
  })

  it('label sets are identified regardless of key order', () => {
    r.counters.inc('weir_c', { a: '1', b: '2' })
    r.counters.inc('weir_c', { b: '2', a: '1' })
    expect(r.counters.get('weir_c', { a: '1', b: '2' })).toBe(2)
    expect(r.render()).toBe('# TYPE weir_c counter\nweir_c{a="1",b="2"} 2\n')
  })

  it('counters are monotonic: inc by 1 by default, by n when given, never down', () => {
    r.counters.inc('weir_c')
    r.counters.inc('weir_c')
    r.counters.inc('weir_c', {}, 5)
    expect(r.counters.get('weir_c')).toBe(7)
    expect(() => r.counters.inc('weir_c', {}, -1)).toThrow(/cannot decrease/)
    expect(r.counters.get('weir_c')).toBe(7)
    expect(r.counters.get('weir_never')).toBe(0)
  })

  it('gauges overwrite; the last set wins', () => {
    r.gauges.set('weir_g', 5)
    r.gauges.set('weir_g', 2.5)
    expect(r.gauges.get('weir_g')).toBe(2.5)
    expect(r.render()).toBe('# TYPE weir_g gauge\nweir_g 2.5\n')
  })

  it('a gauge never set is absent (no family, no sample); get() is null', () => {
    r.counters.inc('weir_c')
    expect(r.gauges.get('weir_last_zmq_tx_timestamp_seconds')).toBeNull()
    expect(r.render()).not.toContain('weir_last_zmq_tx_timestamp_seconds')
  })

  it('floats render as-is; non-finite values use the +Inf/-Inf/NaN spellings', () => {
    r.gauges.set('weir_f', 1758500000.123)
    r.gauges.set('weir_inf', Number.POSITIVE_INFINITY)
    r.gauges.set('weir_ninf', Number.NEGATIVE_INFINITY)
    r.gauges.set('weir_nan', Number.NaN)
    const out = r.render()
    expect(out).toContain('weir_f 1758500000.123\n')
    expect(out).toContain('weir_inf +Inf\n')
    expect(out).toContain('weir_ninf -Inf\n')
    expect(out).toContain('weir_nan NaN\n')
  })

  it('render(extra) merges scrape-time gauges: new families, overwrites of a registered gauge, and refuses a counter name', () => {
    r.gauges.set('weir_g', 1)
    r.counters.inc('weir_c')
    const out = r.render([
      { name: 'weir_g', value: 9 },
      { name: 'weir_up', value: 1 },
      { name: 'weir_lbl', value: 3, labels: { k: 'v' } },
    ])
    expect(out).toBe(
      [
        '# TYPE weir_c counter',
        'weir_c 1',
        '# TYPE weir_g gauge',
        'weir_g 9',
        '# TYPE weir_lbl gauge',
        'weir_lbl{k="v"} 3',
        '# TYPE weir_up gauge',
        'weir_up 1',
        '',
      ].join('\n'),
    )
    // extra samples never leak into the registry
    expect(r.gauges.get('weir_g')).toBe(1)
    expect(r.gauges.get('weir_up')).toBeNull()
    expect(() => r.render([{ name: 'weir_c', value: 1 }])).toThrow(/is already a counter/)
  })

  it('declared families render at 0 before any increment; declare never resets a live value', () => {
    r.counters.declare('weir_c', [{ result: 'ok' }, { result: 'fail' }])
    expect(r.render()).toBe('# TYPE weir_c counter\nweir_c{result="ok"} 0\nweir_c{result="fail"} 0\n')
    r.counters.inc('weir_c', { result: 'ok' })
    r.counters.declare('weir_c', [{ result: 'ok' }])
    expect(r.counters.get('weir_c', { result: 'ok' })).toBe(1)
  })

  it('a metric name has ONE type: registering it as the other kind is refused in both orders, so no family renders two # TYPE lines', () => {
    r.counters.inc('weir_c')
    expect(() => r.gauges.set('weir_c', 1)).toThrow(/weir_c is already a counter/)
    r.gauges.set('weir_g', 1)
    expect(() => r.counters.inc('weir_g')).toThrow(/weir_g is already a gauge/)
    expect(() => r.counters.declare('weir_g')).toThrow(/weir_g is already a gauge/)
    const types = r.render().split('\n').filter((l) => l.startsWith('# TYPE '))
    expect(types).toEqual(['# TYPE weir_c counter', '# TYPE weir_g gauge'])
  })

  it('invalid metric and label names are rejected', () => {
    expect(() => r.counters.inc('weir-bad')).toThrow(/invalid metric name/)
    expect(() => r.gauges.set('1starts_with_digit', 1)).toThrow(/invalid metric name/)
    r.gauges.set('weir_g', 1, { 'bad-label': 'x' })
    expect(() => r.render()).toThrow(/invalid label name/)
  })

  it('the process singleton declares every weir counter family at 0, and reset() restores that state', () => {
    metrics.reset()
    const out = metrics.render()
    for (const event of ['seen', 'confirmed', 'dropped', 'demoted', 'conflicted', 'expired']) {
      expect(out).toContain(`weir_events_enqueued_total{event="${event}"} 0\n`)
    }
    expect(out).toContain('weir_webhook_deliveries_total{result="ok"} 0\n')
    expect(out).toContain('weir_webhook_deliveries_total{result="fail"} 0\n')
    expect(out).toContain('weir_events_dead_lettered_total 0\n')
    expect(out).toContain('weir_blocks_processed_total 0\n')
    expect(out).toContain('weir_reorgs_total 0\n')
    expect(out).not.toContain('heartbeat')

    metrics.counters.inc('weir_reorgs_total')
    metrics.gauges.set('weir_last_zmq_tx_timestamp_seconds', 1)
    metrics.reset()
    expect(metrics.counters.get('weir_reorgs_total')).toBe(0)
    expect(metrics.gauges.get('weir_last_zmq_tx_timestamp_seconds')).toBeNull()
  })
})
