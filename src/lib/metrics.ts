/**
 * In-process metrics registry — the source of `GET /metrics`. Spec: docs/DESIGN.md "Health
 * from chain lag".
 *
 * Process-local and I/O-free: `counters.inc` / `gauges.set` touch a Map and nothing else, so
 * an engine hot path can call them freely and a redis outage cannot reach them. Counters
 * reset on restart — that is what `_total` means. Gauges that need I/O (heights, watch
 * count, outbox, redis memory) are NOT pushed here: the admin handler computes them at
 * scrape time and passes them to `render()` as extra samples.
 *
 * `render()` is a hand-rolled Prometheus text exposition 0.0.4 formatter: one `# TYPE` line
 * per family, families sorted by name, labels sorted by key and escaped (`\\`, `\"`, `\n`),
 * values as JS prints them (`+Inf`/`-Inf`/`NaN` per the format). No dependency.
 *
 * The weir families are declared at load (counters start at 0, known label values included)
 * so every counter is present from the first scrape — `increase()` needs the zero sample.
 * A gauge is absent until first set (e.g. the ZMQ timestamps before the first message).
 */

export type Labels = Readonly<Record<string, string>>

/** A gauge computed by the scraper, merged into `render()` output. */
export interface GaugeSample {
  name: string
  value: number
  labels?: Labels
}

interface Sample {
  labels: Labels
  value: number
}

/** name → (label key → sample); the inner Map keeps declaration/insertion order. */
type Family = Map<string, Sample>

const NAME_RE = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/
const LABEL_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

function assertName(name: string): void {
  if (!NAME_RE.test(name)) throw new Error(`[metrics] invalid metric name "${name}"`)
}

/** Sorted-by-key serialization — the identity of a label set. */
function labelKey(labels: Labels): string {
  return JSON.stringify(Object.keys(labels).sort().map((k) => [k, labels[k]]))
}

function escapeLabelValue(v: string): string {
  return v.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')
}

function formatValue(v: number): string {
  if (Number.isNaN(v)) return 'NaN'
  if (v === Number.POSITIVE_INFINITY) return '+Inf'
  if (v === Number.NEGATIVE_INFINITY) return '-Inf'
  return String(v)
}

function formatSample(name: string, s: Sample): string {
  const keys = Object.keys(s.labels).sort()
  if (keys.length === 0) return `${name} ${formatValue(s.value)}`
  const pairs = keys.map((k) => {
    if (!LABEL_RE.test(k)) throw new Error(`[metrics] invalid label name "${k}" on ${name}`)
    return `${k}="${escapeLabelValue(s.labels[k] as string)}"`
  })
  return `${name}{${pairs.join(',')}} ${formatValue(s.value)}`
}

export class MetricsRegistry {
  private readonly counterFamilies = new Map<string, Family>()
  private readonly gaugeFamilies = new Map<string, Family>()

  readonly counters = {
    /** Add `by` (default 1, must be ≥ 0 — counters only go up) to the sample with these labels. */
    inc: (name: string, labels: Labels = {}, by = 1): void => {
      if (!(by >= 0)) throw new Error(`[metrics] counter ${name} cannot decrease (by=${by})`)
      const family = this.family(this.counterFamilies, name)
      const key = labelKey(labels)
      const existing = family.get(key)
      if (existing) existing.value += by
      else family.set(key, { labels: { ...labels }, value: by })
    },
    /** Register a family with these label sets at 0 so it renders before its first increment. */
    declare: (name: string, labelSets: Labels[] = [{}]): void => {
      const family = this.family(this.counterFamilies, name)
      for (const labels of labelSets) {
        const key = labelKey(labels)
        if (!family.has(key)) family.set(key, { labels: { ...labels }, value: 0 })
      }
    },
    /** Current value, 0 when never incremented (tests and the scraper read through this). */
    get: (name: string, labels: Labels = {}): number => this.counterFamilies.get(name)?.get(labelKey(labels))?.value ?? 0,
  }

  readonly gauges = {
    /** Overwrite the sample with these labels. */
    set: (name: string, value: number, labels: Labels = {}): void => {
      const family = this.family(this.gaugeFamilies, name)
      family.set(labelKey(labels), { labels: { ...labels }, value })
    },
    /** null when never set. */
    get: (name: string, labels: Labels = {}): number | null => this.gaugeFamilies.get(name)?.get(labelKey(labels))?.value ?? null,
  }

  /** A name has ONE type: registering it as the other kind (either order) is refused, so no family ever renders two `# TYPE` lines. */
  private family(families: Map<string, Family>, name: string): Family {
    let family = families.get(name)
    if (family === undefined) {
      assertName(name)
      const other = families === this.counterFamilies ? this.gaugeFamilies : this.counterFamilies
      if (other.has(name)) {
        throw new Error(`[metrics] ${name} is already a ${families === this.counterFamilies ? 'gauge' : 'counter'} — a metric name has one type`)
      }
      family = new Map()
      families.set(name, family)
    }
    return family
  }

  /**
   * Prometheus text exposition 0.0.4. `extra` gauges (scrape-time reads) are merged in: a
   * name already registered as a gauge gets the extra sample added/overwritten; a name that
   * is also a counter is rejected (a family has one type).
   */
  render(extra: GaugeSample[] = []): string {
    const gauges = new Map<string, Family>()
    for (const [name, family] of this.gaugeFamilies) gauges.set(name, new Map(family))
    for (const g of extra) {
      if (this.counterFamilies.has(g.name)) throw new Error(`[metrics] ${g.name} is already a counter — a metric name has one type`)
      let family = gauges.get(g.name)
      if (family === undefined) {
        assertName(g.name)
        family = new Map()
        gauges.set(g.name, family)
      }
      const labels = g.labels ?? {}
      family.set(labelKey(labels), { labels: { ...labels }, value: g.value })
    }

    const blocks: Array<{ name: string; type: 'counter' | 'gauge'; family: Family }> = []
    for (const [name, family] of this.counterFamilies) blocks.push({ name, type: 'counter', family })
    for (const [name, family] of gauges) blocks.push({ name, type: 'gauge', family })
    blocks.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))

    const lines: string[] = []
    for (const b of blocks) {
      if (b.family.size === 0) continue
      lines.push(`# TYPE ${b.name} ${b.type}`)
      for (const s of b.family.values()) lines.push(formatSample(b.name, s))
    }
    return lines.length === 0 ? '' : `${lines.join('\n')}\n`
  }

  /** Back to the declared zero state (tests). */
  reset(): void {
    this.counterFamilies.clear()
    this.gaugeFamilies.clear()
    declareWeirFamilies(this)
  }
}

/** The event types that go through the outbox — `heartbeat` bypasses it and is not counted. */
const ENQUEUED_EVENTS = ['seen', 'confirmed', 'dropped', 'demoted', 'conflicted', 'expired'] as const

/** Every weir counter family, at 0, so `/metrics` shows them from the first scrape. */
function declareWeirFamilies(r: MetricsRegistry): void {
  r.counters.declare(
    'weir_events_enqueued_total',
    ENQUEUED_EVENTS.map((event) => ({ event })),
  )
  r.counters.declare('weir_webhook_deliveries_total', [{ result: 'ok' }, { result: 'fail' }])
  r.counters.declare('weir_events_dead_lettered_total')
  r.counters.declare('weir_blocks_processed_total')
  r.counters.declare('weir_reorgs_total')
}

/** THE registry — one per process; every module increments this one. */
export const metrics = new MetricsRegistry()
declareWeirFamilies(metrics)
