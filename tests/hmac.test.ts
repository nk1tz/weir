import { describe, expect, it } from 'vitest'
import { signBody, verifySignature } from '../src/lib/hmac'

const SECRET = 'weir-test-secret'
const BODY = JSON.stringify({ version: 1, event: 'seen', txid: 'ab'.repeat(32), confs: 0 })
const T = 1_700_000_000

describe('signBody', () => {
  it('returns the full header value t=<t>, v1=<hex>', () => {
    const header = signBody(SECRET, BODY, T)
    expect(header).toMatch(/^t=1700000000, v1=[0-9a-f]{64}$/)
  })

  it('is deterministic for identical inputs', () => {
    expect(signBody(SECRET, BODY, T)).toBe(signBody(SECRET, BODY, T))
  })
})

describe('verifySignature', () => {
  it('round-trips a signed body', () => {
    const header = signBody(SECRET, BODY, T)
    expect(verifySignature(SECRET, BODY, header, 300, T)).toBe(true)
  })

  it('accepts within tolerance, including the exact boundary', () => {
    const header = signBody(SECRET, BODY, T)
    expect(verifySignature(SECRET, BODY, header, 300, T + 299)).toBe(true)
    expect(verifySignature(SECRET, BODY, header, 300, T + 300)).toBe(true)
    expect(verifySignature(SECRET, BODY, header, 300, T - 300)).toBe(true)
  })

  it('verifies with default nowSeconds against a freshly signed body', () => {
    const now = Math.floor(Date.now() / 1000)
    const header = signBody(SECRET, BODY, now)
    expect(verifySignature(SECRET, BODY, header)).toBe(true)
  })

  it('parses tolerantly: extra whitespace and reversed part order', () => {
    const header = signBody(SECRET, BODY, T)
    const [tPart, v1Part] = header.split(', ') as [string, string]
    expect(verifySignature(SECRET, BODY, `  ${tPart} ,   ${v1Part}  `, 300, T)).toBe(true)
    expect(verifySignature(SECRET, BODY, `${v1Part},${tPart}`, 300, T)).toBe(true)
    expect(verifySignature(SECRET, BODY, ` t = ${T} , v1 = ${v1Part.split('=')[1]} `, 300, T)).toBe(true)
  })

  it('fails on a tampered body', () => {
    const header = signBody(SECRET, BODY, T)
    expect(verifySignature(SECRET, BODY + 'x', header, 300, T)).toBe(false)
    expect(verifySignature(SECRET, BODY.replace('seen', 'confirmed'), header, 300, T)).toBe(false)
  })

  it('fails when t is stale beyond tolerance (both directions)', () => {
    const header = signBody(SECRET, BODY, T)
    expect(verifySignature(SECRET, BODY, header, 300, T + 301)).toBe(false)
    expect(verifySignature(SECRET, BODY, header, 300, T - 301)).toBe(false)
  })

  it('fails on malformed headers', () => {
    const validHex = signBody(SECRET, BODY, T).split('v1=')[1] as string
    expect(verifySignature(SECRET, BODY, '', 300, T)).toBe(false)
    expect(verifySignature(SECRET, BODY, 'garbage', 300, T)).toBe(false)
    expect(verifySignature(SECRET, BODY, `t=${T}`, 300, T)).toBe(false) // missing v1
    expect(verifySignature(SECRET, BODY, `v1=${validHex}`, 300, T)).toBe(false) // missing t
    expect(verifySignature(SECRET, BODY, `t=abc, v1=${validHex}`, 300, T)).toBe(false) // non-numeric t
    expect(verifySignature(SECRET, BODY, `t=${T}.5, v1=${validHex}`, 300, T)).toBe(false) // non-integer t
    expect(verifySignature(SECRET, BODY, `t=-${T}, v1=${validHex}`, 300, T)).toBe(false) // negative t
    expect(verifySignature(SECRET, BODY, `t=${T}, v1=not-hex`, 300, T)).toBe(false) // non-hex v1
    expect(verifySignature(SECRET, BODY, `t=${T}, v1=${validHex.slice(0, 32)}`, 300, T)).toBe(false) // truncated digest
    expect(verifySignature(SECRET, BODY, `t=${T}, v1=${validHex.slice(0, 63)}`, 300, T)).toBe(false) // odd-length hex
    expect(verifySignature(SECRET, BODY, `t=${T}, v2=${validHex}`, 300, T)).toBe(false) // unknown version only
  })

  it('fails with the wrong secret', () => {
    const header = signBody(SECRET, BODY, T)
    expect(verifySignature('wrong-secret', BODY, header, 300, T)).toBe(false)
  })

  it('accepts an uppercase hex digest (case-insensitive v1)', () => {
    const header = signBody(SECRET, BODY, T)
    const upper = header.replace(/v1=(.+)$/, (_, hex: string) => `v1=${hex.toUpperCase()}`)
    expect(verifySignature(SECRET, BODY, upper, 300, T)).toBe(true)
  })
})
