import { describe, expect, it } from 'vitest'
import { bech32, bech32m } from 'bech32'
import { decodeBlock, decodeRawTx, isValidAddress } from '../src/bitcoin/decoder'
import {
  genesisBlockHash,
  genesisBlockHex,
  genesisCoinbaseValueSats,
  genesisPrevHash,
  genesisTime,
  genesisTxid,
  txFixtures,
} from './fixtures/decoderFixtures'

// -- helpers ---------------------------------------------------------------

function fixture(txid: string) {
  const f = txFixtures.find((f) => f.txid === txid)
  if (!f) throw new Error(`missing fixture ${txid}`)
  return f
}

/** Minimal valid-enough block: 80-byte header + varint count + raw txs. */
function buildRawBlock(prevHashHex: string, timeSec: number, txHexes: string[]): Buffer {
  if (txHexes.length >= 0xfd) throw new Error('test helper only supports small blocks')
  const header = Buffer.alloc(80)
  header.writeUInt32LE(0x20000000, 0) // version
  Buffer.from(prevHashHex, 'hex').reverse().copy(header, 4) // prevHash, little-endian on the wire
  // merkle root (bytes 36-67) left zeroed — decodeBlock does not validate it
  header.writeUInt32LE(timeSec, 68)
  header.writeUInt32LE(0x1d00ffff, 72) // bits
  header.writeUInt32LE(42, 76) // nonce
  return Buffer.concat([header, Buffer.from([txHexes.length]), ...txHexes.map((h) => Buffer.from(h, 'hex'))])
}

// -- decodeRawTx: fixture round-trips --------------------------------------

describe('decodeRawTx fixtures', () => {
  for (const f of txFixtures) {
    it(`decodes ${f.name}`, () => {
      const decoded = decodeRawTx(f.rawTxHex, f.network)
      expect(decoded.txid).toBe(f.txid)
      expect(decoded.hex).toBe(f.rawTxHex)
      expect(decoded.outputs).toEqual(f.outputs)
    })
  }

  it('accepts a Buffer and yields identical results (hex normalized from buffer)', () => {
    const f = txFixtures[0]!
    const fromHex = decodeRawTx(f.rawTxHex, f.network)
    const fromBuf = decodeRawTx(Buffer.from(f.rawTxHex, 'hex'), f.network)
    expect(fromBuf).toEqual(fromHex)
  })

  it('throws on malformed input', () => {
    expect(() => decodeRawTx('deadbeef', 'mainnet')).toThrow()
    expect(() => decodeRawTx(Buffer.from('00', 'hex'), 'mainnet')).toThrow()
  })
})

// -- decodeRawTx: hardcoded known-good mainnet cases per script type -------

describe('decodeRawTx known-good mainnet addresses per script type', () => {
  it('p2wpkh', () => {
    const f = fixture('2f4d8b12392b92a50a544f209f06f7e779e83e30eaf1e48df53c41c87b2e8b0b')
    const out = decodeRawTx(f.rawTxHex, 'mainnet').outputs[0]!
    expect(out).toEqual({
      vout: 0,
      valueSats: 1079300,
      address: 'bc1q707eygu9v4rcp5j2rgzdqfmf948ejtxsslw5gu',
      scriptType: 'p2wpkh',
    })
  })

  it('p2wsh', () => {
    const f = fixture('2f4d8b12392b92a50a544f209f06f7e779e83e30eaf1e48df53c41c87b2e8b0b')
    const out = decodeRawTx(f.rawTxHex, 'mainnet').outputs[3]!
    expect(out).toEqual({
      vout: 3,
      valueSats: 1700000,
      address: 'bc1qpa3uuspt4c2jxu5ej7hdkwd3yd8la2msx8q0dxj54w2s3478gk3ql7k4sh',
      scriptType: 'p2wsh',
    })
  })

  it('p2tr', () => {
    const f = fixture('c85f1c0c429c1027c809ef71a0e05aebc34cff7d13f792e1ac0ae216a016b1b5')
    const out = decodeRawTx(f.rawTxHex, 'mainnet').outputs[1]!
    expect(out).toEqual({
      vout: 1,
      valueSats: 10000,
      address: 'bc1phrk90rkchqd0ydx94yndn26cyc20tqa6vzf7fh2wg4jfhy44239qmetdwe',
      scriptType: 'p2tr',
    })
  })

  it('p2pkh', () => {
    const f = fixture('704668d96430b0e8c9d03d218228ab11ef3ddf119248362dce29924630c238d3')
    const out = decodeRawTx(f.rawTxHex, 'mainnet').outputs[1]!
    expect(out).toEqual({
      vout: 1,
      valueSats: 782412,
      address: '1AZphbNjdmsP4oHNAyZVvDJEzfNEDieHAC',
      scriptType: 'p2pkh',
    })
  })

  it('p2sh', () => {
    const f = fixture('c85f1c0c429c1027c809ef71a0e05aebc34cff7d13f792e1ac0ae216a016b1b5')
    const out = decodeRawTx(f.rawTxHex, 'mainnet').outputs[0]!
    expect(out).toEqual({
      vout: 0,
      valueSats: 1200,
      address: '38SGXaGnBmrBhqmhc7soNvp5P3W7H6Nv6G',
      scriptType: 'p2sh',
    })
  })
})

// -- decodeRawTx: unknown script types are kept, never dropped --------------

describe('unknown script types', () => {
  it('keeps a mid-array OP_RETURN output with address null and the REAL vout index', () => {
    const f = fixture('b31dde7472809b73e9d4edb701c43a2d02861007bcb462c5229a98bfc1964892')
    const decoded = decodeRawTx(f.rawTxHex, 'testnet')
    expect(decoded.outputs).toHaveLength(3)
    expect(decoded.outputs[1]).toEqual({ vout: 1, valueSats: 0, address: null, scriptType: null })
    // neighbours keep their true indices — the upstream decoder collapsed them
    expect(decoded.outputs[2]!.vout).toBe(2)
    expect(decoded.outputs[2]!.address).toBe('tb1qv6xfz68lu92vf7dp38se7mlfhk88f2tqyzcrzs')
  })

  it('keeps the coinbase witness-commitment OP_RETURN output', () => {
    const f = fixture('33b68057b18e1226064c37dcaf4e53142115ac8025ed35b7fea6ebb70f5a5b29')
    const decoded = decodeRawTx(f.rawTxHex, 'mainnet')
    expect(decoded.outputs).toHaveLength(2)
    expect(decoded.outputs[1]).toEqual({ vout: 1, valueSats: 0, address: null, scriptType: null })
  })

  it('reports P2PK (genesis coinbase) as address null but keeps valueSats', () => {
    const block = decodeBlock(Buffer.from(genesisBlockHex, 'hex'), 'mainnet')
    expect(block.txs[0]!.outputs).toEqual([
      { vout: 0, valueSats: genesisCoinbaseValueSats, address: null, scriptType: null },
    ])
  })
})

// -- decodeRawTx: network prefix handling -----------------------------------

describe('network prefixes', () => {
  const testnetFixture = fixture('e6bdde7e2ef03a49777d69167cb921744c07ea4092052067d32468082299fa81')

  it('signet encodes identically to testnet (tb/6f/c4)', () => {
    const onTestnet = decodeRawTx(testnetFixture.rawTxHex, 'testnet')
    const onSignet = decodeRawTx(testnetFixture.rawTxHex, 'signet')
    expect(onSignet.outputs).toEqual(onTestnet.outputs)
    expect(onSignet.outputs[1]!.address).toBe('tb1qc7psdze9j0r38rv8gj2kl8gysqevtqyqs20upw')
  })

  it('regtest uses the bcrt bech32 prefix but testnet base58 bytes', () => {
    const onRegtest = decodeRawTx(testnetFixture.rawTxHex, 'regtest')
    // p2pkh: same version byte as testnet
    expect(onRegtest.outputs[0]!.address).toBe('mumMe9wWeLb26oGKf334CKiRYiWMpmKpUp')
    // p2wpkh: bcrt prefix, same program
    const regtestAddr = onRegtest.outputs[1]!.address!
    expect(regtestAddr.startsWith('bcrt1q')).toBe(true)
    expect(isValidAddress(regtestAddr, 'regtest')).toBe(true)
    expect(isValidAddress(regtestAddr, 'testnet')).toBe(false)
    expect(isValidAddress(regtestAddr, 'mainnet')).toBe(false)
  })
})

// -- witness v0 must use bech32, v1 must use bech32m ------------------------

describe('bech32 vs bech32m', () => {
  const program20 = Buffer.from('c783068b2593c7138d8744956f9d048032c58080', 'hex')
  const program32 = Buffer.from('b8ec578ed8b81af234c5a926d9ab582614f583ba6093e4dd4e45649b92b5544a', 'hex')

  it('v0 addresses carry a bech32 (not bech32m) checksum', () => {
    const decoded = decodeRawTx(
      fixture('e6bdde7e2ef03a49777d69167cb921744c07ea4092052067d32468082299fa81').rawTxHex,
      'testnet',
    )
    const addr = decoded.outputs[1]!.address!
    // decodable ONLY by bech32; bech32m decode must reject it
    expect(bech32.decode(addr, 90).words[0]).toBe(0)
    expect(() => bech32m.decode(addr, 90)).toThrow()
  })

  it('v1 addresses carry a bech32m (not bech32) checksum', () => {
    const decoded = decodeRawTx(
      fixture('c85f1c0c429c1027c809ef71a0e05aebc34cff7d13f792e1ac0ae216a016b1b5').rawTxHex,
      'mainnet',
    )
    const addr = decoded.outputs[1]!.address!
    expect(bech32m.decode(addr, 90).words[0]).toBe(1)
    expect(() => bech32.decode(addr, 90)).toThrow()
  })

  it('isValidAddress rejects the upstream-bug encodings (wrong checksum variant)', () => {
    const v1WithBech32 = bech32.encode('bc', [1, ...bech32.toWords(program32)])
    const v0WithBech32m = bech32m.encode('bc', [0, ...bech32m.toWords(program20)])
    expect(isValidAddress(v1WithBech32, 'mainnet')).toBe(false)
    expect(isValidAddress(v0WithBech32m, 'mainnet')).toBe(false)
  })
})

// -- decodeBlock -------------------------------------------------------------

describe('decodeBlock', () => {
  it('decodes the mainnet genesis block (known hash/prevHash/time)', () => {
    const block = decodeBlock(Buffer.from(genesisBlockHex, 'hex'), 'mainnet')
    expect(block.hash).toBe(genesisBlockHash)
    expect(block.prevHash).toBe(genesisPrevHash)
    expect(block.time).toBe(genesisTime)
    expect(block.txs).toHaveLength(1)
    expect(block.txs[0]!.txid).toBe(genesisTxid)
  })

  it('decodes segwit transactions inside a block with full addresses', () => {
    const f1 = fixture('2f4d8b12392b92a50a544f209f06f7e779e83e30eaf1e48df53c41c87b2e8b0b')
    const f2 = fixture('704668d96430b0e8c9d03d218228ab11ef3ddf119248362dce29924630c238d3')
    const prevHash = '00000000d1145790a8694403d4063f323d499e655c83426834d4ce2f8dd4a2ee'
    const time = 1735689600
    const raw = buildRawBlock(prevHash, time, [f1.rawTxHex, f2.rawTxHex])

    const block = decodeBlock(raw, 'mainnet')
    expect(block.hash).toMatch(/^[0-9a-f]{64}$/)
    expect(block.prevHash).toBe(prevHash)
    expect(block.time).toBe(time)
    expect(block.txs).toHaveLength(2)
    expect(block.txs[0]!.txid).toBe(f1.txid)
    expect(block.txs[0]!.hex).toBe(f1.rawTxHex)
    expect(block.txs[0]!.outputs).toEqual(f1.outputs)
    expect(block.txs[1]!.txid).toBe(f2.txid)
    expect(block.txs[1]!.outputs).toEqual(f2.outputs)
  })

  it('throws on malformed input', () => {
    expect(() => decodeBlock(Buffer.from('deadbeef', 'hex'), 'mainnet')).toThrow()
  })
})

// -- isValidAddress ----------------------------------------------------------

describe('isValidAddress', () => {
  const mainnetAddrs = {
    p2wpkh: 'bc1q707eygu9v4rcp5j2rgzdqfmf948ejtxsslw5gu',
    p2wsh: 'bc1qpa3uuspt4c2jxu5ej7hdkwd3yd8la2msx8q0dxj54w2s3478gk3ql7k4sh',
    p2tr: 'bc1phrk90rkchqd0ydx94yndn26cyc20tqa6vzf7fh2wg4jfhy44239qmetdwe',
    p2pkh: '1AZphbNjdmsP4oHNAyZVvDJEzfNEDieHAC',
    p2sh: '38SGXaGnBmrBhqmhc7soNvp5P3W7H6Nv6G',
  }
  const testnetAddrs = {
    p2wpkh: 'tb1qc7psdze9j0r38rv8gj2kl8gysqevtqyqs20upw',
    p2tr: 'tb1p009h0xvcmepnxewrt8sphjvtpxcymsadrsaudwextgajee92vfxsvljhv9',
    p2pkh: 'mumMe9wWeLb26oGKf334CKiRYiWMpmKpUp',
    p2sh: '2N8JDhrLqtwZ4MGC1QAcwyiQg3v6ffhCrJb',
  }

  it('accepts all five script types on mainnet', () => {
    for (const addr of Object.values(mainnetAddrs)) {
      expect(isValidAddress(addr, 'mainnet'), addr).toBe(true)
    }
  })

  it('rejects testnet-family addresses on mainnet', () => {
    for (const addr of Object.values(testnetAddrs)) {
      expect(isValidAddress(addr, 'mainnet'), addr).toBe(false)
    }
  })

  it('accepts testnet addresses on testnet, rejects mainnet ones', () => {
    for (const addr of Object.values(testnetAddrs)) {
      expect(isValidAddress(addr, 'testnet'), addr).toBe(true)
    }
    for (const addr of Object.values(mainnetAddrs)) {
      expect(isValidAddress(addr, 'testnet'), addr).toBe(false)
    }
  })

  it('accepts tb1... (and testnet base58) on signet, rejects mainnet addresses', () => {
    for (const addr of Object.values(testnetAddrs)) {
      expect(isValidAddress(addr, 'signet'), addr).toBe(true)
    }
    for (const addr of Object.values(mainnetAddrs)) {
      expect(isValidAddress(addr, 'signet'), addr).toBe(false)
    }
  })

  it('rejects tb1/bc1 bech32 on regtest but accepts testnet base58 (shared version bytes)', () => {
    expect(isValidAddress(testnetAddrs.p2wpkh, 'regtest')).toBe(false)
    expect(isValidAddress(mainnetAddrs.p2wpkh, 'regtest')).toBe(false)
    expect(isValidAddress(testnetAddrs.p2pkh, 'regtest')).toBe(true)
    expect(isValidAddress(testnetAddrs.p2sh, 'regtest')).toBe(true)
  })

  it('rejects garbage and checksum-corrupted addresses on every network', () => {
    // flip the final character of a valid bech32 address (q -> p)
    const corrupted = testnetAddrs.p2wpkh.slice(0, -1) + 'p'
    for (const net of ['mainnet', 'testnet', 'signet', 'regtest'] as const) {
      expect(isValidAddress('', net)).toBe(false)
      expect(isValidAddress('not-an-address', net)).toBe(false)
      expect(isValidAddress(corrupted, net)).toBe(false)
    }
  })
})

// -- non-minimal push lookalikes (BIP141/BIP16 require exact byte templates) --

describe('script template strictness', () => {
  // Build a minimal 1-in/1-out tx around an arbitrary output script.
  function txWithScript(scriptHex: string): string {
    // hand-rolled wire format: version | 1 input (null-ish prevout, empty sig) | 1 output | locktime
    const prevout = '11'.repeat(32) + '00000000'
    const input = prevout + '00' + 'ffffffff'
    const script = scriptHex
    const scriptLen = (script.length / 2).toString(16).padStart(2, '0')
    const output = 'e803000000000000' + scriptLen + script // 1000 sats LE
    return '02000000' + '01' + input + '01' + output + '00000000'
  }

  it('exact templates still decode (positive controls)', () => {
    const p2wpkh = decodeRawTx(txWithScript('0014' + 'ab'.repeat(20)), 'mainnet').outputs[0]!
    expect(p2wpkh.scriptType).toBe('p2wpkh')
    expect(p2wpkh.address).not.toBeNull()

    const p2sh = decodeRawTx(txWithScript('a914' + 'ab'.repeat(20) + '87'), 'mainnet').outputs[0]!
    expect(p2sh.scriptType).toBe('p2sh')

    const p2tr = decodeRawTx(txWithScript('5120' + 'ab'.repeat(32)), 'mainnet').outputs[0]!
    expect(p2tr.scriptType).toBe('p2tr')
  })

  it('non-minimal PUSHDATA1 lookalikes are NOT classified (they do not pay the address)', () => {
    // OP_0 PUSHDATA1 0x14 <20B> — decompiles like p2wpkh but is anyone-can-spend
    const segwitLookalike = decodeRawTx(txWithScript('004c14' + 'ab'.repeat(20)), 'mainnet').outputs[0]!
    expect(segwitLookalike.scriptType).toBeNull()
    expect(segwitLookalike.address).toBeNull()

    // OP_HASH160 PUSHDATA1 0x14 <20B> OP_EQUAL — p2sh lookalike, spendable by preimage
    const p2shLookalike = decodeRawTx(txWithScript('a94c14' + 'ab'.repeat(20) + '87'), 'mainnet').outputs[0]!
    expect(p2shLookalike.scriptType).toBeNull()
    expect(p2shLookalike.address).toBeNull()

    // OP_1 PUSHDATA1 0x20 <32B> — p2tr lookalike
    const p2trLookalike = decodeRawTx(txWithScript('514c20' + 'ab'.repeat(32)), 'mainnet').outputs[0]!
    expect(p2trLookalike.scriptType).toBeNull()
    expect(p2trLookalike.address).toBeNull()

    // OP_DUP OP_HASH160 PUSHDATA1 0x14 <20B> OP_EQUALVERIFY OP_CHECKSIG — p2pkh lookalike
    const p2pkhLookalike = decodeRawTx(
      txWithScript('76a94c14' + 'ab'.repeat(20) + '88ac'),
      'mainnet',
    ).outputs[0]!
    expect(p2pkhLookalike.scriptType).toBeNull()
    expect(p2pkhLookalike.address).toBeNull()
  })
})
