/**
 * Pure bitcoin wire-format decoding — no I/O.
 *
 * Ported from blockhooksV2 decodeRawTransaction with the spec'd fixes:
 * - EVERY output is kept: unrecognized script types (OP_RETURN, P2PK, bare
 *   multisig, future witness versions, ...) yield address:null/scriptType:null
 *   instead of being dropped, so `vout` indices are always the real ones.
 * - valueSats carried through from the wire (bitcoinjs v6 gives satoshis).
 * - witness v0 (p2wpkh/p2wsh) encodes with bech32, witness v1 (p2tr) with
 *   bech32m — the upstream code mixed bech32m.toWords into v0 encoding.
 * - signet added (same address prefixes as testnet).
 */
import * as bitcoinjs from 'bitcoinjs-lib'
import bs58check from 'bs58check'
import { bech32, bech32m } from 'bech32'
import { DecodedBlock, DecodedOutput, DecodedTx, Network, ScriptType } from '../lib/types'

interface AddressPrefixes {
  /** base58check version byte for p2pkh */
  p2pkh: number
  /** base58check version byte for p2sh */
  p2sh: number
  /** human-readable part for bech32/bech32m */
  bech32: string
}

/** mainnet bc/00/05; testnet AND signet tb/6f/c4; regtest bcrt/6f/c4 */
const PREFIXES: Record<Network, AddressPrefixes> = {
  mainnet: { p2pkh: 0x00, p2sh: 0x05, bech32: 'bc' },
  testnet: { p2pkh: 0x6f, p2sh: 0xc4, bech32: 'tb' },
  signet: { p2pkh: 0x6f, p2sh: 0xc4, bech32: 'tb' },
  regtest: { p2pkh: 0x6f, p2sh: 0xc4, bech32: 'bcrt' },
}

/** bitcoinjs network params; signet shares testnet's address encoding. */
const BITCOINJS_NETWORKS: Record<Network, bitcoinjs.networks.Network> = {
  mainnet: bitcoinjs.networks.bitcoin,
  testnet: bitcoinjs.networks.testnet,
  signet: bitcoinjs.networks.testnet,
  regtest: bitcoinjs.networks.regtest,
}

function encodeAddress(type: ScriptType, hash: Buffer, network: Network): string {
  const prefixes = PREFIXES[network]
  switch (type) {
    case 'p2wpkh':
    case 'p2wsh':
      // witness v0 — bech32 checksum (BIP173)
      return bech32.encode(prefixes.bech32, [0, ...bech32.toWords(hash)])
    case 'p2tr':
      // witness v1 — bech32m checksum (BIP350)
      return bech32m.encode(prefixes.bech32, [1, ...bech32m.toWords(hash)])
    case 'p2pkh':
      return bs58check.encode(Buffer.concat([Buffer.from([prefixes.p2pkh]), hash]))
    case 'p2sh':
      return bs58check.encode(Buffer.concat([Buffer.from([prefixes.p2sh]), hash]))
  }
}

/**
 * Identify one of the five supported scriptPubKey templates by EXACT BYTE PATTERN,
 * the way Bitcoin Core's Solver does. Deliberately NOT decompile-based: decompiling
 * erases push-opcode minimality, so a non-minimal lookalike (e.g. `OP_0 PUSHDATA1
 * 0x14 <20B>`) would classify as p2wpkh even though BIP141 requires the exact
 * 2-byte-prefixed form and such an output does NOT pay the p2wpkh address — for
 * segwit it is anyone-can-spend. Byte templates make lookalikes fall through to
 * null (unrecognized), which is the correct verdict for a payment notifier.
 */
function identifyScript(s: Buffer): { type: ScriptType; hash: Buffer } | null {
  // BIP141 P2WPKH: OP_0 PUSH20 — exactly 0x00 0x14 <20 bytes>
  if (s.length === 22 && s[0] === 0x00 && s[1] === 0x14) {
    return { type: 'p2wpkh', hash: s.subarray(2) }
  }
  // BIP141 P2WSH: OP_0 PUSH32 — exactly 0x00 0x20 <32 bytes>
  if (s.length === 34 && s[0] === 0x00 && s[1] === 0x20) {
    return { type: 'p2wsh', hash: s.subarray(2) }
  }
  // BIP341 P2TR: OP_1 PUSH32 — exactly 0x51 0x20 <32 bytes>
  if (s.length === 34 && s[0] === 0x51 && s[1] === 0x20) {
    return { type: 'p2tr', hash: s.subarray(2) }
  }
  // P2PKH: OP_DUP OP_HASH160 PUSH20 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
  if (
    s.length === 25 &&
    s[0] === 0x76 &&
    s[1] === 0xa9 &&
    s[2] === 0x14 &&
    s[23] === 0x88 &&
    s[24] === 0xac
  ) {
    return { type: 'p2pkh', hash: s.subarray(3, 23) }
  }
  // BIP16 P2SH: OP_HASH160 PUSH20 <20 bytes> OP_EQUAL — exact form required by consensus
  if (s.length === 23 && s[0] === 0xa9 && s[1] === 0x14 && s[22] === 0x87) {
    return { type: 'p2sh', hash: s.subarray(2, 22) }
  }
  return null
}

function decodeOutputs(tx: bitcoinjs.Transaction, network: Network): DecodedOutput[] {
  return tx.outs.map((out, vout): DecodedOutput => {
    const identified = identifyScript(out.script)
    if (identified === null) {
      // Unknown script type: keep the output (real vout index) with a null address.
      return { vout, valueSats: out.value, address: null, scriptType: null }
    }
    return {
      vout,
      valueSats: out.value,
      address: encodeAddress(identified.type, identified.hash, network),
      scriptType: identified.type,
    }
  })
}

function toDecodedTx(tx: bitcoinjs.Transaction, hex: string, network: Network): DecodedTx {
  return {
    txid: tx.getId(),
    hex,
    outputs: decodeOutputs(tx, network),
  }
}

/** Decode a raw transaction (ZMQ rawtx buffer or RPC hex). Throws on malformed input. */
export function decodeRawTx(raw: Buffer | string, network: Network): DecodedTx {
  const hex = typeof raw === 'string' ? raw : raw.toString('hex')
  const tx = bitcoinjs.Transaction.fromHex(hex)
  return toDecodedTx(tx, hex, network)
}

/** Decode a raw block (ZMQ rawblock buffer or RPC verbosity-0 result). Throws on malformed input. */
export function decodeBlock(raw: Buffer, network: Network): DecodedBlock {
  const block = bitcoinjs.Block.fromBuffer(raw)
  const transactions = block.transactions ?? []
  // typed optional in bitcoinjs, but always set by fromBuffer for a real header
  if (!block.prevHash) throw new Error('[decoder] block header missing prevHash')
  return {
    hash: block.getId(),
    // header stores prevHash little-endian; reverse a copy for display order
    prevHash: Buffer.from(block.prevHash).reverse().toString('hex'),
    time: block.timestamp,
    txs: transactions.map((tx) => toDecodedTx(tx, tx.toHex(), network)),
  }
}

/** True when `address` parses as a valid address FOR THIS NETWORK. */
export function isValidAddress(address: string, network: Network): boolean {
  const params = BITCOINJS_NETWORKS[network]
  try {
    bitcoinjs.address.toOutputScript(address, params)
    return true
  } catch {
    // toOutputScript throws for malformed/wrong-network addresses — that IS
    // the validation result, not an error to propagate. BUT it also throws
    // for every taproot address because payments.p2tr requires an ECC lib
    // (initEccLib) that weir deliberately doesn't depend on — handle witness
    // v1 below instead of misreporting valid p2tr addresses as invalid.
  }
  try {
    // fromBech32 enforces checksum + encoding variant (v0 must be bech32,
    // v1+ must be bech32m), so this only ever admits well-formed addresses.
    const { version, prefix, data } = bitcoinjs.address.fromBech32(address)
    // BIP350: a witness v1 address is valid when the HRP matches and the
    // program is 32 bytes — point validity is NOT part of address validity.
    return version === 1 && prefix === params.bech32 && data.length === 32
  } catch {
    return false
  }
}
