/**
 * Boot preflight checks, in order, each with a one-line log. Throws (fatal) unless noted.
 *
 * Spec: docs/DESIGN.md "src/boot/preflight.ts".
 */
import type { Network } from '../lib/types'
import type { Rpc } from '../bitcoin/rpc'
import type { Store } from '../store/redis'
import { log } from '../lib/log'

const CTX = 'preflight'

/** bitcoind's getblockchaininfo `chain` value for each weir network */
const CHAIN_FOR: Record<Network, string> = {
  mainnet: 'main',
  testnet: 'test',
  signet: 'signet',
  regtest: 'regtest',
}

export interface PreflightDeps {
  cfg: { network: Network }
  store: Pick<Store, 'memoryInfo' | 'maxmemoryPolicy'>
  rpc: Pick<Rpc, 'getBlockchainInfo' | 'getZmqNotifications'>
}

export async function preflight(deps: PreflightDeps): Promise<void> {
  // 1. redis reachable + eviction policy must not silently delete watches
  const mem = await deps.store.memoryInfo() // throws if redis is unreachable — fatal
  log.info(CTX, `redis reachable (used ${(mem.usedBytes / 1024 / 1024).toFixed(1)} MB)`)
  const policy = await deps.store.maxmemoryPolicy()
  if (policy === null) {
    log.warn(CTX, 'redis CONFIG is blocked (managed redis?) — cannot verify maxmemory-policy; ensure it is noeviction')
  } else if (policy !== 'noeviction') {
    throw new Error(
      `preflight: redis maxmemory-policy is "${policy}" — must be "noeviction" ` +
        '(any eviction policy would silently delete watches). Set: CONFIG SET maxmemory-policy noeviction',
    )
  } else {
    log.info(CTX, 'redis maxmemory-policy=noeviction — OK')
  }

  // 2. rpc reachable + chain matches NETWORK; pruned is fine
  const info = await deps.rpc.getBlockchainInfo() // throws if bitcoind is unreachable — fatal
  const expectedChain = CHAIN_FOR[deps.cfg.network]
  if (info.chain !== expectedChain) {
    throw new Error(
      `preflight: bitcoind chain "${info.chain}" does not match NETWORK=${deps.cfg.network} (expected chain "${expectedChain}")`,
    )
  }
  log.info(
    CTX,
    `bitcoind reachable: chain=${info.chain} blocks=${info.blocks} ` +
      (info.pruned ? 'pruned=true — pruned is fine, weir needs no txindex' : 'pruned=false'),
  )

  // 3. ZMQ must publish rawtx AND rawblock
  const zmq = await deps.rpc.getZmqNotifications()
  const types = new Set(zmq.map((n) => n.type))
  if (!types.has('pubrawtx') || !types.has('pubrawblock')) {
    throw new Error(
      'preflight: bitcoind is missing required ZMQ publishers ' +
        `(found: ${zmq.length === 0 ? 'none' : [...types].join(', ')}). Add to bitcoin.conf and restart bitcoind:\n` +
        '  zmqpubrawtx=tcp://0.0.0.0:28332\n' +
        '  zmqpubrawblock=tcp://0.0.0.0:28332',
    )
  }
  log.info(CTX, `zmq publishers OK (${zmq.map((n) => `${n.type}=${n.address}`).join(', ')})`)

  // 4. capacity estimate for the configured redis maxmemory
  if (mem.maxBytes !== null) {
    const capacity = Math.max(0, Math.floor((mem.maxBytes / 1.5 - 45 * 1024 * 1024) / 330))
    log.info(
      CTX,
      `redis maxmemory ${(mem.maxBytes / 1024 / 1024).toFixed(0)} MB — estimated watch capacity ~${capacity} addresses`,
    )
  } else {
    log.info(CTX, 'redis has no maxmemory set — skipping capacity estimate')
  }
}
