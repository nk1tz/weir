/**
 * Decoder test fixtures. Raw tx hexes adapted from
 * blockhooksV2/packages/zmqapp/src/lib/decodeRawTransaction/rawTransactionExamples.json
 * (real mainnet/testnet transactions), with expected outputs EXTENDED to weir's
 * contract: every output present (OP_RETURN etc. as address:null), real vout
 * index, valueSats. Genesis block hex added for decodeBlock.
 */
import { DecodedOutput, Network } from '../../src/lib/types'

export interface TxFixture {
  name: string
  network: Network
  rawTxHex: string
  txid: string
  outputs: DecodedOutput[]
}

export const txFixtures: TxFixture[] = [
  {
    name: 'mainnet mixed p2sh/p2tr/p2wpkh (7 outputs)',
    network: 'mainnet',
    rawTxHex:
      '02000000000104d6e02bb00761155432a8a454f13b871b22a84ded95fb82b80af2d3edb125cc79040000001716001411a185349d00f1872d62f94465b881a55c9d33fdffffffff921a9be1096e8d4632f65f0228cc40968383cacdb3aebde5d47396fcb4c838ea050000001716001411a185349d00f1872d62f94465b881a55c9d33fdffffffffae3093785a1137106b652166cfeac51ef37b91e19814a4618d22e3cc2bee76ca0000000000ffffffffd6e02bb00761155432a8a454f13b871b22a84ded95fb82b80af2d3edb125cc79060000001716001411a185349d00f1872d62f94465b881a55c9d33fdffffffff07b00400000000000017a91449fed4f3e370e8b316211da393a83afaad98fa72871027000000000000225120b8ec578ed8b81af234c5a926d9ab582614f583ba6093e4dd4e45649b92b5544abcab01000000000017a914a80053a49e90f825c3a17dcc9911be34336dfb8187c409000000000000160014c015c65276d5f38d599d445c4cb03aa7aa0dc365580200000000000017a91449fed4f3e370e8b316211da393a83afaad98fa7287580200000000000017a91449fed4f3e370e8b316211da393a83afaad98fa7287984a01000000000017a91449fed4f3e370e8b316211da393a83afaad98fa728702483045022100f8473466d330a9c82f130b80fe04e766d363b43541ea152aa9799cedb404243502205f70bf21d55082ab751fe6945311ff92141b554abb8bbda20e626ea1191b30ca012102b0082e37e891aab629da6bfc40665de22b1d49c0eb01dea3e65b5801fe1328e80248304502210088e63cc574731a84006a1a26de031384adb6740b1c3ba5522bf580dd37fe0f9f02206e7386553fea5522fc483795388f0d145871078113ec7bce059166e6f6bf3bf1012102b0082e37e891aab629da6bfc40665de22b1d49c0eb01dea3e65b5801fe1328e801419d6cf7ddf7747a0f1ca7adbb5ebd1221c053191517c4c0d13d7b554e231359d3a2087dc8ad15ddfe1af901a0263a4304a0c9827a13d4611829fdc572049a2a7e8302473044022017876c10b61cafa920d570fdade455d3356d9269073f8aac9c9796d20c28cd2a022051b206a65ee62d95c99a31bf07c0dc9b2a9095684456794b2e06279a29341b24012102b0082e37e891aab629da6bfc40665de22b1d49c0eb01dea3e65b5801fe1328e800000000',
    txid: 'c85f1c0c429c1027c809ef71a0e05aebc34cff7d13f792e1ac0ae216a016b1b5',
    outputs: [
      { vout: 0, valueSats: 1200, address: '38SGXaGnBmrBhqmhc7soNvp5P3W7H6Nv6G', scriptType: 'p2sh' },
      {
        vout: 1,
        valueSats: 10000,
        address: 'bc1phrk90rkchqd0ydx94yndn26cyc20tqa6vzf7fh2wg4jfhy44239qmetdwe',
        scriptType: 'p2tr',
      },
      { vout: 2, valueSats: 109500, address: '3H1KqUP7ZDbWtXzdK88N33m1q1Qkbz8c9c', scriptType: 'p2sh' },
      { vout: 3, valueSats: 2500, address: 'bc1qcq2uv5nk6hec6kvag3wyevp6574qmsm9scjxc2', scriptType: 'p2wpkh' },
      { vout: 4, valueSats: 600, address: '38SGXaGnBmrBhqmhc7soNvp5P3W7H6Nv6G', scriptType: 'p2sh' },
      { vout: 5, valueSats: 600, address: '38SGXaGnBmrBhqmhc7soNvp5P3W7H6Nv6G', scriptType: 'p2sh' },
      { vout: 6, valueSats: 84632, address: '38SGXaGnBmrBhqmhc7soNvp5P3W7H6Nv6G', scriptType: 'p2sh' },
    ],
  },
  {
    name: 'mainnet mixed p2wpkh/p2tr/p2sh/p2wsh (9 outputs)',
    network: 'mainnet',
    rawTxHex:
      '01000000000103bfd1e4a981aa6c9f8bdc32a91f0091ecc8a57f8ea52779a3b0e2e2ca8f26dc5e0400000000ffffffff32179209f578564954163a56886c7c3e71a8c65805b707101cfb938f4868008300000000fdfe00004830450221009efc36dd7edb5e439d1beb9da931e4ce5c7ef295121a74d6db23100d912eda110220602f597f6ade14a39b64320f3d90ce460575714c6db030a49619cb647152b94a0148304502210094975623353c941885f70d2ea23fcac79957fb4c01eedaabec6f7c01b17a4c6e0220548d7aef6f53e2eacdb1e2ddcc1c95be2db5f2c3af2590c42e5c64f1846f0ec6014c69522103a93724f6d774d6b1b13aef4ef9e1e4bf56c36b34aff380b180f6ada9c6634cd02103fd0e9a025201605f4a51df9dc17e37fec28c499b8cb4f8760742275f19fc643b2102bbe78b3b6682a99e0dd918c26a1884d3255558b0026a8beb72a6d47eba223d8053aeffffffff00f50867029e6869896528703e117d646250cee4e156857b901f382b5599af9f0500000000ffffffff090478100000000000160014f3fd922385654780d24a1a04d027692d4f992cd0d8729900020000002251209f3a8ed509d96dd2678bdbcb98f9b650a04b0d9aa542fa97ef363a232bad2584f0c602000000000017a91441d8eb236bcd8bed568a9170b190333162c1d48787a0f01900000000002200200f63ce402bae1523729997aedb39b1234ffeab7031c0f69a54ab9508d7c745a2ff45d00000000000160014e8705f088e6db8197614c4343f56220d9ba97716721e42150000000017a9143a97a9f255c5f554002f52b1ec68f25eea63ab738750c9290000000000160014438928e7b3b856e07f4c3aa0bfebb193424919f2f7795b00000000001600144a3c9a849a3de0270d36f38a7a02d4cd534e43f5ddc03f3600000000220020e5c7c00d174631d2d1e365d6347b016fb87b6a0c08902d8e443989cb771fa7ec040047304402206510aff212c3f5cce0bdeedc1e3587fbc5e517224c43691b1a52d6745020417302205ede6afb494cd08658a55b95dedece0a8cf33a50bb634985e3e2c6342f98c48501473044022012eb833a2c468d66632e2e9ae38450405e7c06f98822d92e8c258bf24c54fb21022058182766919d984a61133a0eeba5a49820325d2efd812dbbca2d9ec3e7ae8db701695221026064e5b88c4fff7dba7dc0300db8dbfc1faff14f9ddbaacbcaa4f70124de0e93210331870350912385ca9a9d537e9cf9d80c6c9558e31d654f82f3164fdc5955e9642103c7b133a0f463a501d8c58c8eb8c7b6e9e4ddfb7d4a7bf6365a4732201569bc8353ae00040047304402201bbf44040f25cc7585d5c9f7ed30c031df0f956d540162ed0e252855bdc7d6ee02204f4e738c80e87466c5bc7e26e29a2a1f17b50bf49fd4f8925140bab2c538054701483045022100b45fd576d498e6fe910340f7e02d1df7e1e4e730133a830ae870435e3e5b157a0220013ab9af5038bb04797c9fccb9300137b859163a5d3883b9157d6dd616977af101695221026064e5b88c4fff7dba7dc0300db8dbfc1faff14f9ddbaacbcaa4f70124de0e93210331870350912385ca9a9d537e9cf9d80c6c9558e31d654f82f3164fdc5955e9642103c7b133a0f463a501d8c58c8eb8c7b6e9e4ddfb7d4a7bf6365a4732201569bc8353ae00000000',
    txid: '2f4d8b12392b92a50a544f209f06f7e779e83e30eaf1e48df53c41c87b2e8b0b',
    outputs: [
      {
        vout: 0,
        valueSats: 1079300,
        address: 'bc1q707eygu9v4rcp5j2rgzdqfmf948ejtxsslw5gu',
        scriptType: 'p2wpkh',
      },
      {
        vout: 1,
        valueSats: 8599991000,
        address: 'bc1pnuaga4gfm9kayeutm09e37dk2zsykrv654p049l0xcazx2adykzqvztfh6',
        scriptType: 'p2tr',
      },
      { vout: 2, valueSats: 182000, address: '37hBhmuHETzLN1z4ocLfVTX3V9siKHpjHg', scriptType: 'p2sh' },
      {
        vout: 3,
        valueSats: 1700000,
        address: 'bc1qpa3uuspt4c2jxu5ej7hdkwd3yd8la2msx8q0dxj54w2s3478gk3ql7k4sh',
        scriptType: 'p2wsh',
      },
      {
        vout: 4,
        valueSats: 13649407,
        address: 'bc1qapc97zywdkupjas5cs6r743zpkd6jackjuenpy',
        scriptType: 'p2wpkh',
      },
      { vout: 5, valueSats: 356654706, address: '372poTb7ZfJE6CV8yhTM3PtMzzq1bATDUD', scriptType: 'p2sh' },
      {
        vout: 6,
        valueSats: 2738512,
        address: 'bc1qgwyj3eanhptwql6v82stl6a3jdpyjx0j5ujks2',
        scriptType: 'p2wpkh',
      },
      {
        vout: 7,
        valueSats: 5994999,
        address: 'bc1qfg7f4py68hszwrfk7w985qk5e4f5usl422k9hw',
        scriptType: 'p2wpkh',
      },
      {
        vout: 8,
        valueSats: 910147805,
        address: 'bc1quhruqrghgcca950rvhtrg7cpd7u8k6svpzgzmrjy8xyukacl5lkq0r8l2d',
        scriptType: 'p2wsh',
      },
    ],
  },
  {
    name: 'mainnet mixed p2wpkh/p2pkh/p2sh (6 outputs)',
    network: 'mainnet',
    rawTxHex:
      '010000000001017ddb56b0b91e19bbc22cb3ea113a7a50dd8c687561a98794dd29815b50ba73290000000000fdffffff065571f01900000000160014bc2280aec6cee84f13a05110fe4d5b3dfb9d39674cf00b00000000001976a91468ee42bbf540cb73ae4a2b37f72b642e5feb62f188ac425a0a0000000000160014e8c4d6e1230dbe749c44a72a2b9b6e9ebc94104080e60400000000001976a914459f3411b9610a1c906b38b5eae7350f7eff6e9388ac76500300000000001976a914474ad633a3e598062d445d9fad5fcfec89af0b7388ac552a03000000000017a914bab0d72e80d44fe9524020708f7c26aaacadb34087024830450221009433ccf0ccb955d61ca1cc68f160d8f0f722addb7937fe7fa4417de06aac6b54022023f597b236d861af5ca5e0062902830bf960e0b020907be13e563180056ea81a01210282d649902c7cb2500e8a3489bbe09504e2c49c632bfbb1803d813e535279d28c00000000',
    txid: '704668d96430b0e8c9d03d218228ab11ef3ddf119248362dce29924630c238d3',
    outputs: [
      {
        vout: 0,
        valueSats: 435188053,
        address: 'bc1qhs3gptkxem5y7yaq2yg0un2m8hae6wt87gkx4n',
        scriptType: 'p2wpkh',
      },
      { vout: 1, valueSats: 782412, address: '1AZphbNjdmsP4oHNAyZVvDJEzfNEDieHAC', scriptType: 'p2pkh' },
      {
        vout: 2,
        valueSats: 678466,
        address: 'bc1qarzddcfrpkl8f8zy5u4zhxmwn67fgyzqcnkvps',
        scriptType: 'p2wpkh',
      },
      { vout: 3, valueSats: 321152, address: '17M8LtAWnuVJ1EzUreUcBZQ7ZQUaSLoARX', scriptType: 'p2pkh' },
      { vout: 4, valueSats: 217206, address: '17VxdNGvf4EioqGyZhi6exx8tKdV5kMvNA', scriptType: 'p2pkh' },
      { vout: 5, valueSats: 207445, address: '3Ji9TKazG74BeLtKkrYqVnERbm8m59oTog', scriptType: 'p2sh' },
    ],
  },
  {
    name: 'testnet p2tr + p2sh',
    network: 'testnet',
    rawTxHex:
      '02000000000102f4601bcf8e4511823d08768266bb2f041067585c2700b9630118249e6979e9200100000017160014112780f20db3c47ca00f1a074b3a1282cd4524c0ffffffffc4cabcd99f5e559d9b7d1933f92ef5ffdac0dd868417e9a9e9d47cf12cd8cdf30100000017160014112780f20db3c47ca00f1a074b3a1282cd4524c0ffffffff0283d60000000000002251207bcb779998de433365c359e01bc98b09b04dc3ad1c3bc6bb265a3b2ce4aa624dbecd0d000000000017a91457293a07f4fee31ec35d566e17b3be57c1ac73d28702483045022100c9fb5212b3e01796fa20f714467a4b982ccf4db91e11d078651674da32c1ef7b0220033303269ef3c75e512fadfb8869a1ec9fad3fdacc4edee46a38afaca3150c910121034343b320fb5b4bcfcc3b65762319947247c1f4e9743e79c7bdfaa7945ac26a5e024730440220362febc8e5e81a09a2b1f678531a64482efebd309ef00c51f644de58473bc873022010a4f0b8ae32e990807b0f89f438e1eaff126d9b1a0c4008750a029a664cc2dd0121034343b320fb5b4bcfcc3b65762319947247c1f4e9743e79c7bdfaa7945ac26a5e00000000',
    txid: '2e7073d686f3ae02715a7c3bbbf03a0d1d859b39c5f6ec46e22f093e472d285a',
    outputs: [
      {
        vout: 0,
        valueSats: 54915,
        address: 'tb1p009h0xvcmepnxewrt8sphjvtpxcymsadrsaudwextgajee92vfxsvljhv9',
        scriptType: 'p2tr',
      },
      { vout: 1, valueSats: 904638, address: '2N1C6ATFYM7KkRX8wi4LgqyccjEgrUdCVnA', scriptType: 'p2sh' },
    ],
  },
  {
    name: 'testnet p2pkh + p2wpkh',
    network: 'testnet',
    rawTxHex:
      '01000000000101e67d071c53ac643107891d1b9ddfdac97c992a2d288fdd568cf104d9be0a87770100000000ffffffff02a1230000000000001976a9149c4b12bb5a2e7e4b2721a25d8abebd6a8144d41288ac11b3640f00000000160014c783068b2593c7138d8744956f9d048032c580800247304402202db283392430412fe08efb4d2d5cd5e76fbd906c3e65da5f83745ba9b1311b4202205af6bc3fbcce68f93773e92e32ff9ad6fc5fde1d94bee482f8bd2245f3f2904e012103f500418025ba3babca935e9f7617c438210ab72ae3ece0b25e5dff579c31ddd100000000',
    txid: 'e6bdde7e2ef03a49777d69167cb921744c07ea4092052067d32468082299fa81',
    outputs: [
      { vout: 0, valueSats: 9121, address: 'mumMe9wWeLb26oGKf334CKiRYiWMpmKpUp', scriptType: 'p2pkh' },
      {
        vout: 1,
        valueSats: 258257681,
        address: 'tb1qc7psdze9j0r38rv8gj2kl8gysqevtqyqs20upw',
        scriptType: 'p2wpkh',
      },
    ],
  },
  {
    name: 'testnet coinbase with OP_RETURN witness commitment (kept as null)',
    network: 'testnet',
    rawTxHex:
      '010000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff1a039937250120130909092009092009047223000b0f0000000000ffffffff02f894250000000000160014820d4a343a44e915c36494995c2899abe37418930000000000000000266a24aa21a9ed72b67c960e20791b83f769f2b000d07b4bf0c9d85901538ce3060dc8df0c077c0120000000000000000000000000000000000000000000000000000000000000000000000000',
    txid: 'a10cf4e6884d25b1a68fe6a930f48fe1caf6d3c3a9c04bd5af84265c9e9648d7',
    outputs: [
      {
        vout: 0,
        valueSats: 2462968,
        address: 'tb1qsgx55dp6gn53tsmyjjv4c2ye403hgxynxs0dnm',
        scriptType: 'p2wpkh',
      },
      { vout: 1, valueSats: 0, address: null, scriptType: null },
    ],
  },
  {
    name: 'testnet with mid-array OP_RETURN (vout indices preserved)',
    network: 'testnet',
    rawTxHex:
      '02000000000101fa7cda7134fb826640580c051542335a955615ccd0a858ad1f5bbc8f350fb1830200000000feffffff0360ea00000000000017a914a51ae5ec8a66d620ff11bf1fd0fe53df68909e228700000000000000001c6a1a890001a275f02323759be3e09f761a7de91e76a07eb607014d00eeaa000000000000160014668c9168ffe154c4f9a189e19f6fe9bd8e74a96002483045022100f14de028978534ee013b665625933e6924ed20bd5958181ad9b7677e7f021be202202ba3e407202f087f11a21581b38bbfaf8f5bb795c70747d76d761b871e00b121012103075a282b6e1726c30c5b4563a89976d48a7d359bc3cf83a53da5e84facc64fca00000000',
    txid: 'b31dde7472809b73e9d4edb701c43a2d02861007bcb462c5229a98bfc1964892',
    outputs: [
      { vout: 0, valueSats: 60000, address: '2N8JDhrLqtwZ4MGC1QAcwyiQg3v6ffhCrJb', scriptType: 'p2sh' },
      { vout: 1, valueSats: 0, address: null, scriptType: null },
      { vout: 2, valueSats: 43758, address: 'tb1qv6xfz68lu92vf7dp38se7mlfhk88f2tqyzcrzs', scriptType: 'p2wpkh' },
    ],
  },
  {
    name: 'mainnet coinbase p2pkh + OP_RETURN (kept as null)',
    network: 'mainnet',
    rawTxHex:
      '010000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff3b03d5240c20012f506f7765726564206279204c75786f7220546563682f33320d5e74d8485005fb68a93df18ad30b722010862f7833210000000000ffffffff02817ab226000000001976a914e44bef4026aacca861b70f1fa6f7e1c134ab543d88ac0000000000000000266a24aa21a9edf5591a810bcaedc85a31fe2f3d964839b65535dc76cf2ee70f7b3cf6233b366b0120000000000000000000000000000000000000000000000000000000000000000000000000',
    txid: '33b68057b18e1226064c37dcaf4e53142115ac8025ed35b7fea6ebb70f5a5b29',
    outputs: [
      { vout: 0, valueSats: 649230977, address: '1Mp82mJt6d8XzX8bN2GAkpDqBukiwLuKrA', scriptType: 'p2pkh' },
      { vout: 1, valueSats: 0, address: null, scriptType: null },
    ],
  },
]

/**
 * The mainnet genesis block, raw. Known-good header values for decodeBlock:
 * hash 000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f,
 * prevHash all zeros, time 1231006505. Its single (coinbase) tx pays a P2PK
 * output — not one of weir's five types, so address must decode to null.
 */
export const genesisBlockHex =
  '0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000'

export const genesisBlockHash = '000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f'
export const genesisPrevHash = '0000000000000000000000000000000000000000000000000000000000000000'
export const genesisTime = 1231006505
export const genesisTxid = '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
export const genesisCoinbaseValueSats = 5000000000
