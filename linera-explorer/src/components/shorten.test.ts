import { short_crypto_hash, short_app_id, short_id } from '../../pkg/linera_explorer'
import { set_test_config } from './utils'

const HASH = '1fe0d0bb557f1a9057a2fca119566b439aa70d04918b71ea1485d5da2c7566b5'
// `AccountOwner` is displayed with a `0x` prefix, so it is not a valid `CryptoHash`.
const OWNER = '0x5487b70625ce71f7ee29154ad32aefa1c526cb483bdb783dea2e1d17bc497844'

beforeAll(async () => {
  await set_test_config()
})

test('short_crypto_hash shortens a hash', () => {
  expect(short_crypto_hash(HASH)).toBe('1fe0d0bb557f1a90')
})

test('short_crypto_hash falls back instead of panicking on a non-hash', () => {
  // Regression test: an `AccountOwner` used to panic here, taking down the whole UI.
  expect(short_crypto_hash(OWNER)).toBe('0x54..7844')
  expect(short_crypto_hash('')).toBe('')
  expect(short_crypto_hash('not a hash at all')).toBe('not .. all')
})

test('short_id and short_app_id elide the middle of long identifiers only', () => {
  expect(short_id('0x00')).toBe('0x00')
  expect(short_id(OWNER)).toBe('0x54..7844')
  expect(short_app_id(HASH)).toBe('1fe0..66b5')
})
