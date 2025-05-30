import { ethers } from 'ethers';
import { EmbeddedEIP191Signer } from '../src';

test('constructs signer from mnemonic correctly', async () => {
  const phrase = 'test test test test test test test test test test test junk';

  const signer = EmbeddedEIP191Signer.fromMnemonic(phrase);
  const expectedWallet = ethers.Wallet.fromPhrase(phrase);

  // In Linera EIP-191 compatible wallet, the owner is the wallet address.
  const owner = expectedWallet.address.toLowerCase();
  const publicKey = await signer.get_public_key(owner);

  expect(publicKey).toBe(expectedWallet.signingKey.publicKey.toLowerCase());
  expect(await signer.contains_key(owner)).toBe(true);
});