import { ethers } from "ethers";
import { PrivateKey } from "../src";

test("constructs signer from mnemonic correctly", async () => {
  const phrase = "test test test test test test test test test test test junk";

  const signer = PrivateKey.fromMnemonic(phrase);
  const expectedWallet = ethers.Wallet.fromPhrase(phrase);

  // In Linera EIP-191 compatible wallet, the owner is the wallet address.
  const owner = expectedWallet.address.toLowerCase();
  const publicKey = await signer.getPublicKey(owner);

  expect(publicKey).toBe(expectedWallet.signingKey.publicKey.toLowerCase());
  expect(await signer.containsKey(owner)).toBe(true);
});

test("signs message correctly", async () => {
  const secretKey =
    "f77a21701522a03b01c111ad2d2cdaf2b8403b47507ee0aec3c2e52b765d7a66";
  const signer = new PrivateKey(secretKey);
  const cryptoHash =
    "c520e2b24b05e70c39c36d4aa98e9129ac0079ea002d4c382e6996ea11946d1e";
  const owner = signer.address().toLowerCase();
  const signature = await signer.sign(owner, Buffer.from(cryptoHash, "hex"));
  expect(signature).toBe(
    "0xe257048813b851f812ba6e508e972d8bb09504824692b027ca95d31301dbe8c7103a2f35ce9950d031d260f412dcba09c24027288872a67abe261c0a3e55c9121b",
  );
});
