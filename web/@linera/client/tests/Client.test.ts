import { expect, test } from "vitest";
import * as linera from "../dist";

async function getWallet() {
  await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  return await faucet.createWallet();
}

test("successfully retrieves a wallet from the faucet", async () => {
  await getWallet();
});

test("successfully runs the client and connects to the network", async () => {
  await new linera.Client(await getWallet(), linera.signer.PrivateKey.createRandom());
});

test("fails to create a wallet that already exists", async () => {
  const lin = await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  const signer = linera.signer.PrivateKey.fromMnemonic("test test test test test test test test test test test junk");
  const wallet1 = await faucet.createWallet();
  const _chainId = await faucet.claimChain(wallet1, signer.address());
  const wallet2 = await faucet.createWallet();

  try {
    await faucet.claimChain(wallet2, signer.address());
  } catch (e) {
    expect(e).toBeInstanceOf(linera.LockError);
    return;
  }

  throw new Error("should have errored");
});
