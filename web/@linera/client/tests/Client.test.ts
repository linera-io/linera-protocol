import { expect, test } from "vitest";
import * as linera from "../dist";

test("successfully retrieves a wallet from the faucet", async () => {
  await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  await faucet.createWallet();
});

test("successfully runs the client and connects to the network", async () => {
  await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  await new linera.Client(await faucet.createWallet(), linera.signer.PrivateKey.createRandom());
});

test("fails to create a wallet that already exists", async () => {
  const lin = await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  const signer = linera.signer.PrivateKey.createRandom();
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

test("releases resources on asyncDispose()", async () => {
  await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  const signer = linera.signer.PrivateKey.createRandom();

  const wallet1 = await faucet.createWallet();
  await faucet.claimChain(wallet1, signer.address());
  const client1 = await new linera.Client(wallet1, signer);
  await client1.asyncDispose();

  const wallet2 = await faucet.createWallet();
  await faucet.claimChain(wallet2, signer.address());
  const client2 = await new linera.Client(wallet2, signer);
  await client2.asyncDispose();
}, 150000)
