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

test("releases resources on asyncDispose()", async () => {
  await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  const signer = linera.signer.PrivateKey.fromMnemonic("test test test test test test test test test test test junk");

  const wallet1 = await faucet.createWallet();
  await faucet.claimChain(wallet1, signer.address());
  const client1 = await new linera.Client(wallet1, signer);
  await client1.asyncDispose();

  const wallet2 = await faucet.createWallet();
  await faucet.claimChain(wallet2, signer.address());
  const client2 = await new linera.Client(wallet2, signer);
  await client2.asyncDispose();
}, 150000)
