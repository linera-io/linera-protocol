import { expect, test } from "vitest";
import { ethers } from "ethers";
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
  await new linera.Client(await getWallet(), linera.PrivateKeySigner.createRandom());
});
