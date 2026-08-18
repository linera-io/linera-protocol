import { expect, test } from "vitest";
import * as linera from "../dist";

// Claims a chain from the faucet, then connects to it with a signer whose `sign` throws
// `thrown` while its other methods keep working. Signing only happens when a block is
// proposed, so the chain is usable up until the operation under test.
async function chainRefusingToSign(thrown: unknown) {
  await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  const inner = linera.signer.PrivateKey.createRandom();
  const owner = inner.address();
  const wallet = await faucet.createWallet();
  const chainId = await faucet.claimChain(wallet, owner);

  const signer: linera.Signer = {
    async sign(_owner: string, _value: Uint8Array): Promise<string> {
      throw thrown;
    },
    getPublicKey: (owner: string) => inner.getPublicKey(owner),
    containsKey: (owner: string) => inner.containsKey(owner),
  };

  const client = await new linera.Client(wallet, signer);
  const chain = await client.chain(chainId, { owner });
  await chain.synchronize();
  return chain;
}

// Proposing a block is the cheapest operation that asks the signer for a signature.
function propose(chain: linera.Chain) {
  const stranger = linera.signer.PrivateKey.createRandom().address();
  return chain.addOwner(stranger, { weight: 100 });
}

test("reports the message of an `Error` thrown by the signer", async () => {
  const chain = await chainRefusingToSign(new Error("the vault is on fire"));
  await expect(propose(chain)).rejects.toThrow("the vault is on fire");
}, 150000);

test("reports the `name` of a thrown error that is not a plain `Error`", async () => {
  const thrown = new DOMException("the user said no", "NotAllowedError");
  const chain = await chainRefusingToSign(thrown);
  await expect(propose(chain)).rejects.toThrow("NotAllowedError: the user said no");
}, 150000);

test("reports a thrown value that is not an error at all", async () => {
  const chain = await chainRefusingToSign("the vault is on fire");
  await expect(propose(chain)).rejects.toThrow("the vault is on fire");
}, 150000);
