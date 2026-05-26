import { expect, test } from "vitest";
import * as linera from "../dist";

const VALID_ROUND_KINDS = ["fast", "multiLeader", "singleLeader", "validator"];

// Claims a fresh chain from the faucet and returns a connected `Chain` handle
// together with the owner that controls it. The faucet hands out chains with a
// single regular owner and several multi-leader rounds (`ChainOwnership::single`),
// so a fresh chain starts in the multi-leader round.
async function freshChain() {
  await linera.initialize();
  const faucet = await new linera.Faucet(import.meta.env.LINERA_FAUCET_URL);
  const signer = linera.signer.PrivateKey.createRandom();
  const owner = signer.address();
  const wallet = await faucet.createWallet();
  const chainId = await faucet.claimChain(wallet, owner);
  const client = await new linera.Client(wallet, signer);
  const chain = await client.chain(chainId, { owner });
  return { chain, owner };
}

test("nextRound reports a well-formed multi-leader round for a fresh chain", async () => {
  const { chain } = await freshChain();
  const round = await chain.nextRound();

  expect(VALID_ROUND_KINDS).toContain(round.kind);
  expect(round.number).toBeGreaterThanOrEqual(0);
  // A fresh faucet chain has a single regular owner and starts in a multi-leader
  // round, where any eligible owner may propose (no single designated leader).
  expect(round.kind).toBe("multiLeader");
  expect(round.leader == null).toBe(true);
  // The chain's sole owner is the connected identity, so it may propose.
  expect(round.canPropose).toBe(true);
}, 150000);

test("isOwner reflects chain membership", async () => {
  const { chain, owner } = await freshChain();
  // The chain's sole owner is recognized; a random address is not.
  expect(await chain.isOwner(owner)).toBe(true);
  const stranger = linera.signer.PrivateKey.createRandom().address();
  expect(await chain.isOwner(stranger)).toBe(false);

  // After adding it, the stranger is recognized as an owner.
  await chain.addOwner(stranger, { weight: 100 });
  expect(await chain.isOwner(stranger)).toBe(true);
}, 150000);

test("clearPendingProposal is a no-op when nothing is pending", async () => {
  const { chain } = await freshChain();
  // No proposal has failed, so there is nothing to clear; this should resolve.
  await chain.clearPendingProposal();
  // The round structure is unchanged.
  expect((await chain.nextRound()).kind).toBe("multiLeader");
}, 150000);

test("setMultiLeaderRounds changes the round structure", async () => {
  const { chain, owner } = await freshChain();
  expect((await chain.nextRound()).kind).toBe("multiLeader");

  // With zero multi-leader rounds, an owner-based chain starts directly in a
  // single-leader round, where the sole owner is the designated leader.
  await chain.setMultiLeaderRounds(0);
  const single = await chain.nextRound();
  expect(single.kind).toBe("singleLeader");
  expect(single.leader).toBe(owner);
  expect(single.canPropose).toBe(true);

  // Restoring multi-leader rounds brings back the multi-leader round.
  await chain.setMultiLeaderRounds(3);
  expect((await chain.nextRound()).kind).toBe("multiLeader");
}, 150000);
