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
  // The chain was just claimed, so pull its state into the local node before the tests
  // read from it (balance, ownership, round are all local reads).
  await chain.synchronize();
  return { chain, chainId, owner };
}

test("nextRoundInfo reports a well-formed multi-leader round for a fresh chain", async () => {
  const { chain } = await freshChain();
  const round = await chain.nextRoundInfo();

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

test("ownerWeight reads owner weights", async () => {
  const { chain } = await freshChain();
  const owner = linera.signer.PrivateKey.createRandom().address();

  // An address that is not a regular owner has no weight.
  expect(await chain.ownerWeight(owner)).toBeUndefined();

  await chain.addOwner(owner, { weight: 100 });
  expect(await chain.ownerWeight(owner)).toBe(100);

  // Re-adding an existing owner overwrites its weight.
  await chain.addOwner(owner, { weight: 250 });
  expect(await chain.ownerWeight(owner)).toBe(250);
}, 150000);

test("ownerBalance reads the owner account, which is not the chain balance", async () => {
  const { chain, chainId, owner } = await freshChain();

  // The faucet funds the chain account. The owner's account on that same chain
  // is a different pot, and starts empty.
  expect(Number(await chain.balance())).toBeGreaterThan(0);
  expect(Number(await chain.ownerBalance(owner))).toBe(0);

  // A transfer addressed to `{ chain_id, owner }` lands in the owner account,
  // and only `ownerBalance` can see it.
  await chain.transfer({ recipient: { chain_id: chainId, owner }, amount: 1 });
  expect(Number(await chain.ownerBalance(owner))).toBe(1);
}, 150000);

test("clearPendingProposal is a no-op when nothing is pending", async () => {
  const { chain } = await freshChain();
  // No proposal has failed, so there is nothing to clear; this should resolve.
  await chain.clearPendingProposal();
  // The round structure is unchanged.
  expect((await chain.nextRoundInfo()).kind).toBe("multiLeader");
}, 150000);

test("setMultiLeaderRounds changes the round structure", async () => {
  const { chain, owner } = await freshChain();
  expect((await chain.nextRoundInfo()).kind).toBe("multiLeader");

  // With zero multi-leader rounds, an owner-based chain starts directly in a
  // single-leader round, where the sole owner is the designated leader.
  await chain.setMultiLeaderRounds(0);
  const single = await chain.nextRoundInfo();
  expect(single.kind).toBe("singleLeader");
  // The serialized leader is lowercase, while `owner` is an EIP-55 checksummed
  // address; they denote the same account, so compare case-insensitively.
  expect(single.leader?.toLowerCase()).toBe(owner.toLowerCase());
  expect(single.canPropose).toBe(true);

  // Restoring multi-leader rounds brings back the multi-leader round.
  await chain.setMultiLeaderRounds(3);
  expect((await chain.nextRoundInfo()).kind).toBe("multiLeader");
}, 150000);

test("chainInfo reports the chain's tip, epoch and balance", async () => {
  const { chain, chainId, owner } = await freshChain();
  const before = await chain.chainInfo();

  expect(before.chainId).toBe(chainId);
  expect(before.epoch).toBeGreaterThanOrEqual(0);
  // The faucet funds the chain account.
  expect(Number(before.balance)).toBeGreaterThan(0);
  // A chain with no blocks yet has no last block to point at, and vice versa.
  expect(before.blockHash === undefined).toBe(before.nextBlockHeight === 0);

  // Producing a block extends the log and leaves the tip pointing at it. The transfer
  // is to this same chain, so the listener may add a block of its own to receive the
  // message; the tip only has to move forward, not by exactly one.
  await chain.transfer({ recipient: { chain_id: chainId, owner }, amount: 1 });
  const after = await chain.chainInfo();
  expect(after.nextBlockHeight).toBeGreaterThan(before.nextBlockHeight);
  expect(after.blockHash).toBeDefined();
  expect(after.stateHash).toBeDefined();
}, 150000);

test("blocks walks the chain backwards from its tip", async () => {
  const { chain, chainId, owner } = await freshChain();
  await chain.transfer({ recipient: { chain_id: chainId, owner }, amount: 1 });
  await chain.transfer({ recipient: { chain_id: chainId, owner }, amount: 1 });

  const blocks = await chain.blocks({ limit: 100 });
  // Two transfers, so at least two blocks — the paging check below needs a predecessor.
  expect(blocks.length).toBeGreaterThanOrEqual(2);

  // The walk is contiguous, newest first, and each block names its predecessor.
  for (let i = 0; i < blocks.length - 1; i++) {
    expect(blocks[i].height).toBe(blocks[i + 1].height + 1);
    expect(blocks[i].previousBlockHash).toBe(blocks[i + 1].hash);
  }
  // `limit: 100` is more than a fresh chain has, so the walk reaches its first block,
  // which has no predecessor.
  const first = blocks[blocks.length - 1];
  expect(first.height).toBe(0);
  expect(first.previousBlockHash).toBeUndefined();

  // Paging follows `previousBlockHash`; there is no offset to skip by. Anchoring on a
  // block we already have keeps this independent of any block produced meanwhile.
  const [page1] = await chain.blocks({ from: blocks[0].hash, limit: 1 });
  expect(page1.hash).toBe(blocks[0].hash);
  const [page2] = await chain.blocks({ from: page1.previousBlockHash, limit: 1 });
  expect(page2.hash).toBe(blocks[1].hash);
}, 150000);

test("block returns the full block behind a summary", async () => {
  const { chain, chainId, owner } = await freshChain();
  await chain.transfer({ recipient: { chain_id: chainId, owner }, amount: 1 });

  const [summary] = await chain.blocks({ limit: 1 });
  const full = await chain.block(summary.hash);

  expect(full.hash).toBe(summary.hash);
  // The raw block keeps the protocol's own field names, so `chain_id`, not `chainId`.
  expect(full.block.header.height).toBe(summary.height);
  expect(full.block.header.chain_id).toBe(chainId);

  // The summary's counts describe the body it summarizes. Every transaction is either
  // an operation to execute or a bundle of messages to receive.
  expect(summary.operationCount + summary.incomingBundleCount).toBe(
    full.block.body.transactions.length,
  );
  expect(summary.outgoingMessageCount).toBe(full.block.body.messages.flat().length);
  expect(summary.eventCount).toBe(full.block.body.events.flat().length);

  // With no argument it resolves to whatever the chain's last block is by then.
  expect((await chain.block()).block.header.chain_id).toBe(chainId);

  // A hash that isn't one rejects the promise. It must not throw somewhere the caller
  // can't catch it: an explorer hands these straight to a search box.
  await expect(chain.block("not-a-hash")).rejects.toThrow();
  await expect(chain.blocks({ from: "not-a-hash" })).rejects.toThrow();
}, 150000);

test("blockHashAtHeight indexes the same blocks blocks() walks", async () => {
  const { chain, chainId, owner } = await freshChain();
  await chain.transfer({ recipient: { chain_id: chainId, owner }, amount: 1 });

  const blocks = await chain.blocks({ limit: 100 });
  for (const block of blocks) {
    expect(await chain.blockHashAtHeight(block.height)).toBe(block.hash);
  }

  // A height the chain is nowhere near has no block.
  expect(await chain.blockHashAtHeight(blocks[0].height + 1000)).toBeUndefined();

  // Heights that aren't non-negative integers are rejected rather than truncated.
  await expect(chain.blockHashAtHeight(-1)).rejects.toThrow();
  await expect(chain.blockHashAtHeight(1.5)).rejects.toThrow();
}, 150000);

test("applications lists the chain's applications", async () => {
  const { chain } = await freshChain();
  // A chain straight from the faucet runs nothing yet.
  expect(await chain.applications()).toEqual([]);
}, 150000);

test("events reads a chain's event stream", async () => {
  const { chain } = await freshChain();

  // Stream IDs travel in the form blocks print them: the publishing application,
  // then the hex-encoded stream name. A fresh chain has published nothing here.
  expect(await chain.events({ streamId: "System:65706f636873" })).toEqual([]);

  // A malformed stream ID is an error, not an empty result.
  await expect(chain.events({ streamId: "not-a-stream" })).rejects.toThrow();
}, 150000);

test("readBlob reads a blob the chain refers to", async () => {
  const { chain, chainId } = await freshChain();

  // A chain's ID is the hash of its own description blob, so this one always exists.
  const description = await chain.readBlob(`ChainDescription:${chainId}`);
  expect(description).toBeInstanceOf(Uint8Array);
  expect(description!.length).toBeGreaterThan(0);

  // Reusing that hash under another blob type names a blob that doesn't exist: it is
  // well-formed, so it reads as undefined rather than failing.
  expect(await chain.readBlob(`Data:${chainId}`)).toBeUndefined();

  // A malformed ID, on the other hand, is an error.
  await expect(chain.readBlob("nonsense")).rejects.toThrow();
}, 150000);
