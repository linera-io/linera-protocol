import { beforeAll, expect, test } from "vitest";
import * as linera from "../dist";

// The value the signer throws the next time it is asked to sign.
let thrown: unknown;
let chain: linera.Chain;

// One faucet chain serves every case below: signing is the last step of proposing a
// block, so a refused signature commits nothing and leaves the chain reusable.
beforeAll(async () => {
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
  chain = await client.chain(chainId, { owner });
  await chain.synchronize();
}, 150000);

// Proposes a block — the only operation that asks the signer for a signature — with the
// signer set to throw `value`, and returns the error that comes back.
async function refusing(value: unknown): Promise<Error> {
  thrown = value;
  const stranger = linera.signer.PrivateKey.createRandom().address();
  try {
    await chain.addOwner(stranger, { weight: 100 });
  } catch (error) {
    // A refused signature should leave nothing queued, but make sure, so that one case
    // cannot disturb the next.
    await chain.clearPendingProposal().catch(() => {});
    expect(error).toBeInstanceOf(Error);
    return error as Error;
  }
  throw new Error("the block was proposed without a signature");
}

test("reports the message of a thrown `Error`", async () => {
  const error = await refusing(new Error("the vault is on fire"));
  expect(error.message).toContain("the vault is on fire");
}, 150000);

test("reports the `name` of a thrown error that is not a plain `Error`", async () => {
  const error = await refusing(
    new DOMException("the user said no", "NotAllowedError"),
  );
  expect(error.message).toContain("NotAllowedError: the user said no");
}, 150000);

test("reports the `cause` chain of a thrown error", async () => {
  const inner = new Error("the key is missing");
  const middle = new Error("the vault will not open", { cause: inner });
  const error = await refusing(
    new Error("cannot sign", { cause: middle }),
  );
  expect(error.message).toContain(
    "cannot sign: the vault will not open: the key is missing",
  );
}, 150000);

test("terminates on a `cause` chain that points at itself", async () => {
  const looping = new Error("round and round");
  looping.cause = looping;
  const error = await refusing(looping);
  expect(error.message).toContain("round and round");
  // The chain is cut rather than followed forever, and says so.
  expect(error.message).toContain("(further causes omitted)");
}, 150000);

test("reports thrown values that are not errors at all", async () => {
  expect((await refusing("the vault is on fire")).message).toContain(
    "the vault is on fire",
  );
  expect((await refusing(42)).message).toContain("42");
  expect((await refusing({ code: 3 })).message).toContain('{"code":3}');
  // Neither a string nor JSON-representable: `Promise.reject()` with no argument.
  // `toContain` alone would pass against the old `JsValue(undefined)` rendering, so
  // rule the Rust-internal debug form out explicitly.
  const undef = (await refusing(undefined)).message;
  expect(undef).toContain("undefined");
  expect(undef).not.toContain("JsValue");
}, 150000);
