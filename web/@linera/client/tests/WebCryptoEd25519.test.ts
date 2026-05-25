import { afterEach, beforeEach, expect, test } from "vitest";
import * as linera from "../dist";

const RECORD_KEY = "test-record";
const DB_NAME = "linera-signer";

// Pinned by `linera-base/src/identifiers.rs::ed25519_public_key_to_account_owner_known_vector`.
// Bytes are 0x01..0x20; the expected hex is the captured output of that Rust test.
const KNOWN_PUBKEY = new Uint8Array([
  0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
  0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
  0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
  0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
]);
const OWNER_EXPECTED =
  "0xeacee5344cbec9569e836f95029d476c700f4f5bc007c71c0752c73fba149043";

beforeEach(async () => {
  await linera.initialize();
  await wipeDb();
});

afterEach(async () => {
  await wipeDb();
});

test("accountOwnerFromEd25519PublicKey matches Rust known vector", () => {
  const owner = linera.accountOwnerFromEd25519PublicKey(KNOWN_PUBKEY);
  expect(owner).toBe(OWNER_EXPECTED);
});

test("create() yields a signer whose address matches the derived owner", async () => {
  const signer = await linera.signer.WebCryptoEd25519.create(RECORD_KEY);
  const address = signer.address();
  expect(address).toMatch(/^0x[0-9a-f]{64}$/);
  expect(await signer.containsKey(address)).toBe(true);
  expect(await signer.containsKey("0x" + "0".repeat(64))).toBe(false);
});

test("load() returns null when no record exists", async () => {
  const loaded = await linera.signer.WebCryptoEd25519.load(RECORD_KEY);
  expect(loaded).toBeNull();
});

test("create() then load() returns a signer with the same address", async () => {
  const first = await linera.signer.WebCryptoEd25519.create(RECORD_KEY);
  const second = await linera.signer.WebCryptoEd25519.load(RECORD_KEY);
  expect(second).not.toBeNull();
  expect(second!.address()).toBe(first.address());
});

test("loaded signer can still sign — IndexedDB CryptoKey roundtrip preserves usability", async () => {
  const first = await linera.signer.WebCryptoEd25519.create(RECORD_KEY);
  const owner = first.address();
  const loaded = await linera.signer.WebCryptoEd25519.load(RECORD_KEY);
  expect(loaded).not.toBeNull();

  const message = new Uint8Array(32).fill(0x37);
  const sigHex = await loaded!.sign(owner, message);
  const pubHex = await loaded!.getPublicKey(owner);

  const verifyKey = await crypto.subtle.importKey(
    "raw",
    hexToBytes(pubHex),
    { name: "Ed25519" },
    false,
    ["verify"],
  );
  const ok = await crypto.subtle.verify(
    "Ed25519",
    verifyKey,
    hexToBytes(sigHex),
    message,
  );
  expect(ok).toBe(true);
});

test("loadOrCreate() is idempotent for the same recordKey", async () => {
  const first = await linera.signer.WebCryptoEd25519.loadOrCreate(RECORD_KEY);
  const second = await linera.signer.WebCryptoEd25519.loadOrCreate(RECORD_KEY);
  expect(second.address()).toBe(first.address());
});

test("sign() returns a 64-byte hex string that verifies against the public key", async () => {
  const signer = await linera.signer.WebCryptoEd25519.create(RECORD_KEY);
  const owner = signer.address();
  const message = new Uint8Array(32).fill(0x42);
  const sigHex = await signer.sign(owner, message);
  expect(sigHex).toMatch(/^0x[0-9a-f]{128}$/);

  const pubHex = await signer.getPublicKey(owner);
  expect(pubHex).toMatch(/^0x[0-9a-f]{64}$/);

  const pubBytes = hexToBytes(pubHex);
  const sigBytes = hexToBytes(sigHex);
  const verifyKey = await crypto.subtle.importKey(
    "raw",
    pubBytes,
    { name: "Ed25519" },
    false,
    ["verify"],
  );
  const ok = await crypto.subtle.verify(
    "Ed25519",
    verifyKey,
    sigBytes,
    message,
  );
  expect(ok).toBe(true);
});

test("sign() rejects requests for a mismatched owner", async () => {
  const signer = await linera.signer.WebCryptoEd25519.create(RECORD_KEY);
  const wrongOwner = "0x" + "0".repeat(64);
  await expect(signer.sign(wrongOwner, new Uint8Array(32))).rejects.toThrow(
    /Invalid owner address/,
  );
});

test("getPublicKey() rejects requests for a mismatched owner", async () => {
  const signer = await linera.signer.WebCryptoEd25519.create(RECORD_KEY);
  const wrongOwner = "0x" + "0".repeat(64);
  await expect(signer.getPublicKey(wrongOwner)).rejects.toThrow(
    /Invalid owner address/,
  );
});

test("generate() alone does not persist — load() still returns null", async () => {
  const signer = await linera.signer.WebCryptoEd25519.generate();
  expect(signer.address()).toMatch(/^0x[0-9a-f]{64}$/);
  const loaded = await linera.signer.WebCryptoEd25519.load(RECORD_KEY);
  expect(loaded).toBeNull();
});

test("generate() then persist() makes the keypair loadable", async () => {
  const generated = await linera.signer.WebCryptoEd25519.generate();
  await generated.persist(RECORD_KEY);
  const loaded = await linera.signer.WebCryptoEd25519.load(RECORD_KEY);
  expect(loaded).not.toBeNull();
  expect(loaded!.address()).toBe(generated.address());
});

async function wipeDb(): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const req = indexedDB.deleteDatabase(DB_NAME);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
    req.onblocked = () => resolve();
  });
}

function hexToBytes(hex: string): Uint8Array {
  const trimmed = hex.startsWith("0x") ? hex.slice(2) : hex;
  const out = new Uint8Array(trimmed.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(trimmed.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}
