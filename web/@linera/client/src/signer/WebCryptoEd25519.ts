import type { Signer } from "./Signer.d.ts";
import { accountOwnerFromEd25519PublicKey } from "../wasm/index.js";

type StoredRecord = {
  // Canonical owner address: "0x" + 64 lowercase hex chars. Lowercase is invariant —
  // produced by Rust's `Display for AccountOwner` which uses `hex::encode`. Callers
  // comparing against it must `.toLowerCase()` their side only.
  owner: string;
  publicKey: Uint8Array;
  privateKey: CryptoKey;
};

/**
 * A {@link Signer} backed by a non-extractable Ed25519 key from the Web Crypto API.
 *
 * The private key never leaves the browser's crypto subsystem: it is generated with
 * `extractable: false` and persisted as a `CryptoKey` handle in IndexedDB. An in-page
 * attacker (XSS, malicious dependency) can request signatures while the tab is open but
 * cannot copy the key off the device.
 *
 * Owner addresses are `AccountOwner::Address32(Keccak256(BCS(public_key)))`, derived in
 * Rust via {@link accountOwnerFromEd25519PublicKey}.
 *
 * **Requires** `linera.initialize()` to have been called before `generate()` or
 * `loadOrCreate()` — both call the wasm owner-derivation helper. `load()` and
 * `delete()` are wasm-free and may be invoked before initialization.
 */
export default class WebCryptoEd25519 implements Signer {
  private constructor(private readonly record: StoredRecord) {
    // Shallow freeze guards against accidental mutation of the cached owner / key
    // references by the rest of the codebase. The nested `publicKey` Uint8Array's
    // bytes remain mutable per JS semantics; treat them as read-only.
    Object.freeze(this.record);
  }

  /**
   * Generates a fresh non-extractable Ed25519 keypair in memory.
   *
   * The returned signer is NOT yet persisted to IndexedDB; call {@link persist} to
   * commit it. Use this when the caller needs to take an extra step (e.g. registering
   * the autosigner address on-chain) before the keypair becomes durable.
   */
  static async generate(): Promise<WebCryptoEd25519> {
    const pair = await crypto.subtle.generateKey(
      { name: "Ed25519" },
      false,
      ["sign", "verify"],
    );
    if (!("privateKey" in pair)) {
      throw new Error(
        "crypto.subtle.generateKey did not return a CryptoKeyPair for Ed25519",
      );
    }
    const publicKey = new Uint8Array(
      await crypto.subtle.exportKey("raw", pair.publicKey),
    );
    const owner = accountOwnerFromEd25519PublicKey(publicKey);
    return new WebCryptoEd25519({
      owner,
      publicKey,
      privateKey: pair.privateKey,
    });
  }

  /**
   * Commits this signer's keypair to IndexedDB under `recordKey`. Subsequent calls to
   * {@link load} with the same `recordKey` will return a signer pointing at the same
   * key material. Overwrites any existing record.
   */
  async persist(recordKey: string): Promise<void> {
    const db = await openDb();
    try {
      await txWrite(db, (store) => store.put(this.record, recordKey));
    } finally {
      db.close();
    }
  }

  /**
   * Loads an existing keypair stored under `recordKey`. Returns `null` if no record exists.
   */
  static async load(recordKey: string): Promise<WebCryptoEd25519 | null> {
    const db = await openDb();
    try {
      const stored = await txRead<StoredRecord | undefined>(db, (store) =>
        store.get(recordKey) as IDBRequest<StoredRecord | undefined>,
      );
      if (!stored) return null;
      return new WebCryptoEd25519(stored);
    } finally {
      db.close();
    }
  }

  /** Convenience: load existing, or generate + persist a fresh keypair. */
  static async loadOrCreate(recordKey: string): Promise<WebCryptoEd25519> {
    const existing = await WebCryptoEd25519.load(recordKey);
    if (existing) return existing;
    const signer = await WebCryptoEd25519.generate();
    await signer.persist(recordKey);
    return signer;
  }

  /**
   * Removes the keypair record stored under `recordKey`. No-op if no record exists.
   * Use to revoke a session locally (e.g. after `forgoDelegation` succeeds on-chain
   * or during a migration to a different key shape).
   */
  static async delete(recordKey: string): Promise<void> {
    const db = await openDb();
    try {
      await txWrite(db, (store) => store.delete(recordKey));
    } finally {
      db.close();
    }
  }

  /** The `Address32` account owner address, as `0x` + 64 hex chars (lowercase). */
  address(): string {
    return this.record.owner;
  }

  async sign(owner: string, value: Uint8Array): Promise<string> {
    this.assertOwner(owner);
    console.debug(
      "[Linera Signer] WebCryptoEd25519.sign via Web Crypto API",
      { owner, valueBytes: value.length },
    );
    // Runtime defense: `crypto.subtle.sign` rejects SharedArrayBuffer-backed views in
    // some browsers. Copy into a fresh ArrayBuffer so the call works regardless of how
    // the caller obtained `value`.
    const buf = new Uint8Array(value).buffer as ArrayBuffer;
    const sig = await crypto.subtle.sign("Ed25519", this.record.privateKey, buf);
    return "0x" + bytesToHex(new Uint8Array(sig));
  }

  async getPublicKey(owner: string): Promise<string> {
    this.assertOwner(owner);
    return "0x" + bytesToHex(this.record.publicKey);
  }

  async containsKey(owner: string): Promise<boolean> {
    // record.owner is canonical lowercase; only normalize the caller side.
    return owner.toLowerCase() === this.record.owner;
  }

  private assertOwner(owner: string): void {
    // record.owner is canonical lowercase; only normalize the caller side.
    if (owner.toLowerCase() !== this.record.owner) {
      throw new Error("Invalid owner address");
    }
  }
}

const DB_NAME = "linera-signer";
const STORE_NAME = "keys";
const DB_VERSION = 1;

function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
    };
    // Fires when another open connection on a lower version blocks this upgrade.
    // Surface as an error rather than hanging indefinitely.
    req.onblocked = () =>
      reject(
        new Error(
          `IndexedDB upgrade blocked: another open connection is holding ` +
            `"${DB_NAME}" at an older version. Close other tabs of this site and retry.`,
        ),
      );
    req.onerror = () => reject(req.error);
    req.onsuccess = () => {
      const db = req.result;
      // If another tab triggers a version upgrade, close this connection so the
      // upgrade can proceed rather than blocking it indefinitely.
      db.onversionchange = () => db.close();
      resolve(db);
    };
  });
}

// Resolves on `req.onsuccess`. Reads have no durability barrier — the value is
// already in memory by the time `onsuccess` fires, so waiting for
// `transaction.oncomplete` would only add latency. Contrast with `txWrite` below,
// which must wait for the commit.
function txRead<T>(
  db: IDBDatabase,
  fn: (store: IDBObjectStore) => IDBRequest<T>,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, "readonly");
    const store = transaction.objectStore(STORE_NAME);
    const req = fn(store);
    req.onerror = () => reject(req.error);
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () =>
      reject(transaction.error ?? new Error("IndexedDB read transaction aborted"));
    req.onsuccess = () => resolve(req.result);
  });
}

// Resolves on `transaction.oncomplete` (not `req.onsuccess`) so the caller knows the
// write has reached the IndexedDB log, not just the request's buffer. Without this,
// `persist()` could return before the keypair is durable, and a tab close in the
// intervening window would silently lose the autosigner association.
function txWrite<T>(
  db: IDBDatabase,
  fn: (store: IDBObjectStore) => IDBRequest<T>,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, "readwrite");
    const store = transaction.objectStore(STORE_NAME);
    const req = fn(store);
    let result: T;
    req.onerror = () => reject(req.error);
    req.onsuccess = () => {
      result = req.result;
    };
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () =>
      reject(transaction.error ?? new Error("IndexedDB write transaction aborted"));
    transaction.oncomplete = () => resolve(result);
  });
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}
