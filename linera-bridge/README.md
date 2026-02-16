# linera-bridge

An EVM light client that tracks Linera validator committees and verifies `ConfirmedBlockCertificate`s on-chain. The `LightClient` contract is an admin contract — it manages committee transitions and exposes block verification as a service that other contracts can call, but does not store block data itself.

## Architecture

The bridge has two main layers:

1. **Code generation (build-time)**: Rust types from `linera-base`, `linera-chain`, and `linera-execution` are traced via `serde-reflection` to produce a YAML schema. The `build.rs` feeds this schema into `serde-generate` to emit `BridgeTypes.sol` — a Solidity library with BCS serializers and deserializers for every type in the `ConfirmedBlockCertificate` type graph.

2. **LightClient.sol** — an admin contract that tracks committee epochs and verifies certificate signatures. It exposes `verifyBlock(bytes)` for other contracts to call, returning the deserialized `Block` on success. It does not store blocks.

3. **Microchain.sol** — an abstract contract that tracks blocks for a single Linera microchain. It delegates certificate verification to a `LightClient` instance and enforces chain ID matching and sequential block heights. Concrete subcontracts implement `_onBlock()` to define application-specific logic (e.g., tracking token transfers).

```
                  ┌──────────────┐
                  │ LightClient  │  tracks committees, verifies certificates
                  └──────┬───────┘
                         │ verifyBlock()
          ┌──────────────┼──────────────┐
          │              │              │
   ┌──────▼──────┐ ┌─────▼──────┐ ┌────▼───────┐
   │ Microchain  │ │ Microchain │ │ Microchain │  one per chain,
   │  (USDC)     │ │  (NFT)     │ │  (DEX)     │  enforces chain_id + height
   └─────────────┘ └────────────┘ └────────────┘
```

### Type tracing pipeline

```
Rust types (linera-base, linera-chain, linera-execution)
    │
    ▼  serde-reflection (tests/format.rs)
YAML snapshot (tests/snapshots/format__format.yaml.snap)
    │
    ▼  serde-generate via build.rs
BridgeTypes.sol (src/BridgeTypes.sol)
```

The snapshot is checked in and tested via `insta`. This means the generated Solidity stays in sync with the Rust types — if a struct field is added or an enum variant reordered, the snapshot test fails and the developer must update it explicitly.

## Certificate verification

`ConfirmedBlockCertificate` is a BCS-encoded blob containing a `Block`, a `Round`, and a list of `(PublicKey, Signature)` pairs. The contract verifies it in five steps:

1. **Partial deserialization**: The `Block` is deserialized first to determine its byte boundary in the BCS stream. This allows computing the block hash from raw bytes without re-serializing.

2. **Block hash**: `value_hash = keccak256("Block::" || BCS(block))`. The `"Block::"` prefix matches Linera's `CryptoHash::new` convention, which prepends `"TypeName::"` before hashing. `ConfirmedBlock` is `#[serde(transparent)]` over `Block`, so the type name is `"Block"`.

3. **Round and signatures**: Deserialized from the remaining bytes after the block.

4. **VoteValue hash**: Validators sign `CryptoHash::new(&VoteValue(value_hash, round, CertificateKind::Confirmed))`, which expands to `keccak256("VoteValue::" || BCS(VoteValue))`. The contract reconstructs this using the generated `bcs_serialize_VoteValue`.

5. **Signature verification via `ecrecover`**: Each signature's `(r, s)` values are extracted and passed to `ecrecover`. Since Linera signatures don't include the recovery ID (`v`), the contract tries both `v=27` and `v=28`. Recovered addresses are checked against the current committee's weight mapping, and the total weight must meet the quorum threshold.

### Why `ecrecover` works

Linera validators use secp256k1 keys (the same curve as Ethereum). The contract stores committee members as Ethereum addresses, derived off-chain as `keccak256(uncompressed_pubkey[1:])[12:]`. This lets us use Solidity's native `ecrecover` precompile rather than implementing signature verification from scratch.

## Contract API

### LightClient

#### `verifyBlock(bytes calldata data) → BridgeTypes.Block`

Verifies a BCS-encoded `ConfirmedBlockCertificate` against the current committee and returns the deserialized `Block`. This is a `view` function — it does not modify state. Other contracts (like `Microchain`) call this to get verified block data.

#### `addCommittee(bytes calldata data, bytes calldata committeeBlob, address[] calldata validators, uint64[] calldata weights)`

Advances the committee to the next epoch. This is the only state-modifying operation (besides construction).

1. Verify the certificate against the current committee (signature check).
2. Scan the block's transactions for an `AdminOperation::CreateCommittee { epoch, blob_hash }`.
3. Verify that `keccak256("BlobContent::" || BCS(BlobContent { Committee, committeeBlob }))` matches `blob_hash`. This proves the caller's `committeeBlob` is the one referenced by the certified block.
4. Store `newValidators` and `newWeights` as the committee for the new epoch.

The `committeeBlob` is the BCS-serialized `Committee` from Linera. The contract does not deserialize it — Solidity cannot natively decompress secp256k1 public keys to derive Ethereum addresses, so the caller provides the parsed committee data (addresses + weights) alongside the blob for hash verification. The authenticity chain is: validator signatures → certified block → `CreateCommittee` operation → `blob_hash` → `committeeBlob`.

### Microchain (abstract)

#### `constructor(address _lightClient, bytes32 _chainId)`

Binds the contract to a specific `LightClient` instance and a Linera chain ID (a 32-byte `CryptoHash`).

#### `addBlock(bytes calldata data)`

Verifies a certificate via `lightClient.verifyBlock(data)`, then enforces:
- **Chain ID match**: the block's `header.chain_id` must equal this contract's `chainId`.
- **Sequential heights**: the block's height must be exactly `latestHeight + 1`.

On success, calls the virtual `_onBlock(BridgeTypes.Block)` hook. Subcontracts override this to extract and store application-specific data from the verified block.

## Committee management

Committees are stored per-epoch and must advance monotonically (epoch N can only be followed by epoch N+1).

### Initialization

The constructor takes `(address[], uint64[])` — the genesis committee's validator Ethereum addresses and their voting weights. This is stored as epoch 0.

### Quorum threshold

`quorumThreshold = 2 * totalWeight / 3 + 1`, matching Linera's BFT quorum requirement of `N - f` where `f = ⌊(N-1)/3⌋`.

## Key design decisions

- **Keccak256 everywhere**: Linera's `CryptoHash` uses Keccak256, which is also Solidity's native hash function. This is not a coincidence — it makes on-chain verification cheap.

- **Type name prefixes in hashes**: Linera's `CryptoHash::new<T>(value)` computes `keccak256("TypeName::" || BCS(value))`. The contract must reproduce these prefixes exactly (`"Block::"`, `"VoteValue::"`, `"BlobContent::"`). This is a domain separation mechanism that prevents cross-type hash collisions.

- **Partial deserialization for the block hash**: The contract deserializes the `Block` to get its fields, but computes the hash from the raw BCS bytes (not by re-serializing). This avoids potential round-trip issues and is more gas-efficient.

- **Generated BCS code via serde-generate**: Rather than hand-writing Solidity deserializers, we auto-generate them from the same Rust type definitions that produce the data. This eliminates an entire class of serialization bugs.

- **No committee blob deserialization on-chain**: The `Committee` type has complex nested structures (`BTreeMap`, `ResourceControlPolicy`, custom serde impls). Deserializing it in Solidity would be extremely expensive and require secp256k1 point decompression. Instead, the caller provides pre-parsed data and the contract only verifies the blob hash.

- **Separation of concerns between LightClient and Microchain**: The `LightClient` is a singleton that only manages committees and certificate verification. It has no knowledge of individual chains or their blocks. Each `Microchain` instance tracks a single chain's block sequence and delegates verification to the `LightClient`. This means one `LightClient` deployment can serve any number of `Microchain` contracts, each following a different Linera microchain.

## Testing

Tests use [revm](https://github.com/bluealloy/revm) (Rust EVM) to execute the Solidity contracts in-process, with `solc` for compilation. No external EVM node is required. The test suite covers:

- Full `ConfirmedBlockCertificate` deserialization and field extraction
- Block verification via `verifyBlock` (valid and invalid signatures)
- Committee transitions via `addCommittee` with `CreateCommittee` verification
- Blob hash mismatch rejection
- Non-sequential epoch rejection
- Microchain block tracking with chain ID enforcement
- Microchain rejection of wrong chain ID and non-sequential heights

### Prerequisites

- `solc` (Solidity compiler) must be on `$PATH`
- Run tests: `cargo test -p linera-bridge`
