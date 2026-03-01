# linera-bridge

An EVM light client that tracks Linera validator committees and verifies `ConfirmedBlockCertificate`s on-chain. The `LightClient` contract is an admin contract — it manages committee transitions and exposes block verification as a service that other contracts can call, but does not store block data itself.

## Architecture

The bridge has three layers:

1. **Code generation (build-time)**: Rust types from `linera-base`, `linera-chain`, and `linera-execution` are traced via `serde-reflection` to produce a YAML schema. The `build.rs` feeds this schema into `serde-generate` to emit `BridgeTypes.sol` — a Solidity library with BCS serializers and deserializers for every type in the `ConfirmedBlockCertificate` type graph.

2. **Solidity contracts** (`src/solidity/`):
   - **LightClient.sol** — an admin contract that tracks committee epochs and verifies certificate signatures. It exposes `verifyBlock(bytes)` for other contracts to call, returning the deserialized `Block` on success. It does not store blocks.
   - **Microchain.sol** — an abstract contract that tracks blocks for a single Linera microchain. It delegates certificate verification to a `LightClient` instance and enforces chain ID matching and sequential block heights. Concrete subcontracts implement `_onBlock()` to define application-specific logic (e.g., tracking token transfers).

3. **Rust ABI layer** (`src/light_client.rs`, `src/microchain.rs`): Typed bindings for calling the Solidity contracts from Rust. Uses `alloy-sol-types`'s `sol!` macro to generate `*Call` structs (e.g., `addCommitteeCall`, `addBlockCall`) with `abi_encode()` / `abi_decode_returns()` methods. This lets Rust code construct contract calls without hand-encoding ABI bytes.

```
                  ┌──────────────┐
                  │ LightClient  │  tracks committees, verifies certificates
                  └──────┬───────┘
                         │ verifyBlock()
          ┌──────────────┼──────────────┐
          │              │              │
   ┌──────▼──────┐ ┌─────▼──────┐ ┌────▼───────┐
   │ Fungible-   │ │ Microchain │ │ Microchain │  one per chain,
   │  Bridge     │ │  (NFT)     │ │  (DEX)     │  enforces chain_id + height
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
BridgeTypes.sol (src/solidity/BridgeTypes.sol)
```

Application-specific types follow the same pipeline. For example, `FungibleOperation` from the fungible token application:

```
Rust types (linera-sdk::abis::fungible)
    │
    ▼  serde-reflection (tests/format_fungible.rs)
YAML snapshot (tests/snapshots/format_fungible__format_fungible.yaml.snap)
    │
    ▼  serde-generate via build.rs (shared types declared as external_definitions)
FungibleTypes.sol (src/solidity/FungibleTypes.sol)
    imports BridgeTypes.sol for shared types
```

`FungibleTypes.sol` only contains types and deserializers unique to the fungible application (`FungibleOperation` and its variants). Shared types like `Account`, `AccountOwner`, and `Amount` are reused from `BridgeTypes.sol` via import — `build.rs` passes them as `external_definitions` to `serde-generate`, which emits qualified `BridgeTypes.` references and `import` statements instead of duplicate definitions.

All snapshots are checked in and tested via `insta`. This means the generated Solidity stays in sync with the Rust types — if a struct field is added or an enum variant reordered, the snapshot test fails and the developer must update it explicitly.

## Certificate verification

`ConfirmedBlockCertificate` is a BCS-encoded blob containing a `Block`, a `Round`, and a list of `(PublicKey, Signature)` pairs. The contract verifies it in five steps:

1. **Partial deserialization**: The `Block` is deserialized first to determine its byte boundary in the BCS stream. This allows computing the block hash from raw bytes without re-serializing.

2. **Block hash**: `value_hash = keccak256("Block::" || BCS(block))`. The `"Block::"` prefix matches Linera's `CryptoHash::new` convention, which prepends `"TypeName::"` before hashing. `ConfirmedBlock` is `#[serde(transparent)]` over `Block`, so the type name is `"Block"`.

3. **Round and signatures**: Deserialized from the remaining bytes after the block.

4. **VoteValue hash**: Validators sign `CryptoHash::new(&VoteValue(value_hash, round, CertificateKind::Confirmed))`, which expands to `keccak256("VoteValue::" || BCS(VoteValue))`. The contract reconstructs this using the generated `bcs_serialize_VoteValue`.

5. **Signature verification via `ecrecover`**: Each signature's `(r, s)` values are extracted and passed to `ecrecover`. Since Linera signatures don't include the recovery ID (`v`), the contract tries both `v=27` and `v=28`. Recovered addresses are checked against the block's epoch committee's weight mapping. Each signer is counted at most once (duplicate signers are rejected). The total weight must meet the quorum threshold.

### Why `ecrecover` works

Linera validators use secp256k1 keys (the same curve as Ethereum). The contract stores committee members as Ethereum addresses, derived off-chain as `keccak256(uncompressed_pubkey[1:])[12:]`. This lets us use Solidity's native `ecrecover` precompile rather than implementing signature verification from scratch.

## Contract API

### LightClient

#### `verifyBlock(bytes calldata data) → (BridgeTypes.Block, bytes32)`

Verifies a BCS-encoded `ConfirmedBlockCertificate` against the committee for the block's declared epoch and returns the deserialized `Block` and the `signedHash` (for duplicate detection). This is a `view` function — it does not modify state. Other contracts (like `Microchain`) call this to get verified block data.

#### `addCommittee(bytes calldata data, bytes calldata committeeBlob, bytes[] calldata validators)`

Advances the committee to the next epoch. This is the only state-modifying operation (besides construction).

1. Verify the certificate against the block's epoch committee (signature check).
2. Require the block is from the admin chain and the current epoch.
3. Scan the block's transactions for an `AdminOperation::CreateCommittee { epoch, blob_hash }`.
4. Verify that `keccak256("BlobContent::" || BCS(BlobContent { Committee, committeeBlob }))` matches `blob_hash`. This proves the caller's `committeeBlob` is the one referenced by the certified block.
5. Parse the committee blob on-chain. For each validator, verify the caller's uncompressed public key matches the blob's compressed key (x-coordinate, y-parity, and secp256k1 curve membership), then derive the Ethereum address via `keccak256(uncompressed_key)`.
6. Store derived addresses and blob-extracted weights as the committee for the new epoch.

The `committeeBlob` is the BCS-serialized `Committee` from Linera. The `validators` parameter is an array of 64-byte uncompressed secp256k1 public keys (without the `0x04` prefix). The caller must provide these separately because the blob only contains compressed keys (33 bytes: x-coordinate + y-parity prefix), and Ethereum addresses are derived from the uncompressed form (`keccak256(x || y)[12:]`). Decompressing a key on-chain would require computing a modular square root on the secp256k1 field — expensive in the EVM. Instead, the caller provides the uncompressed keys and the contract verifies them against the blob's compressed keys: it checks that the x-coordinate matches, the y-parity matches, and the point satisfies y² ≡ x³ + 7 (mod p). This curve membership check uses Solidity's `mulmod`/`addmod` builtins and is cheap. Addresses and weights are extracted from the blob rather than caller-provided, preventing substitution attacks. The authenticity chain is: validator signatures → certified block → `CreateCommittee` operation → `blob_hash` → `committeeBlob` → parsed validators/weights.

The constructor takes `(address[], uint64[], bytes32, uint32)` — the genesis committee's validator addresses, weights, the admin chain ID, and the initial epoch. The deployer is trusted at genesis (no blob exists).

### Microchain (abstract)

#### `constructor(address _lightClient, bytes32 _chainId, uint64 _latestHeight)`

Binds the contract to a specific `LightClient` instance, a Linera chain ID (a 32-byte `CryptoHash`), and an initial block height.

#### `addBlock(bytes calldata data)`

Verifies a certificate via `lightClient.verifyBlock(data)`, then enforces:
- **Chain ID match**: the block's `header.chain_id` must equal this contract's `chainId`.
- **Sequential heights**: the block's height must equal `nextExpectedHeight`.

On success, calls the virtual `_onBlock(BridgeTypes.Block)` hook. Subcontracts override this to extract and store application-specific data from the verified block.

### FungibleBridge (concrete Microchain)

A `Microchain` subcontract that bridges ERC-20 tokens from Linera to Ethereum. When a fungible `Credit` message targeting an Ethereum address (`Address20`) is received, the contract transfers tokens from its own balance to the recipient.

#### `constructor(address _lightClient, bytes32 _chainId, uint64 _latestHeight, bytes32 _applicationId, address _token)`

Binds to a specific `LightClient`, chain, initial block height, Linera application ID, and ERC-20 token contract. Only messages targeting this `applicationId` are processed; all others are silently skipped.

#### `_onBlock(BridgeTypes.Block)`

Scans the block's `ReceiveMessages` transactions for `Message::User` entries matching `applicationId`. For each match, the opaque `bytes` payload is deserialized as a `FungibleTypes.Message`. Only `Credit` messages with an `Address20` target (Ethereum address) trigger an ERC-20 `transfer` from the bridge's balance to the target.

## Rust API

The crate exposes typed bindings for each contract, plus the generated Solidity source as a constant:

```rust
use linera_bridge::evm::{light_client, microchain, BRIDGE_TYPES_SOURCE};

// Encode a contract call (validators are 64-byte uncompressed public keys)
let call = light_client::addCommitteeCall {
    data: bcs_bytes.into(),
    committeeBlob: committee_bytes.into(),
    validators: vec![uncompressed_key.into()],
};
let calldata: Vec<u8> = call.abi_encode();

// Available call types:
// light_client: addCommitteeCall, verifyBlockCall, currentEpochCall
// microchain:   addBlockCall, nextExpectedHeightCall, lightClientCall, chainIdCall

// Solidity sources (for compilation or deployment tooling):
// BRIDGE_TYPES_SOURCE, FUNGIBLE_TYPES_SOURCE, FUNGIBLE_BRIDGE_SOURCE
// light_client::SOURCE, microchain::SOURCE
```

## Committee management

Committees are stored per-epoch and must advance monotonically (epoch N can only be followed by epoch N+1).

### Initialization

The constructor takes `(address[], uint64[], bytes32, uint32)` — the genesis committee's validator Ethereum addresses, their voting weights, the admin chain ID, and the initial epoch. The committee is stored at the given epoch. Only blocks from the admin chain can drive committee transitions via `addCommittee`.

### Quorum threshold

`quorumThreshold = 2 * totalWeight / 3 + 1`, matching Linera's BFT quorum requirement of `N - f` where `f = ⌊(N-1)/3⌋`.

## Key design decisions

- **Keccak256 everywhere**: Linera's `CryptoHash` uses Keccak256, which is also Solidity's native hash function. This is not a coincidence — it makes on-chain verification cheap.

- **Type name prefixes in hashes**: Linera's `CryptoHash::new<T>(value)` computes `keccak256("TypeName::" || BCS(value))`. The contract must reproduce these prefixes exactly (`"Block::"`, `"VoteValue::"`, `"BlobContent::"`). This is a domain separation mechanism that prevents cross-type hash collisions.

- **Partial deserialization for the block hash**: The contract deserializes the `Block` to get its fields, but computes the hash from the raw BCS bytes (not by re-serializing). This avoids potential round-trip issues and is more gas-efficient.

- **Generated BCS code via serde-generate**: Rather than hand-writing Solidity deserializers, we auto-generate them from the same Rust type definitions that produce the data. This eliminates an entire class of serialization bugs.

- **Partial committee blob parsing on-chain**: The contract parses just enough of the BCS-serialized committee blob to extract compressed public keys and voting weights. It skips fields it doesn't need (network addresses, account public keys, resource control policy). The caller provides uncompressed public keys, which the contract verifies against the blob's compressed keys (x-coordinate match, y-parity match, and secp256k1 curve membership via y² ≡ x³ + 7). This eliminates the need to trust caller-provided addresses and weights, while avoiding the cost of on-chain modular square root for full decompression.

- **Separation of concerns between LightClient and Microchain**: The `LightClient` is a singleton that only manages committees and certificate verification. It has no knowledge of individual chains or their blocks. Each `Microchain` instance tracks a single chain's block sequence and delegates verification to the `LightClient`. This means one `LightClient` deployment can serve any number of `Microchain` contracts, each following a different Linera microchain.

- **Application-specific type generation with shared type reuse**: `FungibleTypes.sol` is generated from a separate serde-reflection snapshot of `FungibleOperation`. Since `FungibleOperation` references types already in `BridgeTypes.sol` (e.g., `Account`, `AccountOwner`, `Amount`), `build.rs` declares them as `external_definitions` so `serde-generate` emits qualified `BridgeTypes.` references and import statements instead of duplicate definitions. This ensures type compatibility — a `BridgeTypes.Account` from block deserialization can be directly compared with an `Account` from a deserialized `FungibleOperation`.

- **No `previous_block_hash` chain-linking in Microchain**: The `Microchain` contract enforces chain ID and sequential heights but does not verify `previous_block_hash` to link blocks into a hash chain. This is safe because a `ConfirmedBlockCertificate` implies BFT-finalized canonicality — a quorum of validators signed this specific block at this height, so no conflicting block can exist for the same chain and height. The contract relies on this protocol-layer guarantee rather than redundantly re-checking hash linking. If the finality semantics of `ConfirmedBlockCertificate` ever change (e.g., to allow rollbacks or forks), a `previous_block_hash` check should be added.

## Testing

Tests use [revm](https://github.com/bluealloy/revm) (Rust EVM) to execute the Solidity contracts in-process, with `solc` for compilation. No external EVM node is required. The test suite covers:

- Block verification via `verifyBlock` (valid and invalid signatures)
- Committee transitions via `addCommittee` with `CreateCommittee` verification
- Blob hash mismatch rejection
- Non-sequential epoch rejection
- Substituted public key rejection (key doesn't match blob)
- Off-curve public key rejection (fake y-coordinate)
- Non-admin chain rejection
- Wrong block epoch rejection (stale epoch block replayed for transition)
- Duplicate signer rejection (same validator signature repeated)
- Epoch-bound committee verification (block verified against its declared epoch)
- Microchain block tracking with chain ID enforcement
- Microchain rejection of wrong chain ID and non-sequential heights
- Microchain rejection of duplicate block submissions
- FungibleBridge ERC-20 transfer on Credit message
- FungibleBridge accumulated transfers across blocks
- FungibleBridge skips non-EVM targets (Address32)
- FungibleBridge ignores messages for other application IDs

### Prerequisites

- `solc` (Solidity compiler) must be on `$PATH`
- Run tests: `cargo test -p linera-bridge`
