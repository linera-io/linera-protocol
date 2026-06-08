# Block hash must commit to the `BlockHeader`, not the whole `Block`

Date: 2026-06-08
Branch target: `main` (no backward compatibility required)

## Problem

`Block` carries a `BlockHeader` that holds a cryptographic hash of every field in
the `BlockBody` (`transactions_hash`, `messages_hash`, `events_hash`, …). The
intent was a light-client / relay path: ship a `BlockHeader` plus a single
`BlockBody` field and prove that field was part of the block, without relaying the
whole block.

That design was never completed. The hash that validators sign in a
`ConfirmedBlockCertificate` is computed over the **entire serialized block**, not
over the `BlockHeader`. The header's per-field hashes never enter the signed
preimage, so a holder of the header alone cannot reproduce the signed hash, and the
inclusion proof never closes.

## Root cause (verified)

The signed value is the `Hashed<Block>` hash:

- `linera-chain/src/data_types/mod.rs:567` — `Vote::new` signs
  `VoteValue(value.hash(), round, KIND)`.
- `linera-chain/src/certificate/mod.rs:177` — `ConfirmedBlock::hash()` =
  `self.inner().hash()` = the `Hashed<Block>` hash.
- `linera-base/src/hashed.rs:29` — `Hashed::new` sets `hash = CryptoHash::new(&block)`.

`CryptoHash::new(&block)` hashes the BCS bytes of the whole block:

- `linera-base/src/crypto/mod.rs:387-392` — `Hashable::write` =
  `"Block::" + bcs::serialize_into(&block)`.
- `linera-chain/src/block.rs:245-262` — `Serialize for Block` emits
  `SerializedHeader` (the 7 scalar header fields) **plus the full `body`**.
- `linera-chain/src/block.rs:647-657` — `SerializedHeader` contains only the 7
  scalar fields; the 8 content hashes are **dropped** from the wire format and only
  recomputed at deserialize (`block.rs:264-310`) for in-memory access.

So the signed preimage is
`"Block::" + bcs(7 scalar header fields) + bcs(entire BlockBody)`. The header's
content hashes are not part of it. Grep confirms nothing ever hashes a
`BlockHeader` alone, and `BlockHeader` does not implement `BcsHashable`.

Underlying coupling: `Serialize for Block` does double duty — it is both the
storage/wire format and (via `Hashable::write`) the hash preimage. To commit to the
header only while still storing the body, the two must be decoupled.

## Goal / scope

Approved scope (all three):

1. **Commitment** — the consensus block hash becomes `CryptoHash::new(&BlockHeader)`.
2. **Proof API** — given a `BlockHeader` + one `BlockBody` field, verify the field
   belongs to the block.
3. **Integrity check** — on block deserialize, verify the body matches the header's
   content hashes; reject mismatches.

## Design

### Commitment model

Block hash ≡ `CryptoHash::new(&BlockHeader)` = `Keccak256("BlockHeader::" + bcs(BlockHeader))`.

The header's 8 content-hash fields already commit to every body field:

| header field | body field | helper |
|---|---|---|
| `transactions_hash` | `transactions` | `hash_vec` |
| `messages_hash` | `messages` | `hash_vec_vec` |
| `previous_message_blocks_hash` | `previous_message_blocks` | `PreviousMessageBlocksMap` |
| `previous_event_blocks_hash` | `previous_event_blocks` | `PreviousEventBlocksMap` |
| `oracle_responses_hash` | `oracle_responses` | `hash_vec_vec` |
| `events_hash` | `events` | `hash_vec_vec` |
| `blobs_hash` | `blobs` | `hash_vec_vec` |
| `operation_results_hash` | `operation_results` | `hash_vec` |

Hashing the header therefore transitively commits to the whole block. Trust chain:
validator signature → header hash → field hashes → body fields.

### `linera-base/src/hashed.rs`

- Add `Hashed::with_hash(value, hash)` — a constructor that stores a precomputed
  hash without requiring `T: BcsHashable`. Lets `Hashed<Block>` hold the header hash.

### `linera-chain/src/block.rs`

- `impl BcsHashable<'_> for BlockHeader {}`.
- **Remove** `impl BcsHashable<'_> for Block {}` (block.rs:631) so the body can never
  re-enter a hash preimage.
- `Block::hash(&self) -> CryptoHash { CryptoHash::new(&self.header) }` — the single
  canonical block hash.
- `Serialize for Block`: emit the **full** `BlockHeader` (all 15 fields, including
  the content hashes) + `body`. Delete the `SerializedHeader` truncation.
- `Deserialize for Block`: read header + body; recompute the 8 field-hashes from the
  body; **verify** each equals the header's; return `de::Error` on mismatch. (The
  current deserialize already recomputes all 8 hashes, so this adds comparisons, not
  new hashing work.)
- `ConfirmedBlock` / `ValidatedBlock` wrappers:
  - `::new(block)` → `Self(Hashed::with_hash(block, block.hash()))`.
  - Replace the `#[serde(transparent)]` derive with an explicit `Deserialize` that
    builds `Hashed::with_hash(block, block.hash())`. Serialize keeps delegating to
    the inner `Block`. Required because the generic `Deserialize for Hashed<T>`
    requires `T: BcsHashable`, which `Block` no longer is.

### `linera-core/src/client/mod.rs:2198`

- `CryptoHash::new(executed_block)` → `executed_block.hash()` — the one site that
  hashes a raw `Block`.

### Integrity check (scope 3)

Lives in `Deserialize for Block`: any deserialized block is guaranteed
header↔body consistent or it is rejected with a captured, asserted error message.

### Proof / verification API (scope 2)

Model "one body field + its identity" as an enum and verify it against the header,
reusing the existing `hashing` helpers so the hash logic has a single source.

```rust
pub enum BlockBodyField {
    Transactions(Vec<Transaction>),
    Messages(Vec<Vec<OutgoingMessage>>),
    PreviousMessageBlocks(BTreeMap<ChainId, (CryptoHash, BlockHeight)>),
    PreviousEventBlocks(BTreeMap<StreamId, (CryptoHash, BlockHeight)>),
    OracleResponses(Vec<Vec<OracleResponse>>),
    Events(Vec<Vec<Event>>),
    Blobs(Vec<Vec<Blob>>),
    OperationResults(Vec<OperationResult>),
}

impl BlockHeader {
    /// Whether `field` is the body field this header commits to.
    pub fn verifies(&self, field: &BlockBodyField) -> bool { /* recompute one hash, compare */ }
}
```

Light-client flow, two steps:

1. **Header authenticity** — `CryptoHash::new(&header) == cert.value.value_hash` plus
   signature check. Already exists (LiteCertificate machinery); now works because the
   header alone reproduces the signed hash.
2. **Field inclusion** — `header.verifies(&field)`. New.

The enum is preferred over 8 ad-hoc methods because it is the natural relay payload
`(BlockHeader, BlockBodyField)`.

## Touch-points / blast radius

- `linera-base/src/hashed.rs` — `with_hash`.
- `linera-chain/src/block.rs` — bulk of the change.
- `linera-core/src/client/mod.rs:2198` — raw hash → `.hash()`.
- Golden / schema snapshots — `linera-rpc/tests/format.rs` and the rpc-schema test in
  the pre-push hook regenerate (wire format and block hashes change).
- Genesis / stored chains — fresh genesis on `main`; deployment is out of code scope.

## Test plan (TDD, failing-first)

1. `cert.hash() == CryptoHash::new(&cert.block().header)` — reconstructable from the
   header alone. Fails today.
2. Integrity — deserialize a block whose serialized header hashes do not match its
   body → `de::Error`; assert the exact error string (pristine output).
3. Round-trip — `bcs` serialize → deserialize preserves block and hash.
4. Proof API — `header.verifies(field)` true for each of the 8 variants; mutated
   field → false.
5. Light-client end-to-end — real `ConfirmedBlockCertificate` from test helpers:
   check signatures against `CryptoHash::new(&header)`, then verify one field via
   `verifies`, all without the rest of the body. Real data, no mocks.
6. Regenerate snapshots; `cargo test -p linera-rpc` and the rpc-schema test green.

## Out of scope

- Backward compatibility / migration of existing block hashes (target is `main`,
  fresh genesis).
- The relayer/transport plumbing that ships `(BlockHeader, BlockBodyField)` over the
  wire — this design provides the verification primitive it would call.

## Risks

- Every block hash changes; any committed test vector or snapshot referencing block
  hashes must be regenerated.
- Wire/storage format of a block grows by ~256 bytes (8 content hashes now
  transmitted rather than recomputed) — accepted as the cost of header-only transport.
