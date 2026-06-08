# Block Header Commitment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the consensus block hash commit to the `BlockHeader` (which already hashes every body field) instead of the whole serialized `Block`, so a `BlockHeader` plus a single `BlockBody` field is enough to prove inclusion.

**Architecture:** Decouple "what is hashed" from "what is stored". The block's canonical hash becomes `CryptoHash::new(&BlockHeader)`. `Block`'s wire format now carries the full `BlockHeader` (all 15 fields) plus the body, and `Deserialize for Block` verifies the body against the header's content hashes. `ConfirmedBlock`/`ValidatedBlock` store the header hash via a new `Hashed::with_hash` constructor. A `BlockBodyField` enum plus `BlockHeader::verifies` provides the inclusion-proof primitive.

**Tech Stack:** Rust, serde, bcs (canonical hashing/serialization), insta (RPC format snapshot).

**Reference spec:** `2026-06-08-block-header-commitment-design.md` (worktree root).

**Worktree:** `.worktrees/block-header-commitment`, branch `block-header-commitment` off `origin/main`. Run all `git` commands with `GIT_CONFIG_NOSYSTEM=1` (sandbox blocks `/etc/gitconfig`). Run all `cargo`/`git` commands from the worktree root.

---

## File Structure

- `linera-base/src/hashed.rs` — add `Hashed::with_hash(value, hash)` (precomputed-hash constructor, no `BcsHashable` bound).
- `linera-chain/src/block.rs` — the bulk: `BlockHeader: BcsHashable`, `Block::hash()`, rewritten `Serialize`/`Deserialize for Block` with integrity check, remove `BcsHashable for Block` and `SerializedHeader`, manual serde for `ConfirmedBlock`/`ValidatedBlock`, `BlockBodyField` + `BlockHeader::verifies`.
- `linera-chain/src/unit_tests/data_types_tests.rs` — all new unit tests (reuses existing helpers `make_first_block`, `BlockExecutionOutcome::with`, `dummy_chain_id`).
- `linera-core/src/client/mod.rs:2254` — raw `CryptoHash::new(executed_block)` → `executed_block.hash()`.
- `linera-indexer/lib/src/db/postgres/tests.rs`, `linera-indexer/lib/src/db/sqlite/tests.rs` — `Hashed::new(test_block.clone()).hash()` → `test_block.hash()` (4 sites).
- `linera-rpc/tests/snapshots/format__format.yaml.snap` — regenerated (wire format changed).

---

### Task 1: `Hashed::with_hash` constructor

**Files:**
- Modify: `linera-base/src/hashed.rs:23-31` (add method in the existing `impl<T> Hashed<T>` block)
- Test: `linera-base/src/hashed.rs` (inline `#[cfg(test)]` module at end of file)

- [ ] **Step 1: Write the failing test**

Append to `linera-base/src/hashed.rs`:

```rust
#[cfg(test)]
mod tests {
    use crate::{
        crypto::{BcsHashable, CryptoHash},
        hashed::Hashed,
    };

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Dummy(u8);
    impl BcsHashable<'_> for Dummy {}

    #[test]
    fn with_hash_stores_provided_hash() {
        let forced = CryptoHash::test_hash("forced");
        let hashed = Hashed::with_hash(Dummy(7), forced);
        assert_eq!(hashed.hash(), forced);
        assert_ne!(hashed.hash(), CryptoHash::new(&Dummy(7)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p linera-base --features test hashed::tests::with_hash_stores_provided_hash`
Expected: FAIL to compile — `no function or associated item named with_hash`.

(Note: `CryptoHash::test_hash` requires the `with_testing`/`test` feature; if the feature flag name differs, run `cargo test -p linera-base --all-features hashed::tests`.)

- [ ] **Step 3: Add the constructor**

In `linera-base/src/hashed.rs`, inside `impl<T> Hashed<T> {` (after `new`, around line 31):

```rust
    /// Creates a [`Hashed`] from a value and a precomputed hash, without recomputing it.
    ///
    /// The caller is responsible for the hash being the canonical hash of `value`.
    pub fn with_hash(value: T, hash: CryptoHash) -> Self {
        Self { value, hash }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p linera-base --all-features hashed::tests::with_hash_stores_provided_hash`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
GIT_CONFIG_NOSYSTEM=1 git add linera-base/src/hashed.rs
GIT_CONFIG_NOSYSTEM=1 git -c commit.gpgsign=false commit -m "Add Hashed::with_hash for precomputed hashes"
```

---

### Task 2: `BlockHeader: BcsHashable` and `Block::hash()`

**Files:**
- Modify: `linera-chain/src/block.rs` (add `impl BcsHashable for BlockHeader` next to `impl BcsHashable<'_> for Block {}` at line 674; add `Block::hash` in the `impl Block` block)
- Test: `linera-chain/src/unit_tests/data_types_tests.rs`

This task is purely additive — `Block` keeps its existing `BcsHashable` impl, so the workspace still builds.

- [ ] **Step 1: Add a shared test helper and the failing test**

In `linera-chain/src/unit_tests/data_types_tests.rs`, extend the import (line 11-14) to add `Block` (`BlockBodyField` is added later, in Task 5):

```rust
use super::*;
use crate::{
    block::{Block, ConfirmedBlock, ValidatedBlock},
    test::{make_first_block, BlockTestExt},
};
```

Add this helper near `dummy_chain_id` (after line 18):

```rust
fn sample_block() -> Block {
    BlockExecutionOutcome {
        messages: vec![Vec::new()],
        previous_message_blocks: BTreeMap::new(),
        previous_event_blocks: BTreeMap::new(),
        state_hash: CryptoHash::test_hash("state"),
        oracle_responses: vec![Vec::new()],
        events: vec![Vec::new()],
        blobs: vec![Vec::new()],
        operation_results: vec![OperationResult::default()],
    }
    .with(make_first_block(dummy_chain_id(1)).with_simple_transfer(dummy_chain_id(2), Amount::ONE))
}

#[test]
fn block_hash_is_header_hash() {
    let block = sample_block();
    assert_eq!(block.hash(), CryptoHash::new(&block.header));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p linera-chain data_types_tests::block_hash_is_header_hash`
Expected: FAIL to compile — `no method named hash found for ... Block` and `BlockBodyField` unresolved.

- [ ] **Step 3: Implement `BcsHashable for BlockHeader` and `Block::hash`**

In `linera-chain/src/block.rs`, at line 674 (next to the existing `impl BcsHashable<'_> for Block {}`):

```rust
impl BcsHashable<'_> for Block {}
impl BcsHashable<'_> for BlockHeader {}
```

In the `impl Block { ... }` block (e.g. right after `pub fn new(...)` near line 453), add:

```rust
    /// Returns the hash of this block, which commits to the entire block via its header.
    pub fn hash(&self) -> CryptoHash {
        CryptoHash::new(&self.header)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p linera-chain data_types_tests::block_hash_is_header_hash`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
GIT_CONFIG_NOSYSTEM=1 git add linera-chain/src/block.rs linera-chain/src/unit_tests/data_types_tests.rs
GIT_CONFIG_NOSYSTEM=1 git -c commit.gpgsign=false commit -m "Add Block::hash and BcsHashable for BlockHeader"
```

---

### Task 3: Switch the commitment to the header hash (atomic, cross-crate)

This is the core behavioral change. It removes `BcsHashable for Block`, which breaks every site that hashes a raw `Block` or relies on the generic `Hashed<Block>` (de)serialize. All those sites are fixed in this same commit so the workspace builds after it.

**Files:**
- Modify: `linera-chain/src/block.rs` — `Serialize`/`Deserialize for Block`; remove `SerializedHeader`; remove `impl BcsHashable<'_> for Block {}`; `ConfirmedBlock`/`ValidatedBlock` manual serde + `with_hash`.
- Modify: `linera-core/src/client/mod.rs:2254`.
- Modify: `linera-indexer/lib/src/db/postgres/tests.rs` (lines ~101, ~182), `linera-indexer/lib/src/db/sqlite/tests.rs` (lines ~90, ~184).
- Test: `linera-chain/src/unit_tests/data_types_tests.rs`.

- [ ] **Step 1: Write the failing tests**

In `linera-chain/src/unit_tests/data_types_tests.rs`, add:

```rust
#[test]
fn confirmed_block_hash_is_header_hash() {
    let block = sample_block();
    let header_hash = CryptoHash::new(&block.header);
    assert_eq!(ConfirmedBlock::new(block.clone()).hash(), header_hash);
    assert_eq!(ValidatedBlock::new(block).hash(), header_hash);
}

#[test]
fn block_serde_round_trip_preserves_hash() {
    let block = sample_block();
    let hash = block.hash();
    let bytes = bcs::to_bytes(&block).unwrap();
    let restored: Block = bcs::from_bytes(&bytes).unwrap();
    assert_eq!(restored, block);
    assert_eq!(restored.hash(), hash);
}

#[test]
fn deserialize_rejects_body_not_matching_header() {
    let mut block = sample_block();
    // Corrupt one header hash so it no longer matches the body.
    block.header.events_hash = CryptoHash::test_hash("wrong");
    let bytes = bcs::to_bytes(&block).unwrap();
    let err = bcs::from_bytes::<Block>(&bytes).unwrap_err();
    assert!(
        err.to_string().contains("body does not match its header hashes"),
        "unexpected error: {err}"
    );
}
```

Add `ValidatedBlock::height()` is already available; no extra helpers needed.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p linera-chain data_types_tests::confirmed_block_hash_is_header_hash`
Expected: FAIL — `confirmed_block_hash_is_header_hash` asserts equality that is false today (the confirmed hash currently includes the body).

- [ ] **Step 3: Rewrite `Serialize for Block`**

Replace `linera-chain/src/block.rs:247-264` with:

```rust
impl Serialize for Block {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Block", 2)?;
        state.serialize_field("header", &self.header)?;
        state.serialize_field("body", &self.body)?;
        state.end()
    }
}
```

- [ ] **Step 4: Rewrite `Deserialize for Block` with the integrity check**

Replace `linera-chain/src/block.rs:266-312` with:

```rust
impl<'de> Deserialize<'de> for Block {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "Block")]
        struct Inner {
            header: BlockHeader,
            body: BlockBody,
        }
        let Inner { header, body } = Inner::deserialize(deserializer)?;

        let expected = BlockHeader {
            transactions_hash: hashing::hash_vec(&body.transactions),
            messages_hash: hashing::hash_vec_vec(&body.messages),
            previous_message_blocks_hash: CryptoHash::new(&PreviousMessageBlocksMap {
                inner: Cow::Borrowed(&body.previous_message_blocks),
            }),
            previous_event_blocks_hash: CryptoHash::new(&PreviousEventBlocksMap {
                inner: Cow::Borrowed(&body.previous_event_blocks),
            }),
            oracle_responses_hash: hashing::hash_vec_vec(&body.oracle_responses),
            events_hash: hashing::hash_vec_vec(&body.events),
            blobs_hash: hashing::hash_vec_vec(&body.blobs),
            operation_results_hash: hashing::hash_vec(&body.operation_results),
            ..header.clone()
        };
        if expected != header {
            return Err(serde::de::Error::custom(
                "block body does not match its header hashes",
            ));
        }

        Ok(Self { header, body })
    }
}
```

- [ ] **Step 5: Remove `SerializedHeader`**

Delete the `SerializedHeader` struct at `linera-chain/src/block.rs:690-700`:

```rust
#[derive(Serialize, Deserialize)]
#[serde(rename = "BlockHeader")]
struct SerializedHeader {
    chain_id: ChainId,
    epoch: Epoch,
    height: BlockHeight,
    timestamp: Timestamp,
    state_hash: CryptoHash,
    previous_block_hash: Option<CryptoHash>,
    authenticated_owner: Option<AccountOwner>,
}
```

- [ ] **Step 6: Remove `impl BcsHashable<'_> for Block {}`**

At `linera-chain/src/block.rs:674`, delete the `Block` line, keeping the header one:

```rust
impl BcsHashable<'_> for BlockHeader {}
```

- [ ] **Step 7: Give `ConfirmedBlock` and `ValidatedBlock` manual serde + `with_hash`**

Replace `ValidatedBlock`'s derive/attribute (`linera-chain/src/block.rs:32-34`):

```rust
#[derive(Debug, PartialEq, Eq, Clone, Allocative)]
pub struct ValidatedBlock(Hashed<Block>);

impl Serialize for ValidatedBlock {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ValidatedBlock {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::new(Block::deserialize(deserializer)?))
    }
}
```

Change `ValidatedBlock::new` (`block.rs:38-40`) to use `with_hash`:

```rust
    pub fn new(block: Block) -> Self {
        let hash = block.hash();
        Self(Hashed::with_hash(block, hash))
    }
```

Replace `ConfirmedBlock`'s derive/attribute (`linera-chain/src/block.rs:78-80`):

```rust
#[derive(Debug, PartialEq, Eq, Clone, Allocative)]
pub struct ConfirmedBlock(Hashed<Block>);

impl Serialize for ConfirmedBlock {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ConfirmedBlock {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::new(Block::deserialize(deserializer)?))
    }
}
```

Change `ConfirmedBlock::new` (`block.rs:99-101`) to use `with_hash`:

```rust
    pub fn new(block: Block) -> Self {
        let hash = block.hash();
        Self(Hashed::with_hash(block, hash))
    }
```

(The `#[async_graphql::Object]` impl on `ConfirmedBlock` and the `From<Hashed<Block>>` impls are unchanged. The manual `Serialize` matches the previous `#[serde(transparent)]` byte layout, so the wire format of the wrappers is unchanged relative to the new `Block` format.)

- [ ] **Step 8: Fix the raw-hash site in linera-core**

`linera-core/src/client/mod.rs:2254`:

```rust
                let hash = executed_block.hash();
```

(was `let hash = CryptoHash::new(executed_block);`). If `CryptoHash` becomes an unused import in that file, remove it from the `use` block to satisfy `-D warnings`.

- [ ] **Step 9: Fix the indexer test sites**

In both `linera-indexer/lib/src/db/postgres/tests.rs` and `linera-indexer/lib/src/db/sqlite/tests.rs`, replace every:

```rust
    let block_hash = Hashed::new(test_block.clone()).hash();
```

with:

```rust
    let block_hash = test_block.hash();
```

(2 occurrences per file.) Remove the now-unused `Hashed` import in those files if the compiler flags it.

- [ ] **Step 10: Build the whole workspace to flush out any remaining breakages**

Run: `cargo build --workspace --all-features --tests 2>&1 | tail -40`
Expected: builds. If the compiler flags any other `CryptoHash::new(&block)` / `Hashed::new(block)` on a `Block`, replace it with `block.hash()` (or `ConfirmedBlock::new(block).hash()` where a `ConfirmedBlock` is wanted). Re-run until clean.

- [ ] **Step 11: Run the new tests**

Run: `cargo test -p linera-chain data_types_tests::confirmed_block_hash_is_header_hash data_types_tests::block_serde_round_trip_preserves_hash data_types_tests::deserialize_rejects_body_not_matching_header`
Expected: all PASS.

- [ ] **Step 12: Run the existing linera-chain suite (no regressions)**

Run: `cargo test -p linera-chain 2>&1 | tail -30`
Expected: PASS. (`test_signed_values` / `test_certificates` do not hardcode hashes and should still pass.)

- [ ] **Step 13: Commit**

```bash
GIT_CONFIG_NOSYSTEM=1 git add linera-chain/src/block.rs linera-chain/src/unit_tests/data_types_tests.rs linera-core/src/client/mod.rs linera-indexer/lib/src/db/postgres/tests.rs linera-indexer/lib/src/db/sqlite/tests.rs
GIT_CONFIG_NOSYSTEM=1 git -c commit.gpgsign=false commit -m "Commit to BlockHeader hash instead of whole Block"
```

---

### Task 4: Regenerate the RPC format snapshot

The `Block` wire format changed (header now carries all 15 fields), so the serde-reflection registry in `linera-rpc` changed.

**Files:**
- Modify (regenerate): `linera-rpc/tests/snapshots/format__format.yaml.snap`

- [ ] **Step 1: Observe the snapshot test fail**

Run: `cargo test -p linera-rpc --test format 2>&1 | tail -30`
Expected: FAIL — snapshot mismatch. The diff should show `BlockHeader` gaining the 8 content-hash fields in the `Block`/`BlockHeader` entries, and nothing semantically unexpected.

- [ ] **Step 2: Inspect the diff, then accept it**

If `cargo-insta` is installed:

```bash
cargo insta review
```

Otherwise regenerate in place:

```bash
INSTA_UPDATE=always cargo test -p linera-rpc --test format
```

Then read the diff and confirm the only changes are the added `BlockHeader` hash fields:

```bash
GIT_CONFIG_NOSYSTEM=1 git diff -- linera-rpc/tests/snapshots/format__format.yaml.snap | head -80
```

- [ ] **Step 3: Verify the snapshot test passes**

Run: `cargo test -p linera-rpc --test format`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
GIT_CONFIG_NOSYSTEM=1 git add linera-rpc/tests/snapshots/format__format.yaml.snap
GIT_CONFIG_NOSYSTEM=1 git -c commit.gpgsign=false commit -m "Regenerate RPC format snapshot for new block layout"
```

---

### Task 5: Inclusion-proof API (`BlockBodyField` + `BlockHeader::verifies`)

**Files:**
- Modify: `linera-chain/src/block.rs` — add `BlockBodyField` enum and `BlockHeader::verifies`.
- Test: `linera-chain/src/unit_tests/data_types_tests.rs`.

- [ ] **Step 1: Write the failing tests**

In `linera-chain/src/unit_tests/data_types_tests.rs` (ensure `BlockBodyField` is in the `crate::block::{...}` import):

```rust
#[test]
fn header_verifies_each_body_field() {
    let block = sample_block();
    let h = &block.header;
    let b = &block.body;
    assert!(h.verifies(&BlockBodyField::Transactions(b.transactions.clone())));
    assert!(h.verifies(&BlockBodyField::Messages(b.messages.clone())));
    assert!(h.verifies(&BlockBodyField::PreviousMessageBlocks(b.previous_message_blocks.clone())));
    assert!(h.verifies(&BlockBodyField::PreviousEventBlocks(b.previous_event_blocks.clone())));
    assert!(h.verifies(&BlockBodyField::OracleResponses(b.oracle_responses.clone())));
    assert!(h.verifies(&BlockBodyField::Events(b.events.clone())));
    assert!(h.verifies(&BlockBodyField::Blobs(b.blobs.clone())));
    assert!(h.verifies(&BlockBodyField::OperationResults(b.operation_results.clone())));
}

#[test]
fn header_rejects_wrong_field() {
    let block = sample_block();
    // sample_block contains one transfer, so an empty transactions list must not verify.
    assert!(!block
        .header
        .verifies(&BlockBodyField::Transactions(Vec::new())));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p linera-chain data_types_tests::header_verifies_each_body_field`
Expected: FAIL to compile — `BlockBodyField` and `verifies` not found.

- [ ] **Step 3: Implement the enum and `verifies`**

In `linera-chain/src/block.rs`, after the `impl Block { ... }` block (before `impl BcsHashable<'_> for BlockHeader {}`), add:

```rust
/// A single field of a [`BlockBody`], paired with enough data to recompute its hash and
/// check it against the matching hash in a [`BlockHeader`]. Lets a holder of a header prove
/// that one body field belongs to the block without the rest of the body.
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
    /// Returns whether `field` is the body field this header commits to.
    pub fn verifies(&self, field: &BlockBodyField) -> bool {
        match field {
            BlockBodyField::Transactions(v) => hashing::hash_vec(v) == self.transactions_hash,
            BlockBodyField::Messages(v) => hashing::hash_vec_vec(v) == self.messages_hash,
            BlockBodyField::PreviousMessageBlocks(m) => {
                CryptoHash::new(&PreviousMessageBlocksMap {
                    inner: Cow::Borrowed(m),
                }) == self.previous_message_blocks_hash
            }
            BlockBodyField::PreviousEventBlocks(m) => {
                CryptoHash::new(&PreviousEventBlocksMap {
                    inner: Cow::Borrowed(m),
                }) == self.previous_event_blocks_hash
            }
            BlockBodyField::OracleResponses(v) => {
                hashing::hash_vec_vec(v) == self.oracle_responses_hash
            }
            BlockBodyField::Events(v) => hashing::hash_vec_vec(v) == self.events_hash,
            BlockBodyField::Blobs(v) => hashing::hash_vec_vec(v) == self.blobs_hash,
            BlockBodyField::OperationResults(v) => {
                hashing::hash_vec(v) == self.operation_results_hash
            }
        }
    }
}
```

All referenced types (`Transaction`, `OutgoingMessage`, `OracleResponse`, `Event`, `Blob`, `OperationResult`, `ChainId`, `StreamId`, `CryptoHash`, `BlockHeight`, `BTreeMap`, `Cow`) are already imported at the top of `block.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p linera-chain data_types_tests::header_verifies_each_body_field data_types_tests::header_rejects_wrong_field`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
GIT_CONFIG_NOSYSTEM=1 git add linera-chain/src/block.rs linera-chain/src/unit_tests/data_types_tests.rs
GIT_CONFIG_NOSYSTEM=1 git -c commit.gpgsign=false commit -m "Add BlockBodyField inclusion-proof API"
```

---

### Task 6: Light-client end-to-end test

Proves the full claim: a real `ConfirmedBlockCertificate` can be verified, and one body field proven, from the `BlockHeader` alone.

**Files:**
- Test: `linera-chain/src/unit_tests/data_types_tests.rs`.

- [ ] **Step 1: Write the test**

Add to `linera-chain/src/unit_tests/data_types_tests.rs`:

```rust
#[test]
fn light_client_verifies_header_and_one_field() {
    let validator_key_pair = ValidatorKeypair::generate();
    let account_secret = AccountSecretKey::Ed25519(Ed25519SecretKey::generate());
    let committee = Committee::make_simple(vec![(
        validator_key_pair.public_key,
        account_secret.public(),
    )]);

    let value = ConfirmedBlock::new(sample_block());
    let vote = LiteVote::new(
        LiteValue::new(&value),
        Round::Fast,
        &validator_key_pair.secret_key,
    );
    let mut builder = SignatureAggregator::new(value.clone(), Round::Fast, &committee);
    let certificate = builder
        .append(validator_key_pair.public_key, vote.signature)
        .unwrap()
        .unwrap();

    // The signed commitment is reproducible from the header alone.
    let header = certificate.block().header.clone();
    assert_eq!(CryptoHash::new(&header), certificate.hash());

    // One body field can be proven against that header without the rest of the body.
    let events = certificate.block().body.events.clone();
    assert!(header.verifies(&BlockBodyField::Events(events)));
}
```

Confirm the imports at the top of the file include `Committee` and the validator/account key types. They are referenced by the existing `test_certificates`, so they are already imported via `use super::*;` and the explicit `use linera_base::crypto::{...}` line. If `Committee` is not in scope, add `use linera_execution::committee::Committee;`.

- [ ] **Step 2: Run the test**

Run: `cargo test -p linera-chain data_types_tests::light_client_verifies_header_and_one_field`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
GIT_CONFIG_NOSYSTEM=1 git add linera-chain/src/unit_tests/data_types_tests.rs
GIT_CONFIG_NOSYSTEM=1 git -c commit.gpgsign=false commit -m "Add light-client header-and-field verification test"
```

---

### Task 7: Full verification

Surface any remaining failures (notably any test elsewhere that hardcoded a block/certificate hash) and satisfy the pre-push gates.

- [ ] **Step 1: Run the directly-affected crate test suites**

Run: `cargo test -p linera-base -p linera-chain -p linera-rpc 2>&1 | tail -30`
Expected: PASS.

- [ ] **Step 2: Run the downstream crate that changed**

Run: `cargo test -p linera-core 2>&1 | tail -40`
Expected: PASS. If a test asserts a specific (now-changed) block or certificate hash, regenerate that expected value from the test's own construction (do not weaken the assertion); if a snapshot, `cargo insta review`. Investigate any failure for a real regression before regenerating.

- [ ] **Step 3: Workspace build incl. all features/targets (matches pre-push clippy scope)**

Run: `cargo clippy --workspace --all-targets --all-features 2>&1 | tail -40`
Expected: no warnings (pre-push runs `-D warnings`). Fix any unused-import warnings introduced by the removed `CryptoHash::new`/`Hashed::new` calls.

- [ ] **Step 4: Format**

Run: `cargo fmt --all` then `GIT_CONFIG_NOSYSTEM=1 git diff --stat`
Expected: no unexpected churn; stage any formatting of the files we touched.

- [ ] **Step 5: Commit any fixes from Steps 2-4**

```bash
GIT_CONFIG_NOSYSTEM=1 git add -A
GIT_CONFIG_NOSYSTEM=1 git status   # review before committing
GIT_CONFIG_NOSYSTEM=1 git -c commit.gpgsign=false commit -m "Fix downstream hash assertions, lints, and formatting"
```

(Only run `git add -A` after reviewing `git status`. Do not stage the design/plan markdown files — Mat asked not to commit the spec.)

- [ ] **Step 6: Stop and hand off**

The remaining gates are heavy and Mat-owned: the full pre-push hook (fmt, clippy `--locked --all-targets --all-features -D warnings`, machete, taplo, rpc schema), and the e2e suites. Report status and use `superpowers:finishing-a-development-branch` to decide on PR vs. further testing. Do not push or open a PR without Mat's approval.

---

## Self-Review

**Spec coverage:**
- Scope 1 (commitment = header hash): Tasks 2, 3 (`Block::hash`, wrappers via `with_hash`, `confirmed_block_hash_is_header_hash`). ✓
- Scope 2 (proof API): Task 5 (`BlockBodyField` + `verifies`), Task 6 (light-client e2e). ✓
- Scope 3 (integrity on deserialize): Task 3 Step 4 + `deserialize_rejects_body_not_matching_header`. ✓
- Touch-points from spec — `hashed.rs` (Task 1), `block.rs` (Tasks 2/3/5), `client/mod.rs:2254` (Task 3 Step 8), format snapshot (Task 4). ✓
- Extra breakages not in the spec but required: indexer test sites (Task 3 Step 9). Captured.

**Placeholder scan:** No TBD/TODO; every code step shows full code. The `verifies` body and serde impls are complete.

**Type consistency:** `Hashed::with_hash(value, hash)` defined in Task 1, used in Task 3. `Block::hash(&self) -> CryptoHash` defined in Task 2, used in Tasks 3/6 and the indexer/core fixes. `BlockBodyField` variants defined in Task 5 match those used in Tasks 5/6 tests. Error string `"block body does not match its header hashes"` (Task 3 Step 4) matches the substring asserted in `deserialize_rejects_body_not_matching_header` (`"body does not match its header hashes"`). Field name `authenticated_owner` matches `origin/main`.

**Ordering:** Each commit leaves the workspace building (Task 3 fixes all cross-crate breakages in one commit). The only momentary red is the `linera-rpc` format snapshot between Task 3 and Task 4 — expected red→green within the session.
