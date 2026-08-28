// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What may be assumed about anything read back from storage.
//!
//! Every result elsewhere in this specification reads state before it reasons about it: a vote is
//! justified by the manager state, a block by its ancestors, a bundle by an inbox cursor. What
//! entitles the reader to act on those bytes is the subject here.
//!
//! Stored data divides by what its **validity proof** is — the artifact a reader could check to
//! establish that what came back is what should have been there:
//!
//! | kind | validity proof | if it were wrong |
//! |---|---|---|
//! | *certified* | a certificate, checked once on admission | detected by re-verifying signatures |
//! | *derived* | **none** | undetectable; can only be recomputed from the certified prefix |
//! | *configuration* | none; assumed identical network-wide | not detectable |
//!
//! Content addressing sits beside this rather than inside it. A hash key proves *integrity* — that
//! the bytes are the ones the key names — which is a different question from whether the data ought
//! to be there at all. A blob's validity proof is a certificate like anything else's.
//!
//! The third row is the one that carries risk, and the specification has been quietly relying on
//! it: the `ChainError::CorruptedChainState` sites in `linera_chain::chain` are assertions about
//! derived data, made at the point of use because there is nothing to check it against earlier.
//!
//! Shared storage is partitioned by `RootKey` — `BlobId`, `BlockHash`, `Event`, `BlockByHeight`,
//! `EventBlockHeight`, `ChainState`, `NetworkDescription`, `BlockExporterState` — and the
//! classification runs across that partition rather than along it: `ChainState` alone holds fields
//! of three different kinds.

use linera_chain::manager::proof::model::{CorrectValidator, MaxByzantineWeight, StorageAtomicity};

use super::availability::BlockOutputsArePersisted;

/// **Lemma (Content addressing proves integrity, not validity).** For a blob, the key determines
/// the value: a `BlobId` is a hash of the content together with its `BlobType`, so bytes stored
/// under it are either the ones the key names or detectably wrong, with no appeal to who wrote
/// them. Certificates are keyed the same way, by the hash of the block they confirm.
///
/// This settles *substitution* and nothing else. That a blob was paid for, and that anyone is
/// obliged to keep its bytes available, are separate claims with a separate proof —
/// [`BlobValidityRestsOnCertificates`].
///
/// *Proof.* `RootKey::BlobId(blob.id())` derives the storage root from `Blob::id`, which hashes the
/// content. Recomputing the id of what comes back and comparing it to the key is therefore a
/// complete check, requiring no committee, no signature and no trust in the store. ∎
///
/// **Where the check is actually performed.** At trust boundaries, and only there. A blob arriving
/// from another node goes through `RemoteNode::download_blob`, which builds `Blob::new(blob)` —
/// recomputing the id — and rejects the response when `blob.id() != blob_id`. A blob read back from
/// this node's own storage does not: `DbStorage::read_blob` constructs it with
/// `Blob::new_with_id_unchecked`, taking the store at its word.
///
/// That asymmetry is deliberate and worth stating, because it locates the residual trust exactly.
/// Content addressing does not make storage trustworthy; it makes storage *auditable*, and the
/// implementation spends that audit where data crosses from a party it does not trust. Within a
/// validator, a store that returns the wrong bytes under a blob key is undetected — which
/// [`StorageAtomicity`] does not cover either, being about whether a write lands, not about whether
/// a read is faithful.
///
/// This is what [`AccountabilityScope`] means when it says blob integrity is free, and what
/// `CheckpointRestoresExecutionState` relies on when it says a node fetching an execution-state dump
/// cannot be handed different bytes.
///
/// [`AccountabilityScope`]: linera_chain::justification::proof::AccountabilityScope
pub trait ContentAddressingProvesIntegrity: CorrectValidator {}

/// **Lemma (A blob's validity rests on certificates, not on its hash).** A blob held by a correct
/// validator is one that a confirmed block published — and so paid for — and every later block that
/// uses it re-attests that it is still owed. Its hash establishes which bytes it is; its
/// certificates establish that it is entitled to exist.
///
/// *Proof.* `BlobState` records exactly this and nothing more:
///
/// | field | what it proves |
/// |---|---|
/// | `origin` | `BlobOrigin::Published { chain_id, block_height }` names the confirmed block that published it, which is where publication was charged; `BlobOrigin::Genesis` is the one exception, holding for blobs every node has from the genesis config |
/// | `last_used_by` | the hash of the most recent certificate that published *or used* the blob — a later quorum's attestation that it is still required |
/// | `epoch` | the epoch of that certificate, so the attestation can be weighed against which committees are still trusted |
///
/// Publication is charged by the block that performs it, at `blob_published` per blob and
/// `blob_byte_published` per byte, and admission is bounded in count and size — that is
/// [`BlobAdmissionIsBounded`]. Use is re-recorded through `maybe_write_blob_states`, which carries
/// forward the certificate that last needed the blob. ∎
///
/// **Re-certification is the same mechanism as elsewhere, applied to bytes.** A blob's entitlement
/// does not expire with the committee that first certified it, because each subsequent use is a
/// fresh attestation under a fresh epoch — the pattern
/// `linera_chain::proof::checkpoints::CheckpointRecertifiesReferencedBlocks` applies to blocks an
/// outbox still references. `BlobState`'s `epoch` field is what makes it checkable.
///
/// **What this does not give is availability.** Nothing here obliges anyone to still hold the bytes:
/// a validity proof establishes that a blob *should* be retrievable, not that it *is*. That gap is
/// [`BlobRetention`], which is currently discharged by omission, since nothing deletes blobs. The
/// shape of `BlobState` is what a retention policy would have to be keyed on — a blob whose
/// `last_used_by` certificate is in a still-trusted epoch is one some live block may still require,
/// which is a different question from how old it is.
///
/// [`BlobAdmissionIsBounded`]: super::availability::BlobAdmissionIsBounded
/// [`BlobRetention`]: super::assumptions::BlobRetention
pub trait BlobValidityRestsOnCertificates: ContentAddressingProvesIntegrity {}

/// **Lemma (Nothing enters shared storage without its validity proof having been checked).** A
/// certificate, a blob or an event in a correct validator's shared storage was verified against the
/// committee for its epoch before it was written.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | `ChainWorkerState::process_confirmed_block` |
/// | reads | `committee_for_epoch(block.header.epoch)` |
/// | writes | `write_blobs_and_certificate`, then `write_events` |
/// | precondition | `certificate.check` returned `Ok`, before either write |
///
/// *Proof.* `process_confirmed_block` resolves the committee for the block's declared epoch and
/// calls `certificate.check` against it. Only afterwards does it write: the certificate and the
/// block's required blobs through `write_blobs_and_certificate`, then the block's events through
/// `write_events`. Both writes are inside the branch guarded by that check, and the ordering is
/// [`BlockOutputsArePersisted`]'s. A blob admitted *ahead* of certification takes the other route,
/// `handle_pending_blob`, which admits only blobs a pending proposal or validated block expects and
/// fails with `WorkerError::UnexpectedBlob` otherwise. ∎
///
/// **The proof is checked once, not on every read.** Nothing re-verifies a certificate's signatures
/// when it is read back, so a reader inside the validator relies on the admission check having
/// happened rather than on the certificate in hand. The proof remains *attached* — a certificate
/// read from storage can be re-verified, and is, when it crosses to another node — so this is a
/// choice about where to spend verification, not a loss of evidence. It is the same shape as
/// [`ContentAddressingProvesIntegrity`]'s asymmetry, for a different kind of proof.
///
/// **Events inherit their proof rather than carrying one.** An event has no signature of its own:
/// it is valid because the block that emitted it is certified, and it is written in the same guarded
/// branch. A reader that has the event but not that block is trusting the writer — which is what
/// `linera_chain::proof::checkpoints::EventFloorTracksCheckpoints` means when it says a
/// cross-chain read resolves only at or above a stream's floor.
pub trait AdmissionChecksTheValidityProof:
    BlobValidityRestsOnCertificates + BlockOutputsArePersisted
{
}

/// **Invariant (Derived state agrees with the certified prefix).** The parts of a chain's state that
/// are neither self-verifying nor quorum-attested — the block-height indexes, the outbox counters
/// and queues, the inbox cursors — are functions of that chain's committed blocks, and equal the
/// value that recomputing them from those blocks would give.
///
/// **This is the class with no validity proof.** A wrong index or a wrong counter is not detectable
/// by rehashing or by re-verifying signatures, because nobody attested it and nothing determines it
/// but the computation that produced it. It can only be *recomputed*. That is why this is stated as
/// an invariant over transitions rather than as a property a reader can check.
///
/// *Proof.* Each is written only by the transition that commits a block, under the exclusive access
/// of [`SequentialChainState`] and the atomicity of [`StorageAtomicity`], so the sequence of values
/// it takes follows the sequence of committed blocks; and by
/// `linera_chain::manager::proof::safety::UniqueChain` that sequence is unique. The base case is an
/// empty chain, where every one of these is empty or zero. ∎
///
/// **Detection is partial, late, and by assertion.** Because there is no proof to check, the
/// implementation catches violations only where a reader happens to require an entry that should be
/// there. `ChainError::CorruptedChainState` is raised at seven sites in `linera_chain::chain`,
/// including:
///
/// * `"message counter should be present"` — an outbox counter missing for a queue entry;
/// * `"Missing outboxes"` — a `nonempty_outboxes` entry with no outbox behind it;
/// * `"missing entry in block_hashes"`, at three separate call sites — the height index short of the
///   tip.
///
/// Each fires at the point of use, which may be arbitrarily long after the write that broke the
/// invariant, and none of them fires for a value that is present but *wrong*. A counter that is
/// merely too small is not detected at all until the queue drains past it.
///
/// **Recovery is by recomputation, which is the only option available.** `reconcile_tracked_outboxes`
/// rebuilds the outbox index, and `ChainStateView::restore_outboxes_from_unfinalized` rebuilds
/// `outboxes`, `outbox_counters` and `nonempty_outboxes` from the on-chain
/// `unfinalized_message_blocks` after a checkpoint bootstrap. Both are re-derivations from data that
/// *does* have a validity proof, which is what makes them trustworthy where the derived state was
/// not.
///
/// **Not covered here.** Fields with no invariant at all, because they are legitimately local and
/// may differ between correct validators or be dropped without fault: `pending_proposed_blobs` and
/// `pending_validated_blobs` (cleared when the manager is reset), `pre_checkpoint_block_trust`
/// (transient, emptied as the certificates arrive), and `received_log`, whose order depends on when
/// certificates were received rather than on what was committed.
///
/// [`SequentialChainState`]: linera_chain::manager::proof::model::SequentialChainState
pub trait DerivedStateAgreesWithCertifiedPrefix:
    AdmissionChecksTheValidityProof + StorageAtomicity + MaxByzantineWeight
{
}
