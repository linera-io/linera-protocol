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
//! The last statement turns the classification outward: two correct validators at equal heights
//! agree on everything with a validity proof, and are entitled to differ on everything without one.
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

/// **Caveat (An inbox entry is never reclaimed).** Every structure cross-chain messaging uses is
/// bounded except one: a recipient keeps an inbox entry for each chain that has *ever* sent it a
/// message, permanently.
///
/// What is reclaimed: an outbox queue drains as bundles are delivered and confirmed, and the outbox
/// itself is then removed — `outboxes.remove_entry(target)` once the queue is empty and not ahead of
/// the tip, with `outbox_counters` and `nonempty_outboxes` cleared alongside. Queued and anticipated
/// bundles leave `added_bundles` on consumption and `removed_bundles` on arrival. The sender's
/// `unfinalized_message_blocks` is trimmed as recipients acknowledge.
///
/// What is not: nothing anywhere removes an entry from `ChainStateView::inboxes`. Once an origin has
/// delivered a single bundle, its `InboxStateView` — cursors and empty queues — persists for the
/// life of the chain. The residue is small per origin and unbounded in count, so the cost falls on
/// exactly the chains a network wants to encourage: a widely used application chain pays for every
/// counterparty it has ever had.
///
/// **Checkpointing preserves this rather than clearing it, by design.**
/// `PreparedCheckpoint::inbox_cursors` records *every* inbox with a non-default
/// `next_cursor_to_remove`, so a node bootstrapping from a checkpoint recreates the full set of
/// origins rather than starting clean. That is deliberate and load-bearing: by
/// `linera_chain::proof::checkpoints::CheckpointPreservesConsumptionBoundary` each origin's
/// `restored_cursor` is what turns a re-pushed already-consumed bundle into a no-op instead of a
/// duplicate consumption. Reclaiming an inbox would forget that boundary, so the two goals are in
/// direct tension and the current design resolves it in favour of correctness.
pub trait InboxEntriesAreNeverReclaimed: DerivedStateAgreesWithCertifiedPrefix {}

/// **Theorem (Storage converges at equal heights).** Take two correct validators that agree on the
/// tip height of every chain. Once cross-chain delivery has quiesced at both — no bundle derivable
/// from a committed block is still undelivered internally — their storage agrees on everything the
/// protocol determines:
///
/// | | agrees | why |
/// |---|---|---|
/// | execution state of every chain | yes, and *certifiably* so | it is a function of the committed prefix, and the last block's `state_hash` attests the value |
/// | committed blocks, their certificates, their events | yes | same prefix, and each is certified data |
/// | blobs a committed block requires | yes | named by the blocks, which agree |
/// | derived indexes and counters | yes | functions of the same prefix ([`DerivedStateAgreesWithCertifiedPrefix`]) |
/// | inbox consumption boundaries | yes | fixed by which bundles the committed blocks consumed |
/// | inbox queues and outbox queues | yes, **only after quiescence** | they hold what is delivered but not yet consumed, which is a function of the prefix *plus* delivery progress |
///
/// This is the first statement here about two validators rather than one, and it is what would make
/// a divergence *detectable*: at equal heights, two correct validators cannot differ on any row
/// above, so a difference convicts one of them of being faulty — which is the missing half of
/// [`AccountabilityScope`], where a mis-executed block leaves no forensic residue.
///
/// *Proof.* Fix a chain and a common tip height `h`.
///
/// *The prefixes coincide.* By [`CommitAgreement`] at most one block is certified per height, and by
/// `UniqueChain` the committed sequence below `h` is unique. Both validators reached `h` only
/// through valid certificates (`TipAdvancesOnlyOnValidCertificate`), so they hold the same blocks at
/// every height below `h`. Note the hypothesis is only about *heights*: agreement on content follows
/// rather than being assumed, and it is [`MaxByzantineWeight`] that makes it follow.
///
/// *Execution state follows the prefix.* By [`DerivedStateAgreesWithCertifiedPrefix`] each
/// validator's execution state equals the result of executing its committed prefix, and by
/// `DeterministicExecution` executing the same prefix yields the same result. The equality is
/// moreover *witnessed*: the `state_hash` in the block at `h - 1` is covered by that block's hash and
/// certified, so the agreed value is one a quorum attested rather than one each validator merely
/// computed.
///
/// *Derived state follows too*, by the same lemma — the height indexes, outbox counters and
/// `nonempty_*` sets are functions of the prefix.
///
/// *Message state needs the quiescence hypothesis.* What a chain has *consumed* is fixed by its
/// committed blocks, so inbox consumption boundaries agree immediately. What is *queued* is not: a
/// bundle is derived from a committed block of the sending chain and then delivered by that
/// validator's own worker for that chain ([`InboxHoldsOnlySentBundles`]), so at any instant one
/// validator may have delivered internally what the other has not. Since the sending chains are at
/// equal heights, both derive the same bundles ([`EffectsSurviveRestart`], sender half); once
/// delivery has quiesced both have delivered all of them, and by
/// [`BundleConsumedAtMostOnce`] neither has consumed one twice. The queues therefore coincide. ∎
///
/// **What does not converge, and need not.** Three classes, all legitimate.
///
/// *Retained history.* A validator that bootstrapped from a checkpoint holds a pruned chain: it has
/// the execution state without the blocks below the checkpoint, and events below a stream's floor
/// are gone ([`EventFloorTracksCheckpoints`]). So two validators at the same heights may hold
/// genuinely different *sets* of blocks and events, and the theorem above claims agreement only on
/// what they both retain. This is the sharpest limit on any consistency check built from it: a
/// missing block is not evidence of a fault.
///
/// *Work ahead of the tip.* `next_height_to_preprocess` may exceed the tip by different amounts,
/// since preprocessing a block does not advance it. One validator may hold certificates and outbox
/// updates for blocks the other has not seen.
///
/// *Local fields.* `pending_proposed_blobs`, `pending_validated_blobs`, `pre_checkpoint_block_trust`
/// and `received_log` are per-validator by construction — the last records the order in which
/// certificates arrived, which is not a function of anything committed.
///
/// **Quiescence is a hypothesis, not a guarantee.** Nothing here says delivery ever quiesces; that
/// would need the outbox to be drained, which no statement provides. So this is a conditional
/// convergence result, and the condition is exactly the one the messaging theme has yet to
/// discharge.
///
/// [`CommitAgreement`]: linera_chain::manager::proof::safety::CommitAgreement
/// [`MaxByzantineWeight`]: linera_chain::manager::proof::model::MaxByzantineWeight
/// [`AccountabilityScope`]: linera_chain::justification::proof::AccountabilityScope
/// [`EventFloorTracksCheckpoints`]: linera_chain::proof::checkpoints::EventFloorTracksCheckpoints
/// [`InboxHoldsOnlySentBundles`]: super::availability::InboxHoldsOnlySentBundles
/// [`EffectsSurviveRestart`]: super::availability::EffectsSurviveRestart
/// [`BundleConsumedAtMostOnce`]: super::availability::BundleConsumedAtMostOnce
pub trait StorageConvergesAtEqualHeights: DerivedStateAgreesWithCertifiedPrefix {}
