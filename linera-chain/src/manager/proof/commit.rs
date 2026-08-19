// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The commit rule: what it takes for a block to become final, and what a node does about it.
//!
//! The protocol has a two-step commit rule outside the fast round — validate, then confirm — and
//! a one-step rule inside it. This module pins down both, and connects "a confirmed block
//! certificate exists" to the observable state a node reaches.

use crate::{
    data_types::proof::quorum::CertificateCarriesCorrectVote,
    manager::proof::{
        model::MaxByzantineWeight,
        voting::{ConfirmationNeedsValidatedCertificate, ProposalGate},
    },
};

/// **Definition (Committed block).** A block `B` at height `h` of a chain is *committed* when a
/// [`ConfirmedBlockCertificate`] for `B`, valid for the committee of its epoch, exists — that
/// is, when a quorum has cast confirmation votes for `B`.
///
/// The commit rule is therefore:
///
/// | round | rule |
/// |---|---|
/// | [`Round::Fast`] | a quorum of confirmation votes on a super owner's fast proposal, with no validation step |
/// | any other round `r` | a quorum of validation votes for `B` in round `r`, then a quorum of confirmation votes for `B` in round `r` |
///
/// Commitment is a property of the *world*, not of any one node: a block can be committed before
/// any particular correct validator learns of it. The node-local reflection of it is
/// [`TipAdvancesOnlyOnValidCertificate`].
///
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
pub trait CommittedBlock {}

/// **Lemma (A commit outside the fast round rests on a validated block certificate).** If a
/// [`ConfirmedBlockCertificate`] for `B` is certified in a round `r` other than
/// [`Round::Fast`](linera_base::data_types::Round::Fast), then a valid
/// [`ValidatedBlockCertificate`] for `B` in the *same* round `r` exists.
///
/// *Proof.* By [`CertificateCarriesCorrectVote`] some correct validator cast a confirmation vote
/// for `B` in round `r`. By [`ConfirmationNeedsValidatedCertificate`], since `r` is not the fast
/// round, a valid [`ValidatedBlockCertificate`] for `B` in round `r` existed when it did so. ∎
///
/// This is what lets the safety argument reason exclusively about *validated* certificates:
/// every commit above the fast round is backed by one, in its own round.
///
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
pub trait CommitRestsOnValidation:
    CertificateCarriesCorrectVote + ConfirmationNeedsValidatedCertificate
{
}

/// **Lemma (The tip advances only on a verified certificate).** A correct validator's
/// [`ChainTipState::next_block_height`] passes from `h` to `h + 1`, and its
/// [`block_hashes`](crate::ChainStateView) records a hash at `h`, only for a block carried by a
/// [`ConfirmedBlockCertificate`] that has passed [`check`] against the committee of the block's
/// epoch.
///
/// *Proof.* The tip register is advanced in one place in `linera_chain::chain`, at the end of
/// [`ChainStateView::apply_confirmed_block`] (`tip.next_block_height.try_add_assign_one()`).
/// That method has a single call site in the workspace outside tests, in
/// `ChainWorkerState::execute_contiguous_block`; `execute_block_with_checkpoint_restore` reaches
/// it by delegating there after installing the snapshot. Both are reached only through
/// `ChainWorkerState::process_confirmed_block`, whose every non-early-return path first
/// evaluates `certificate.check(&committee)?` with `committee` fetched for `block.header.epoch`.
/// The early returns — the `tip.next_block_height > height` skip and the `Preprocess` dispatch —
/// do not advance the tip. ∎
///
/// **Where this is fragile.** [`ChainStateView::apply_confirmed_block`] is `pub`, on a type this
/// crate re-exports, and it takes a [`ConfirmedBlock`] and no [`Committee`] — so it cannot verify
/// anything, and nothing about its signature confines it to verified callers. The enumeration
/// above holds by current usage, not by visibility. A new caller must perform the certificate
/// check itself.
///
/// Two paths deserve explicit mention because they weaken the *precondition* without weakening
/// this lemma. `pre_checkpoint_block_trust` lets a hash recorded by an earlier checkpoint
/// certificate bypass the already-processed skip; and a checkpoint block may install a state
/// snapshot rather than replay its ancestors. Neither bypasses `certificate.check`, so a block
/// entering the tip is always quorum-certified; what they bypass is the *re-execution* of
/// ancestors, which is a matter of state-transition correctness rather than of agreement.
///
/// [`ChainTipState::next_block_height`]: crate::ChainTipState::next_block_height
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`ConfirmedBlock`]: crate::block::ConfirmedBlock
/// [`ChainStateView::apply_confirmed_block`]: crate::ChainStateView::apply_confirmed_block
/// [`Committee`]: linera_execution::committee::Committee
/// [`check`]: crate::types::ConfirmedBlockCertificate::check
pub trait TipAdvancesOnlyOnValidCertificate {}

/// **Lemma (Every certified block was executed by a correct validator).** If a valid
/// [`ValidatedBlockCertificate`] for a block `B` exists, then some correct validator executed
/// `B`'s [`ProposedBlock`] itself and obtained `B`'s [`BlockExecutionOutcome`]. The same follows
/// for a [`ConfirmedBlockCertificate`] outside the fast round, by
/// [`CommitRestsOnValidation`]; inside the fast round it holds directly, a fast proposal carrying
/// no outcome.
///
/// This is what stands between the protocol and a committed block whose outcome is fabricated.
/// It is *not* accountability: a validator that votes for a mis-executed block leaves no
/// extractable proof (see [`AccountabilityScope`]). And unlike the results in
/// [`crate::justification::proof`], it needs [`MaxByzantineWeight`] — so validity, unlike
/// agreement, degrades above the fault bound with no forensic residue.
///
/// *Proof.* Induction on the certificate's round, well founded because rounds are totally
/// ordered. By [`CertificateCarriesCorrectVote`] some correct validator `v` cast a validation vote
/// for `B` in that round, so by [`ProposalGate`] it ran `ChainWorkerState::try_handle_block_proposal`
/// to acceptance on a proposal for `B`. That function computes the block as
///
/// ```text
/// let block = if let Some(outcome) = outcome { outcome.clone().with(proposal.content.block.clone()) }
///             else { self.execute_block(…).await? };
/// ```
///
/// and [`BlockProposal::check_invariants`] admits a carried `outcome` only together with an
/// [`OriginalProposal::Regular`] certificate. So:
///
/// * a **fresh** proposal and a **fast retry** both carry `outcome: None` and are therefore
///   executed by `v` itself — the base case;
/// * a **regular retry** is not re-executed, but `check_invariants` requires its certificate to
///   satisfy `certificate.check_value(&ValidatedBlock::new(outcome.with(block)))`, i.e. to certify
///   exactly this `B`, and `content.round > certificate.round`; the caller verified it with
///   `certificate.check(&committee)`. The induction hypothesis at that strictly lower round
///   supplies the correct validator that executed `B`. ∎
///
/// **All eight outputs, not just the state.** [`BlockHeader`] commits to each component of the
/// outcome separately — [`state_hash`], [`messages_hash`], [`events_hash`], [`blobs_hash`],
/// [`oracle_responses_hash`], [`operation_results_hash`], [`previous_message_blocks_hash`] and
/// [`previous_event_blocks_hash`] — and this lemma covers all of them, since the correct validator
/// computed the whole [`BlockExecutionOutcome`]. That matters because the components differ
/// sharply in reach: `state_hash` is local to the chain, whereas `messages` and `events` leave it
/// and are consumed by other chains, and `blobs` are content-addressed
/// ([`BlobId`](linera_base::identifiers::BlobId) is a hash of the content) and so are the only
/// component that is self-verifying without any execution at all.
///
/// **What this does not give.** The correct validator executed the proposal, but the execution
/// *replays* whatever oracle answers it recorded; a later re-execution of the confirmed block
/// feeds `outcome.oracle_responses` back in rather than re-deriving them. So the lemma certifies
/// that the outcome follows from the proposal *and those oracle answers*, not that the answers
/// were truthful. Oracle results are attested by quorum, which is inherent — they are not
/// reproducible functions of the chain state.
///
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`ProposedBlock`]: crate::data_types::ProposedBlock
/// [`BlockExecutionOutcome`]: crate::data_types::BlockExecutionOutcome
/// [`BlockProposal::check_invariants`]: crate::data_types::BlockProposal::check_invariants
/// [`OriginalProposal::Regular`]: crate::data_types::OriginalProposal::Regular
/// [`BlockHeader`]: crate::block::BlockHeader
/// [`state_hash`]: crate::block::BlockHeader::state_hash
/// [`messages_hash`]: crate::block::BlockHeader::messages_hash
/// [`events_hash`]: crate::block::BlockHeader::events_hash
/// [`blobs_hash`]: crate::block::BlockHeader::blobs_hash
/// [`oracle_responses_hash`]: crate::block::BlockHeader::oracle_responses_hash
/// [`operation_results_hash`]: crate::block::BlockHeader::operation_results_hash
/// [`previous_message_blocks_hash`]: crate::block::BlockHeader::previous_message_blocks_hash
/// [`previous_event_blocks_hash`]: crate::block::BlockHeader::previous_event_blocks_hash
/// [`MaxByzantineWeight`]: crate::manager::proof::model::MaxByzantineWeight
/// [`AccountabilityScope`]: crate::justification::proof::AccountabilityScope
pub trait CertifiedBlockWasExecuted:
    CertificateCarriesCorrectVote + ProposalGate + MaxByzantineWeight + CommitRestsOnValidation
{
}

/// **Lemma (Incoming bundles are matched against the validator's own inbox).** A correct
/// validator casts a validation or fast-confirmation vote for a block only if every
/// [`IncomingBundle`] the block consumes is already present in its own inbox for that origin, and
/// is *equal* to the bundle it holds there.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | `ChainStateView::remove_bundles_from_inboxes`, called from `ChainWorkerState::try_handle_block_proposal` with `must_be_present = true` |
/// | reads | the chain's inboxes, the block's timestamp and incoming bundles |
/// | writes | the inboxes (rolled back before voting — `try_handle_block_proposal` calls `chain.rollback()`) |
/// | precondition | none beyond [`ProposalGate`] |
///
/// *Proof.* `try_handle_block_proposal` calls
/// `remove_bundles_from_inboxes(block.timestamp, true, block.incoming_bundles())` before executing
/// and voting. For each bundle that helper calls `Inbox::remove_bundle` and, because
/// `must_be_present` is set, rejects with [`ChainError::MissingCrossChainUpdates`] unless it
/// returned `true` — which happens only on the branch that found a bundle already in
/// `added_bundles` and checked `bundle == &previous_bundle`. So a bundle the validator has not
/// received, or one that differs in any field from what it received, blocks the vote. ∎
///
/// **The flag is deliberately not set when applying a certified block.**
/// `ChainWorkerState::execute_contiguous_block` passes `must_be_present = false`, so a bundle that
/// has not arrived yet is recorded in `removed_bundles` and reconciled when it does. That is the
/// right asymmetry — by then a quorum has already voted, and this lemma has done its work at
/// voting time — but it means the guarantee lives in the *proposal* path only.
///
/// **What populates the inbox decides what this is worth.** Bundles enter through
/// `ChainWorkerState::process_cross_chain_update`, fed by the *same validator's* worker for the
/// sending chain. That worker derives them from the sending block's `messages` field, and whether
/// that field is the validator's own work depends on how it processed the sender:
///
/// * if it **executed** the sender's block, `execute_contiguous_block` re-executed it and rejected
///   a mismatch against the certificate ([`CertifiedBlockWasExecuted`] and the note there), so the
///   bundles are self-derived;
/// * if it only **preprocessed** the sender's block, `preprocess_certified_block` updated outboxes
///   and event streams *without executing*, so the bundles are taken from the sender's certificate
///   at face value.
///
/// So cross-chain integrity degrades with how much of the chain graph each validator executes —
/// a deployment property, not a protocol one. Under [`MaxByzantineWeight`] this is still sound,
/// since [`CertifiedBlockWasExecuted`] guarantees *some* correct validator executed the sending
/// block; above the fault bound it is not, and the resulting damage is not confined to one chain
/// (see [`AccountabilityScope`]).
///
/// [`IncomingBundle`]: crate::data_types::IncomingBundle
/// [`ChainError::MissingCrossChainUpdates`]: crate::ChainError::MissingCrossChainUpdates
/// [`MaxByzantineWeight`]: crate::manager::proof::model::MaxByzantineWeight
/// [`AccountabilityScope`]: crate::justification::proof::AccountabilityScope
pub trait IncomingBundlesAreSelfDerived: ProposalGate + CertifiedBlockWasExecuted {}
