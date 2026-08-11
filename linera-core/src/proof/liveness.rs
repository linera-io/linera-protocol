// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The liveness theorems, and a precise account of what they exclude.
//!
//! Everything here is conditional on the assumptions in [`super::assumptions`], all of which can
//! fail without endangering [`CommitAgreement`]. Read [`LivenessScope`] alongside
//! [`UnboundedProgress`]: several natural readings of "the protocol makes progress" are *not*
//! implied, and the differences are design choices rather than oversights.
//!
//! [`CommitAgreement`]: linera_chain::manager::proof::safety::CommitAgreement

use linera_chain::manager::proof::{
    commit::{CommittedBlock, TipAdvancesOnlyOnValidCertificate},
    pacemaker::RoundsWithoutTimeout,
    safety::{CommitAgreement, UniqueChain},
};

use super::{
    assumptions::ActiveCorrectDriver,
    progress::{
        EventuallyCorrectLeader, FinalizationQuorumForms, LockRecovery, ProposalAccepted,
        TimeoutCertificateForms, ValidationQuorumForms,
    },
};

/// **Theorem (Round progress).** Consider a chain with at least one regular owner, at a height
/// whose consensus instance has not yet committed. Under [`ActiveCorrectDriver`] and the other
/// assumptions of [`super::assumptions`], there is a round `r` beginning after GST such that the
/// height commits during `r`.
///
/// *Proof.* By [`RoundAdvancement`] the correct validators' common round grows without bound
/// after GST, and by [`RoundTimeoutGrowth`] the timeout of round number `n` is
/// `base_timeout + timeout_increment · n`, unbounded in `n`. Let `T` be the wall-clock time a
/// correct leader needs to complete one round after GST: by
/// [`ProposalAccepted`], [`ValidationQuorumForms`] and [`FinalizationQuorumForms`] this is
/// `O(Δ)` plus bounded local processing, hence finite. Choose `n` with
/// `base_timeout + timeout_increment · n > T`.
///
/// By [`EventuallyCorrectLeader`] there is a [`SingleLeader`] round `r` with number at least `n`,
/// beginning after GST, whose leader is the correct driver's owner. By [`RoundAdvancement`] every
/// correct validator is in `r` when it begins, and by [`LockRecovery`] the driver enters `r`
/// holding a lock at least as high as every correct validator's confirmation. Then:
///
/// 1. by [`ProposalAccepted`], every correct validator accepts the driver's proposal in `r`;
/// 2. by [`ValidationQuorumForms`], a [`ValidatedBlockCertificate`] for the proposed block forms
///    in round `r`;
/// 3. by [`FinalizationQuorumForms`], a [`ConfirmedBlockCertificate`] for it forms in round `r`.
///
/// All three complete within `T < ` the round's timeout, so no correct validator signs a timeout
/// vote for `r` in the meantime ([`TimeoutVoteConditions`]) and the round is not cut short. The
/// block is therefore a [`CommittedBlock`]. ∎
///
/// Step 3's precondition that no correct validator has left `r` is what the timeout comparison
/// buys: without [`RoundTimeoutGrowth`] the round could expire mid-flight, every attempt could
/// fail the same way, and rounds would advance forever without committing.
///
/// [`SingleLeader`]: linera_base::data_types::Round::SingleLeader
/// [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate
/// [`ConfirmedBlockCertificate`]: linera_chain::types::ConfirmedBlockCertificate
/// [`RoundTimeoutGrowth`]: super::assumptions::RoundTimeoutGrowth
/// [`TimeoutVoteConditions`]: linera_chain::manager::proof::pacemaker::TimeoutVoteConditions
/// [`RoundAdvancement`]: super::progress::RoundAdvancement
pub trait RoundProgress:
    EventuallyCorrectLeader
    + LockRecovery
    + ProposalAccepted
    + ValidationQuorumForms
    + FinalizationQuorumForms
    + TimeoutCertificateForms
    + CommittedBlock
{
}

/// **Theorem (Height progress).** Under the same assumptions, once the driver has a block to
/// propose at height `h`, a block at height `h` is committed within finite time after GST, and
/// every correct validator's [`ChainTipState::next_block_height`] reaches `h + 1` — provided it
/// is reachable and receives the certificate.
///
/// *Proof.* Commitment at `h` is [`RoundProgress`]. For the observable half: the driver's
/// `ChainClient::process_pending_block` ends by calling `Client::update_validators` with the new
/// [`ConfirmedBlockCertificate`], which delivers it to every validator; a correct recipient runs
/// `ChainWorkerState::process_confirmed_block`, which verifies it and — since the block is
/// contiguous with the recipient's tip, `h` being the pending height — executes it and advances
/// [`ChainTipState::next_block_height`] to `h + 1` ([`TipAdvancesOnlyOnValidCertificate`]). By
/// [`CommitAgreement`] the block it records is the same at all of them. ∎
///
/// The proviso is not removable and is not a defect: a validator that is partitioned away, or
/// that has been down since before GST, simply has not received the certificate yet. It catches
/// up through the ordinary certificate-download path, since the certificate is by then durable at
/// a quorum.
///
/// [`ChainTipState::next_block_height`]: linera_chain::ChainTipState::next_block_height
/// [`ConfirmedBlockCertificate`]: linera_chain::types::ConfirmedBlockCertificate
pub trait HeightProgress:
    RoundProgress + TipAdvancesOnlyOnValidCertificate + CommitAgreement
{
}

/// **Theorem (Unbounded progress).** If a correct driver ([`ActiveCorrectDriver`]) keeps
/// supplying blocks to propose — never exhausting its stream of operations — then under the
/// assumptions of [`super::assumptions`] every correct, reachable validator's
/// [`ChainTipState::next_block_height`] grows without bound, and by [`UniqueChain`] the
/// validators' committed prefixes remain identical throughout.
///
/// *Proof.* Induction on the height. Given that height `h` has committed and every correct
/// reachable validator has advanced to `h + 1` ([`HeightProgress`]), each such validator's
/// `ChainStateView::reset_chain_manager` has created the consensus instance for `h + 1`
/// ([`ConsensusInstance`]) with its round reset to
/// [`ChainOwnership::first_round`]. The hypotheses of [`RoundProgress`] then hold again at
/// `h + 1`: GST has passed, the driver has a block, and [`EventuallyCorrectLeader`] applies to
/// the fresh instance since the leader seed is the new height. So `h + 1` commits, and the
/// induction continues. Uniqueness of what is committed at each height is [`UniqueChain`]. ∎
///
/// Note that the round numbering restarts at each height, so the round-timeout argument of
/// [`RoundProgress`] restarts too: each height may again spend a bounded number of rounds before
/// its timeout exceeds `T`. This costs latency, not liveness.
///
/// [`ChainTipState::next_block_height`]: linera_chain::ChainTipState::next_block_height
/// [`ConsensusInstance`]: linera_chain::manager::proof::model::ConsensusInstance
/// [`ChainOwnership::first_round`]: linera_base::ownership::ChainOwnership::first_round
pub trait UnboundedProgress: HeightProgress + UniqueChain {}

/// **Remark (What liveness does not claim).** Five exclusions, each of which a reader may
/// reasonably expect [`UnboundedProgress`] to cover.
///
/// * **No progress without a client.** [`ActiveCorrectDriver`] is indispensable: a validator
///   never proposes and never advances a round unprompted. A microchain whose owners have all
///   gone away makes no progress and is not thereby faulty. This is the deepest structural
///   difference from a validator-driven BFT protocol, and it is what makes a Linera validator's
///   per-chain cost proportional to use.
///
/// * **The fast round can wedge a height permanently.** By [`RoundsWithoutTimeout`], with the
///   default [`TimeoutConfig`] the fast round has no timeout at all, and while the current round
///   is fast only a super owner may open a later one. A super owner that issues two conflicting
///   fast proposals splits the correct validators — each locks onto the first it sees, by
///   [`FastConfirmationNeedsEmptyLock`] — so neither reaches a quorum, and the height stops until
///   that same super owner proposes again in a higher round. [`CommitAgreement`] is untouched;
///   this is exactly the trade the fast path makes, and the reason
///   `ChainClient::process_pending_block_inner` refuses to replace a pending fast proposal whose
///   signing key it no longer holds.
///
/// * **Multi-leader rounds are not covered by [`RoundProgress`].** The theorem is stated for
///   [`SingleLeader`] and [`Validator`] rounds, where a unique leader is guaranteed by
///   [`LeaderEligibility`]. In a multi-leader round several owners may propose simultaneously;
///   correct validators then vote for whichever proposal they see first, quorums may not form,
///   and by [`RoundsWithoutTimeout`] a non-final multi-leader round has no timeout — it is left
///   only by a proposal in a higher round. This is by design (multi-leader rounds are the
///   uncontended fast path) and costs nothing, because the round sequence passes through them
///   into single-leader rounds where the theorem applies.
///
/// * **No bound on the number of rounds, and no bound before GST.** [`RoundProgress`] asserts the
///   existence of a successful round, not a bound on how many precede it. Faulty leaders and
///   pre-GST delays each cost rounds.
///
/// * **Nothing is claimed about the *content* of the committed blocks.** A driver competing with
///   other owners may find its own block superseded: [`LockRecovery`] obliges it to re-propose
///   whatever is locked rather than its own pending block. `process_pending_block` then returns
///   the committed certificate for the other block and keeps the driver's proposal pending for a
///   later height. Liveness of the *chain* does not imply liveness of any particular
///   transaction.
///
/// [`SingleLeader`]: linera_base::data_types::Round::SingleLeader
/// [`Validator`]: linera_base::data_types::Round::Validator
/// [`TimeoutConfig`]: linera_base::ownership::TimeoutConfig
/// [`FastConfirmationNeedsEmptyLock`]: linera_chain::manager::proof::voting::FastConfirmationNeedsEmptyLock
/// [`LeaderEligibility`]: linera_chain::manager::proof::pacemaker::LeaderEligibility
pub trait LivenessScope:
    UnboundedProgress + RoundsWithoutTimeout + ActiveCorrectDriver + LockRecovery
{
}
