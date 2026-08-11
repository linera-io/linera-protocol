// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Progress lemmas: the individual steps a correct driver can force after GST.
//!
//! Each result here says that one step of the protocol *completes*, given that the previous ones
//! did. They are assembled into the liveness theorems in [`super::liveness`].
//!
//! The driver is [`ChainClient::process_pending_block`], whose body
//! (`process_pending_block_inner`) performs, in order: request a timeout if the round has expired
//! ([`TimeoutCertificateForms`], [`RoundAdvancement`]); finalize a locking block already in the
//! current round; otherwise choose a block — the locking block if there is one
//! ([`LockRecovery`]) — and a round ([`EventuallyCorrectLeader`]); submit the proposal
//! ([`ProposalAccepted`], [`ValidationQuorumForms`]); and finalize it
//! ([`FinalizationQuorumForms`]).
//!
//! [`ChainClient::process_pending_block`]: crate::client::ChainClient::process_pending_block

use linera_chain::{
    data_types::proof::quorum::CorrectValidatorsFormQuorum,
    manager::proof::{
        commit::CommittedBlock,
        locking::{LockRoundMonotone, UniqueValidatedBlockPerRound},
        pacemaker::{
            LeaderEligibility, RoundsWithoutTimeout, SingleLeaderRoundsNeedTimeout,
            TimeoutCertificateAdvancesRound, TimeoutVoteConditions,
        },
        rounds::CurrentRoundMonotone,
        voting::{
            ConfirmationNeedsValidatedCertificate, ConfirmationOnlyInCurrentRound, ProposalGate,
            UnlockingRequiresHigherCertificate,
        },
    },
};

use super::assumptions::{
    ActiveCorrectDriver, ClockAccuracy, CorrectValidatorAvailability, EventualSynchrony,
    FullReachability, LeaderFairness, RoundTimeoutGrowth,
};

/// **Lemma (A timeout certificate forms).** Suppose that after GST every correct validator is in
/// the same round `r` at the chain's pending height, that `r` has a configured timeout
/// ([`RoundsWithoutTimeout`]), and that the timeout has elapsed on every correct validator's
/// clock. Then a correct driver's [`ChainClient::request_leader_timeout`] returns a valid
/// [`TimeoutCertificate`] for round `r` within `2Δ` plus local processing.
///
/// *Proof.* [`ChainClient::request_leader_timeout`] issues
/// `CommunicateAction::RequestTimeout { chain_id, height, round }` with `round` read from its
/// local [`ChainManagerInfo::current_round`] and `height` from `ChainInfo::next_block_height`,
/// through `Client::communicate_chain_action`. Each recipient runs
/// `ChainWorkerState::vote_for_leader_timeout`, which checks the height against
/// [`ChainTipState::next_block_height`] and calls
/// [`ChainManager::create_timeout_vote`]. By [`TimeoutVoteConditions`] its four conditions hold
/// under the hypotheses, so every correct validator signs; by
/// [`CorrectValidatorAvailability`] and [`EventualSynchrony`] every such vote arrives within Δ
/// of the request.
///
/// The votes aggregate: `communicate_with_quorum` groups by the full signed payload
/// `(value_hash, round, unlocking_round, first_round, justification_commitment)`, and every
/// timeout vote for this height carries the same `Timeout::new(chain_id, height, epoch)` value
/// — identical by [`ClockAccuracy`]-independent construction, since the epoch is the chain's —
/// with `unlocking_round: None`, `first_round: false` and no justification commitment. So all
/// correct votes land in one group, which by [`CorrectValidatorsFormQuorum`] reaches
/// [`quorum_threshold`], and `communicate_with_quorum` returns it. ∎
///
/// The hypothesis "every correct validator is in round `r`" is not free — see
/// [`RoundAdvancement`], which is what establishes it for the next round.
///
/// [`ChainClient::request_leader_timeout`]: crate::client::ChainClient::request_leader_timeout
/// [`TimeoutCertificate`]: linera_chain::types::TimeoutCertificate
/// [`ChainManagerInfo::current_round`]: linera_chain::manager::ChainManagerInfo::current_round
/// [`ChainTipState::next_block_height`]: linera_chain::ChainTipState::next_block_height
/// [`ChainManager::create_timeout_vote`]: linera_chain::manager::ChainManager::create_timeout_vote
/// [`quorum_threshold`]: linera_execution::committee::Committee::quorum_threshold
pub trait TimeoutCertificateForms:
    TimeoutVoteConditions
    + RoundsWithoutTimeout
    + CorrectValidatorsFormQuorum
    + CorrectValidatorAvailability
    + EventualSynchrony
    + ClockAccuracy
{
}

/// **Lemma (Round advancement).** After GST, a correct driver can bring every correct validator
/// into a common round strictly above `r`, within `O(Δ)`, provided `r` has a configured timeout.
/// Consequently the common round grows without bound as long as the driver keeps trying.
///
/// *Proof.* By [`TimeoutCertificateForms`] the driver obtains a [`TimeoutCertificate`] for `r`.
/// [`ChainClient::request_leader_timeout`] then feeds it to its own node and calls
/// `Client::communicate_chain_updates`, which delivers it to the validators; each correct
/// recipient runs `ChainWorkerState::process_timeout`, which verifies it against the committee
/// and calls [`ChainManager::handle_timeout_certificate`]. By
/// [`TimeoutCertificateAdvancesRound`] each then has a current round of at least
/// `ChainOwnership::next_round(r) > r`, and by [`CurrentRoundMonotone`] it stays there.
///
/// They are in a *common* round because [`RoundFloor`] makes the round a deterministic function
/// of the evidence held, and after this step every correct validator holds the same highest
/// timeout certificate — unless some hold additional evidence (a higher lock or proposal), which
/// only moves them higher, and which the driver's own synchronization
/// ([`FullReachability`]) then propagates. Unboundedness follows by induction, using
/// [`RoundTimeoutGrowth`] to know that each successive round again has a finite timeout. ∎
///
/// **This is strictly weaker than liveness.** It says rounds advance, not that a block is
/// committed; an execution in which the driver forever advances rounds without ever committing
/// satisfies this lemma. Turning it into progress is what [`RoundProgress`] does, and it needs
/// [`EventuallyCorrectLeader`] and [`LockRecovery`] besides.
///
/// [`TimeoutCertificate`]: linera_chain::types::TimeoutCertificate
/// [`ChainClient::request_leader_timeout`]: crate::client::ChainClient::request_leader_timeout
/// [`ChainManager::handle_timeout_certificate`]: linera_chain::manager::ChainManager::handle_timeout_certificate
/// [`RoundFloor`]: linera_chain::manager::proof::rounds::RoundFloor
/// [`RoundProgress`]: super::liveness::RoundProgress
pub trait RoundAdvancement:
    TimeoutCertificateForms
    + TimeoutCertificateAdvancesRound
    + CurrentRoundMonotone
    + RoundTimeoutGrowth
    + SingleLeaderRoundsNeedTimeout
{
}

/// **Lemma (Eventually a correct owner leads a round that starts after GST).** Under
/// [`ActiveCorrectDriver`], [`LeaderFairness`] and [`RoundAdvancement`], there are infinitely
/// many [`SingleLeader`] rounds after GST whose leader is the correct driver's owner.
///
/// *Proof.* By [`RoundAdvancement`] the common round grows without bound after GST, so infinitely
/// many single-leader rounds begin after GST — the round sequence passes through
/// `SingleLeader(0), SingleLeader(1), …` by [`ChainOwnership::next_round`], and only leaves them
/// for [`Validator`] rounds on `u32` overflow or via fallback. By [`LeaderFairness`] the driver's
/// owner is the leader of infinitely many of them, and by [`LeaderEligibility`] being the leader
/// is exactly what `ChainManager::can_propose` requires. ∎
///
/// In [`Validator`] rounds the same argument applies with the committee's account keys as the
/// owner set; the correct driver is then a validator operator's client.
///
/// [`SingleLeader`]: linera_base::data_types::Round::SingleLeader
/// [`Validator`]: linera_base::data_types::Round::Validator
/// [`ChainOwnership::next_round`]: linera_base::ownership::ChainOwnership::next_round
pub trait EventuallyCorrectLeader:
    RoundAdvancement + LeaderFairness + ActiveCorrectDriver + LeaderEligibility
{
}

/// **Lemma (Lock recovery).** Under [`FullReachability`], after a correct driver completes
/// [`ChainClient::synchronize_chain_state`] its local
/// [`ChainManagerInfo::requested_locking`] has a round at least as high as the
/// [`confirmed_vote`] round of every correct validator — and, if any correct validator holds a
/// lock at all, is a [`ValidatedBlockCertificate`] for the same block that the highest such
/// validator locked.
///
/// *Proof.* Two steps.
///
/// *Every correct validator's lock dominates its own confirmation.* If a correct validator's
/// [`confirmed_vote`] is in round `p`, then either `p` is [`Round::Fast`] — and by
/// [`FastConfirmationNeedsEmptyLock`] it then holds a `LockingBlock::Fast` at that same round —
/// or by [`ConfirmationNeedsValidatedCertificate`] it confirmed via
/// [`ChainManager::create_final_vote`], which installs the certificate as the lock *before*
/// signing ([`ConfirmationOnlyInCurrentRound`]). Either way its lock round is `≥ p`, and stays so
/// by [`LockRoundMonotone`].
///
/// *The driver collects the maximum.* `Client::synchronize_chain_state_from` reads each
/// validator's [`ChainManagerInfo`] with manager values, and for a
/// `LockingBlock::Regular(cert)` calls `try_process_locking_block_from`, which feeds the
/// certificate to the local node's `process_validated_block`; that calls
/// [`ChainManager::create_final_vote`], whose `update_locking` keeps the higher of the two by
/// [`LockRoundMonotone`]. A `LockingBlock::Fast` is instead replayed as a proposal. Iterating
/// over the validators reached — all of the correct ones, by [`FullReachability`] — leaves the
/// local lock at the maximum. That it is a certificate *for the locked block* is immediate,
/// since a lock *is* the certificate; and by [`UniqueValidatedBlockPerRound`] two correct
/// validators locked at the same round hold certificates for the same block. ∎
///
/// This is what makes [`ProposalAccepted`] possible: the driver re-proposes the block it just
/// recovered, so no correct validator's [`UnlockingRequiresHigherCertificate`] guard can reject
/// it.
///
/// [`ChainClient::synchronize_chain_state`]: crate::client::ChainClient::synchronize_chain_state
/// [`ChainManagerInfo`]: linera_chain::manager::ChainManagerInfo
/// [`ChainManagerInfo::requested_locking`]: linera_chain::manager::ChainManagerInfo::requested_locking
/// [`confirmed_vote`]: field@linera_chain::manager::ChainManager::confirmed_vote
/// [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`ChainManager::create_final_vote`]: linera_chain::manager::ChainManager::create_final_vote
/// [`FastConfirmationNeedsEmptyLock`]: linera_chain::manager::proof::voting::FastConfirmationNeedsEmptyLock
pub trait LockRecovery:
    FullReachability
    + ConfirmationNeedsValidatedCertificate
    + ConfirmationOnlyInCurrentRound
    + LockRoundMonotone
    + UniqueValidatedBlockPerRound
{
}

/// **Lemma (A recovered proposal is accepted).** Let `r` be a [`SingleLeader`] or [`Validator`]
/// round beginning after GST whose leader is the correct driver's owner
/// ([`EventuallyCorrectLeader`]), let every correct validator be in round `r`
/// ([`RoundAdvancement`]), and let the driver have completed lock recovery
/// ([`LockRecovery`]). Then the proposal the driver submits in round `r` passes
/// [`ChainManager::check_proposed_block`] at every correct validator.
///
/// *Proof.* By [`ProposalGate`] acceptance is exactly `check_proposed_block` returning
/// [`Accept`], so we take its guards in order, for a correct validator `v` in round `r`.
///
/// * *Proposer eligibility.* `try_handle_block_proposal` requires
///   `chain.manager.can_propose(&owner, r)`, which holds by [`LeaderEligibility`] since the
///   driver is `r`'s leader; the driver selects `r` through `ChainClient::round_for_new_proposal`,
///   which consults the same `ChainManagerInfo::should_propose`.
/// * *Round.* The `SingleLeader(_) | Validator(_)` arm requires `r == v.current_round()`, which
///   is the hypothesis.
/// * *Validation vote.* Requires `r > v.validated_vote.round`. By [`VoteRoundBelowCurrentRound`]
///   any earlier validation vote of `v` is in a round `≤ v.current_round() = r`, and `= r` is
///   excluded: a vote in round `r` needs a proposal in round `r` accepted by `v`, and by
///   [`LeaderEligibility`] the only proposer `v` accepts in `r` is the driver, which has made no
///   other proposal in `r`.
/// * *Lock.* Requires `r > v.locking_block.round()`. A lock round above `r` would by
///   [`RoundFloor`] put `v.current_round()` above `r`, contradicting the hypothesis; and a lock
///   round *equal* to `r` would require a [`ValidatedBlockCertificate`] in round `r`, which by
///   [`CertificateCarriesCorrectVote`] would require a correct validator's validation vote in
///   round `r` — excluded by the previous point.
/// * *Confirmed vote.* This is the one that needs [`LockRecovery`]. If `v` has a confirmed vote
///   in round `p`, then by [`LockRecovery`] the driver's recovered lock has round `t ≥ p` and
///   certifies the same block `B` that `v` confirmed if `t = p`. The driver proposes `B` as a
///   regular retry carrying that certificate (`process_pending_block_inner` takes the
///   `if let Some(locking) = info.manager.requested_locking` branch and builds
///   `BlockProposal::new_retry_regular`). By [`UnlockingRequiresHigherCertificate`] the guard
///   then requires `p ≤ t` when the blocks match, which holds. (When `t > p` and the blocks
///   differ, the guard requires `p < t`, which also holds.)
///
/// A `LockingBlock::Fast` lock is retried as a `BlockProposal::new_initial` carrying the fast
/// proposal's signature as its `owner_authorization`, and the same guard
/// requires `v.confirmed_vote.round.is_fast()` and a matching block, which holds because a fast
/// confirmation is the only confirmation possible below the fast round's successor. ∎
///
/// [`SingleLeader`]: linera_base::data_types::Round::SingleLeader
/// [`Validator`]: linera_base::data_types::Round::Validator
/// [`ChainManager::check_proposed_block`]: linera_chain::manager::ChainManager::check_proposed_block
/// [`Accept`]: linera_chain::manager::Outcome::Accept
/// [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate
/// [`VoteRoundBelowCurrentRound`]: linera_chain::manager::proof::rounds::VoteRoundBelowCurrentRound
/// [`CertificateCarriesCorrectVote`]: linera_chain::data_types::proof::quorum::CertificateCarriesCorrectVote
/// [`RoundFloor`]: linera_chain::manager::proof::rounds::RoundFloor
pub trait ProposalAccepted:
    EventuallyCorrectLeader
    + RoundAdvancement
    + LockRecovery
    + ProposalGate
    + UnlockingRequiresHigherCertificate
    + LeaderEligibility
{
}

/// **Lemma (The validation quorum forms).** Under the hypotheses of [`ProposalAccepted`], the
/// driver obtains a valid [`ValidatedBlockCertificate`] for its block in round `r` within `2Δ`
/// plus local processing.
///
/// *Proof.* By [`ProposalAccepted`] every correct validator accepts the proposal, so
/// `ChainWorkerState::try_handle_block_proposal` reaches
/// [`ChainManager::create_vote`], which — `r` not being the fast round — signs a validation vote
/// for the block in round `r`. Every such vote carries the same signed payload: same block hash,
/// same round, and the same `unlocking_round`/`justification_commitment` pair, which
/// [`ChainManager::create_vote`] derives from the proposal's own
/// [`ValidatedBlockCertificate`] — identical across validators because the proposal is. So all correct votes fall into one group of
/// `communicate_with_quorum`, which by [`CorrectValidatorsFormQuorum`] reaches the quorum
/// threshold; by [`CorrectValidatorAvailability`] and [`EventualSynchrony`] they arrive within Δ.
/// `Client::submit_block_proposal` assembles them into a certificate, whose justification chain
/// is the retried certificate's `full_justification`. ∎
///
/// Note the blob preconditions: a validator missing a blob the proposal requires answers
/// `WorkerError::BlobsNotFound` instead of voting. That does not cost a round. The retry is
/// *per-validator*, inside `RemoteNodeUpdater::send_block_proposal`'s loop: the arm matching
/// `BlobsNotFound | InactiveChain` sends the proposal's published blobs with `send_pending_blobs`
/// and re-submits to that validator alone, so the other validators' votes are unaffected and the
/// quorum round is not restarted. It terminates because the loop drains its `blob_ids` — the set
/// is fixed by the proposal and each pass takes it with `mem::take`. So the `2Δ` bound above
/// absorbs it as a constant factor rather than an extra round of the protocol.
///
/// [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate
/// [`ChainManager::create_vote`]: linera_chain::manager::ChainManager::create_vote
pub trait ValidationQuorumForms:
    ProposalAccepted + CorrectValidatorsFormQuorum + CorrectValidatorAvailability + EventualSynchrony
{
}

/// **Lemma (The finalization quorum forms).** Given a valid [`ValidatedBlockCertificate`] for
/// block `B` in round `r`, and every correct validator in round `r` after GST, the driver obtains
/// a valid [`ConfirmedBlockCertificate`] for `B` within `2Δ` plus local processing — so `B`
/// becomes a [`CommittedBlock`].
///
/// *Proof.* `Client::finalize_block` sends `CommunicateAction::FinalizeBlock` to every validator.
/// A correct recipient runs `ChainWorkerState::process_validated_block`, which verifies the
/// certificate, checks [`ChainManager::check_validated_block`] — whose guards are
/// `new_round ≥ validated_vote.round`, satisfied since no correct validator voted above `r`, and
/// `new_round > locking_block.round()`, satisfied since no lock in round `r` existed before this
/// certificate — and calls [`ChainManager::create_final_vote`]. That signs, because by
/// [`ConfirmationOnlyInCurrentRound`] it requires the current round to equal `r`, which holds by
/// hypothesis and is re-established by its own `update_locking`/`update_current_round` prelude.
///
/// All confirmation votes again share one payload: the value is `ConfirmedBlock::new(B)`, the
/// round is `r`, and the `first_round` flag and justification commitment are derived from `r` and
/// the certificate, identically at every validator. So they aggregate;
/// [`CorrectValidatorsFormQuorum`] gives the threshold. `Client::finalize_block` then attaches
/// the justification chain the quorum committed to and returns the certificate, which is a
/// [`CommittedBlock`] by definition. ∎
///
/// [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate
/// [`ConfirmedBlockCertificate`]: linera_chain::types::ConfirmedBlockCertificate
/// [`ChainManager::check_validated_block`]: linera_chain::manager::ChainManager::check_validated_block
/// [`ChainManager::create_final_vote`]: linera_chain::manager::ChainManager::create_final_vote
pub trait FinalizationQuorumForms:
    ValidationQuorumForms
    + ConfirmationOnlyInCurrentRound
    + ConfirmationNeedsValidatedCertificate
    + CommittedBlock
    + CorrectValidatorsFormQuorum
{
}
