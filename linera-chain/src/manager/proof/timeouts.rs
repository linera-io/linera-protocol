// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Leaders, round timeouts, and how a height leaves a round it cannot finish in.
//!
//! Nothing here is needed for safety — [`CommitAgreement`] holds however rounds advance, and
//! indeed whether or not they advance at all. These results exist to be consumed by the progress
//! lemmas in `linera_core::proof::progress`, and to record precisely which rounds can be left by
//! which means. The last statement, [`RoundsWithoutTimeout`], is a caveat rather than a
//! guarantee, and is the main reason the liveness theorem is conditional in the way it is.
//!
//! **Round advancement is not autonomous.** A validator never advances a round on its own initiative:
//! it signs a timeout vote only when a client asks it to, through
//! `ChainInfoQuery::request_leader_timeout`, and it never proposes a block at all. Round
//! advancement is therefore driven entirely from `linera_core::client`, which is why the
//! corresponding assumption (`ActiveCorrectDriver`) lives in that crate.
//!
//! [`CommitAgreement`]: crate::manager::proof::safety::CommitAgreement

use crate::{
    data_types::proof::quorum::CertificateCarriesCorrectVote,
    manager::proof::{
        model::ConsensusInstance,
        rounds::{CurrentRoundMonotone, RoundFloor},
    },
};

/// **Lemma (Timeout vote conditions).** A correct validator signs a
/// [`Timeout`](crate::block::Timeout) vote in round `r` only if, at that moment:
///
/// 1. `r` equals its [`ChainManager::current_round`];
/// 2. its [`round_timeout`] is `Some(t)` with `local_time ≥ t`;
/// 3. it has not already signed a timeout vote in round `r`;
///
/// and the value it signs is `Timeout::new(chain_id, height, epoch)` for the chain's pending
/// height and current epoch.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | [`ChainManager::create_timeout_vote`] |
/// | reads | [`current_round`], [`round_timeout`], [`timeout_vote`] |
/// | writes | [`timeout_vote`] |
/// | precondition | `height` equals `ChainTipState::next_block_height`, checked by the caller `ChainWorkerState::vote_for_leader_timeout` |
///
/// *Proof.* Direct reading of [`ChainManager::create_timeout_vote`]: it returns `Ok(false)`
/// without a key pair, then `ensure!(round == self.current_round())`, then
/// `let Some(round_timeout) = *self.round_timeout.get() else { return Err(RoundDoesNotTimeOut) }`,
/// then `ensure!(local_time >= round_timeout)`, then returns `Ok(false)` if
/// `timeout_vote.round == round`. Only after all four does it construct the vote. ∎
///
/// [`ChainManager::create_timeout_vote`]: crate::manager::ChainManager::create_timeout_vote
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`current_round`]: field@crate::manager::ChainManager::current_round
/// [`round_timeout`]: crate::manager::ChainManager::round_timeout
/// [`timeout_vote`]: crate::manager::ChainManager::timeout_vote
pub trait TimeoutVoteConditions {}

/// **Lemma (A timeout certificate proves a correct validator's round expired).** If a valid
/// [`TimeoutCertificate`] is certified in round `r` for a chain and height, then some correct
/// validator was in round `r` at that height, with a configured round timeout that had elapsed.
///
/// *Proof.* By [`CertificateCarriesCorrectVote`] a correct validator cast a timeout vote with
/// the certificate's payload, whose round is `r`. By [`TimeoutVoteConditions`] its
/// [`ChainManager::current_round`] was `r` and its [`round_timeout`] had elapsed. ∎
///
/// This is what makes a timeout certificate meaningful rather than merely well-formed: it cannot
/// be manufactured by faulty validators ahead of time, because a quorum contains a correct
/// validator whose own clock had to have passed the deadline.
///
/// [`TimeoutCertificate`]: crate::types::TimeoutCertificate
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`round_timeout`]: crate::manager::ChainManager::round_timeout
pub trait TimeoutCertificateProvesRoundReached:
    TimeoutVoteConditions + CertificateCarriesCorrectVote
{
}

/// **Lemma (A timeout certificate advances the round).** After a correct validator processes a
/// valid [`TimeoutCertificate`] for round `r` at its pending height, its
/// [`ChainManager::current_round`] is at least `ChainOwnership::next_round(r)`, or
/// `Round::Validator(u32::MAX)` if that is `None`.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | [`ChainManager::handle_timeout_certificate`] |
/// | reads | [`timeout`] |
/// | writes | [`timeout`], and via `update_current_round`: [`current_round`], [`round_timeout`] |
/// | precondition | `certificate.check(committee)`, epoch equality and pending-height equality, all checked by `ChainWorkerState::process_timeout` |
///
/// *Proof.* [`ChainManager::handle_timeout_certificate`] returns early when the stored
/// [`timeout`] is already in a round `≥ r`; otherwise it stores the certificate and calls
/// `update_current_round`. In the first case the stored certificate's round `r' ≥ r`, and a
/// previous application of this lemma already raised the round to `next_round(r') ≥ next_round(r)`
/// by monotonicity of [`ChainOwnership::next_round`] together with [`CurrentRoundMonotone`]. In
/// the second, [`RoundFloor`] includes `next_round(timeout.round)` in the maximum. ∎
///
/// [`TimeoutCertificate`]: crate::types::TimeoutCertificate
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`ChainManager::handle_timeout_certificate`]: crate::manager::ChainManager::handle_timeout_certificate
/// [`ChainOwnership::next_round`]: linera_base::ownership::ChainOwnership::next_round
/// [`timeout`]: crate::manager::ChainManager::timeout
/// [`current_round`]: field@crate::manager::ChainManager::current_round
/// [`round_timeout`]: crate::manager::ChainManager::round_timeout
pub trait TimeoutCertificateAdvancesRound: RoundFloor + CurrentRoundMonotone {}

/// **Lemma (Which rounds can be skipped without a timeout).** For a correct validator:
///
/// * a [`SingleLeader`] round above `SingleLeader(0)`, and any [`Validator`] round, is left only
///   via a [`TimeoutCertificate`] or a locking block in a higher round;
/// * [`Round::Fast`] and [`MultiLeader`] rounds are additionally left by any authenticated
///   proposal in a higher round.
///
/// *Proof.* By [`RoundFloor`] the round can rise only from a timeout certificate, the lock, or
/// [`proposed`] / [`signed_proposal`]. For the proposal inputs:
///
/// * [`ChainManager::update_signed_proposal`] returns `false` immediately for
///   `proposal.content.round > Round::SingleLeader(0)`, so [`signed_proposal`] never carries a
///   higher round;
/// * [`proposed`] is written only by the private `update_proposed`, called from
///   [`ChainManager::create_vote`], which by [`ProposalGate`] runs only after
///   [`ChainManager::check_proposed_block`] returned `Accept`; and that method requires
///   `new_round == current_round` on the `Round::SingleLeader(_) | Round::Validator(_)` arm, so
///   it cannot raise the round either. On the `MultiLeader(_) | SingleLeader(0)` arm it requires
///   only `new_round >= current_round`, which can raise it.
///
/// The lock case is not an exception to the intent: a [`LockingBlock::Regular`] in round `r` is a
/// [`ValidatedBlockCertificate`], hence by [`CertificateCarriesCorrectVote`] evidence that a
/// quorum — including a correct validator — was already in round `r`. So the validator is
/// following the round the chain has demonstrably reached, not being pushed past a leader's turn.
/// ∎
///
/// [`SingleLeader`]: linera_base::data_types::Round::SingleLeader
/// [`Validator`]: linera_base::data_types::Round::Validator
/// [`MultiLeader`]: linera_base::data_types::Round::MultiLeader
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`TimeoutCertificate`]: crate::types::TimeoutCertificate
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`LockingBlock::Regular`]: crate::manager::LockingBlock::Regular
/// [`ChainManager::update_signed_proposal`]: crate::manager::ChainManager::update_signed_proposal
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ProposalGate`]: crate::manager::proof::voting::ProposalGate
/// [`proposed`]: crate::manager::ChainManager::proposed
/// [`signed_proposal`]: crate::manager::ChainManager::signed_proposal
pub trait SingleLeaderRoundsNeedTimeout: RoundFloor + CertificateCarriesCorrectVote {}

/// **Lemma (Leader eligibility).** In a [`SingleLeader`] or [`Validator`] round, exactly one
/// owner may propose, namely `ChainManager::round_leader(round)`; in a [`MultiLeader`] round any
/// chain owner may (or anyone, when
/// [`open_multi_leader_rounds`](linera_base::ownership::ChainOwnership::open_multi_leader_rounds));
/// in [`Round::Fast`] only a super owner may. A super owner may additionally propose in any
/// non-[`Validator`] round.
///
/// The leader of round `n` is drawn by seeding a [`ChaCha8Rng`](rand_chacha::ChaCha8Rng) with
/// `u64::from(n).rotate_left(32) + seed` and sampling the stake-weighted
/// [`WeightedAliasIndex`](rand_distr::WeightedAliasIndex) built from the owners — or, for a
/// [`Validator`] round, from [`fallback_owners`], which
/// [`ChainManager::reset`] populates with the committee's account keys and weights. The seed is
/// the block height, so the leader schedule is fixed per instance and identical at every correct
/// validator.
///
/// *Proof.* [`ChainManager::can_propose`] returns `!round.is_validator()` for a super owner, and
/// otherwise dispatches: `false` for [`Round::Fast`],
/// `ownership.can_propose_in_multi_leader_round(owner)` for [`MultiLeader`], and
/// `self.round_leader(round) == Some(owner)` for the other two. It is enforced at the entry
/// point: `ChainWorkerState::try_handle_block_proposal` rejects a proposal with
/// `WorkerError::InvalidOwner` unless `chain.manager.can_propose(&owner, proposal.content.round)`,
/// where `owner` is recovered from the proposal's signature. The leader computation is the
/// private `compute_round_leader` / `round_leader_index` pair. ∎
///
/// [`SingleLeader`]: linera_base::data_types::Round::SingleLeader
/// [`Validator`]: linera_base::data_types::Round::Validator
/// [`MultiLeader`]: linera_base::data_types::Round::MultiLeader
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`fallback_owners`]: crate::manager::ChainManager::fallback_owners
/// [`ChainManager::reset`]: crate::manager::ChainManager::reset
/// [`ChainManager::can_propose`]: crate::manager::ChainManager::can_propose
pub trait LeaderEligibility {}

/// **Lemma (Fallback).** [`ChainManager::vote_fallback`] signs a timeout vote in the fixed round
/// `Round::SingleLeader(u32::MAX)`, at most once per instance, and only while
/// [`ChainManager::current_round`] is below `Round::Validator(0)`. A quorum of such votes forms a
/// [`TimeoutCertificate`] whose `next_round` is `Round::Validator(0)`, moving the height into
/// validator-led rounds.
///
/// *Proof.* The method returns `false` if [`fallback_vote`] is already set or
/// `current_round >= Round::Validator(0)`, and otherwise signs `Timeout::new(chain_id, height,
/// epoch)` at `Round::SingleLeader(u32::MAX)`. By [`ChainOwnership::next_round`], the successor
/// of `SingleLeader(r)` is `SingleLeader(r + 1)` unless that overflows, and it does at
/// `u32::MAX`, so the successor is `Round::Validator(0)`; combine with
/// [`TimeoutCertificateAdvancesRound`]. ∎
///
/// Note what is *not* checked here, unlike [`TimeoutVoteConditions`]: there is no
/// [`round_timeout`] comparison. The precondition is external — `ChainWorkerState::
/// vote_for_fallback` only calls it after reading the admin chain's epoch event and confirming
/// that `fallback_duration` has elapsed since the next epoch was created. The method's own
/// documentation states this obligation.
///
/// [`ChainManager::vote_fallback`]: crate::manager::ChainManager::vote_fallback
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`TimeoutCertificate`]: crate::types::TimeoutCertificate
/// [`fallback_vote`]: crate::manager::ChainManager::fallback_vote
/// [`round_timeout`]: crate::manager::ChainManager::round_timeout
/// [`ChainOwnership::next_round`]: linera_base::ownership::ChainOwnership::next_round
pub trait FallbackVote: TimeoutCertificateAdvancesRound + ConsensusInstance {}

/// **Caveat (Rounds that never time out).** [`ChainOwnership::round_timeout`] returns `None` —
/// so that [`ChainManager::create_timeout_vote`] fails with
/// [`ChainError::RoundDoesNotTimeOut`] and no timeout certificate can ever form — in three cases:
///
/// | round | `round_timeout` is `None` when |
/// |---|---|
/// | [`Round::Fast`] | `timeout_config.fast_round_duration` is `None`, **which is the default**, or `owners` is empty |
/// | [`MultiLeader(r)`] | `r + 1 != multi_leader_rounds`, i.e. every multi-leader round but the last |
/// | [`SingleLeader`], [`Validator`] | never |
///
/// Consequences, which the liveness argument must and does respect:
///
/// * A non-final multi-leader round is left only by a proposal in a higher round
///   ([`SingleLeaderRoundsNeedTimeout`]). This is by design — multi-leader rounds are skippable —
///   but it means "wait for the timeout" is not a strategy there.
/// * With a super owner and the default [`TimeoutConfig`], the fast round has **no timeout at
///   all**. If the super owner issues two conflicting fast proposals, correct validators split
///   between them ([`FastConfirmationNeedsEmptyLock`] pins each to the first it sees), neither
///   reaches a quorum, and — since only a super owner may open a later round while the current
///   round is fast, by the `is_super(&proposal.owner()) || !current_round.is_fast()` guard in
///   [`ChainManager::check_proposed_block`] — the height cannot progress until that same super
///   owner proposes again in a higher round. A super owner that stops there wedges the chain
///   permanently. This is the caveat the module documentation of [`crate::manager`] states as
///   "super owners must be careful to make only one block proposal", stated precisely.
///
/// Both are liveness properties; [`CommitAgreement`](crate::manager::proof::safety::CommitAgreement)
/// is unaffected.
///
/// [`ChainOwnership::round_timeout`]: linera_base::ownership::ChainOwnership::round_timeout
/// [`ChainManager::create_timeout_vote`]: crate::manager::ChainManager::create_timeout_vote
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`ChainError::RoundDoesNotTimeOut`]: crate::ChainError::RoundDoesNotTimeOut
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`MultiLeader(r)`]: linera_base::data_types::Round::MultiLeader
/// [`SingleLeader`]: linera_base::data_types::Round::SingleLeader
/// [`Validator`]: linera_base::data_types::Round::Validator
/// [`TimeoutConfig`]: linera_base::ownership::TimeoutConfig
/// [`FastConfirmationNeedsEmptyLock`]: crate::manager::proof::voting::FastConfirmationNeedsEmptyLock
pub trait RoundsWithoutTimeout: TimeoutVoteConditions + SingleLeaderRoundsNeedTimeout {}

/// **Remark (In a multi-leader round, the round a validator is in is its own).** Above the fast
/// round, the protocol has two ways to leave a round, and they differ in kind rather than only in
/// trigger. A [`TimeoutCertificate`] is quorum-signed, compact, and convinces anyone who receives
/// it. A skipped multi-leader round leaves no artifact at all: by
/// [`SingleLeaderRoundsNeedTimeout`] it ends when *some owner* proposes higher, which is a
/// unilateral act, and by [`RoundsWithoutTimeout`] no timeout certificate for it can ever exist.
///
/// Three consequences run through the rest of the specification.
///
/// *Correct validators legitimately disagree about the round.* A validator's
/// [`current_round`](crate::manager::ChainManager::current_round) is a maximum over what it has
/// happened to receive, so in the multi-leader regime two correct validators can sit in different
/// rounds with neither being behind in any blameable sense. This is why
/// [`ChainManager::check_proposed_block`] accepts `new_round >= current_round` there while
/// demanding equality in single-leader and validator rounds — the weaker test is what makes
/// disagreement survivable. It is also why the progress lemmas open by *assuming* every correct
/// validator is in the same round: in this regime that is a real hypothesis, not a state the
/// protocol reaches by itself.
///
/// *Catching a validator up costs more than a certificate.* A validator that missed the proposal
/// which ended a round cannot be handed a proof of that fact, because none exists. It has to be
/// sent the chain information itself — which is exactly why `WrongRound` appears as its own class,
/// with its own push route, in `linera_core::proof::availability::MissingDependenciesAreRecoverable`,
/// and why that class is the one where the *requester* may turn out to be the party that is
/// behind. Were every round to end in a timeout certificate, that class would collapse to
/// forwarding one certificate.
///
/// *Advancing the round and unlocking must key on different evidence.* Since one owner can raise
/// the round, the round must never by itself license abandoning a lock — otherwise an owner could
/// discard a locked block by proposing higher. So [`UnlockingRequiresHigherCertificate`] keys
/// unlocking on a [`ValidatedBlockCertificate`] from a higher round, evidence that a quorum moved,
/// and never on `current_round`. The two questions "which round am I in" and "what may I abandon"
/// are deliberately kept apart, and the safety argument depends on their separation:
/// [`SingleLeaderRoundsNeedTimeout`] makes the same point for the lock as a round *input*, where a
/// [`LockingBlock::Regular`] raises the round only because it is itself quorum evidence.
///
/// [`TimeoutCertificate`]: crate::types::TimeoutCertificate
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`UnlockingRequiresHigherCertificate`]: crate::manager::proof::voting::UnlockingRequiresHigherCertificate
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`LockingBlock::Regular`]: crate::manager::LockingBlock::Regular
pub trait MultiLeaderRoundsAreLocal: SingleLeaderRoundsNeedTimeout + RoundsWithoutTimeout {}
