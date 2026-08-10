// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Voting rules: what a correct validator's state must look like for it to sign.
//!
//! These are *local implementation properties*. Each one follows from a single method's control
//! flow together with the guards its call sites apply, with no induction over executions. They
//! are what [`crate::manager::proof::locking`] inducts over, and what
//! [`CertificateCarriesCorrectVote`] converts into constraints on the certificates that can
//! exist at all.
//!
//! Every statement below is accompanied by a *code correspondence* table naming the method that
//! implements the transition, the fields it reads and writes, and the preconditions its callers
//! establish.
//!
//! [`CertificateCarriesCorrectVote`]: crate::data_types::proof::quorum::CertificateCarriesCorrectVote

use crate::manager::proof::model::{CorrectValidator, SerializedChainState};

/// **Lemma (Vote construction sites).** A correct validator signs a block-related vote only in
/// [`ChainManager::create_vote`] and [`ChainManager::create_final_vote`], and a timeout vote only
/// in [`ChainManager::create_timeout_vote`] and [`ChainManager::vote_fallback`]. Specifically:
///
/// | vote kind | round of the vote | sole producer |
/// |---|---|---|
/// | [`Validated`] | `proposal.content.round`, never [`Round::Fast`] | [`ChainManager::create_vote`], `else` branch |
/// | [`Confirmed`] | [`Round::Fast`] | [`ChainManager::create_vote`], `if round.is_fast()` branch |
/// | [`Confirmed`] | `validated.round` | [`ChainManager::create_final_vote`] |
/// | [`Timeout`] | [`ChainManager::current_round`] | [`ChainManager::create_timeout_vote`] |
/// | [`Timeout`] | `Round::SingleLeader(u32::MAX)` | [`ChainManager::vote_fallback`] |
///
/// *Proof.* By [`ValidatorVote`], a vote exists only if one of [`Vote::new`],
/// [`Vote::new_with_unlocking_round`] or [`Vote::new_with_first_round`] was called with the
/// validator's key. Outside test-only code there are exactly five such calls in the workspace,
/// all in `linera_chain::manager`, and they are the five rows above: `Vote::new` in
/// [`create_timeout_vote`] and in [`vote_fallback`]; `Vote::new_with_first_round` in the
/// fast-round branch of [`create_vote`] and in [`create_final_vote`];
/// `Vote::new_with_unlocking_round` in the non-fast branch of [`create_vote`]. In each case the
/// round passed is the one tabulated: [`create_vote`] passes `proposal.content.round`, guarded
/// by `round.is_fast()` into one branch or the other; [`create_final_vote`] passes
/// `validated.round`; [`create_timeout_vote`] passes its `round` argument, which it has just
/// checked to equal [`ChainManager::current_round`]; [`vote_fallback`] passes the constant
/// `Round::SingleLeader(u32::MAX)`. By [`CorrectValidator`] no other code path of a correct
/// validator holds its key. ∎
///
/// **Where this is fragile.** The claim is an exhaustive-search argument over call sites, so it
/// is invalidated by adding a sixth call. A new signing site must either be shown to preserve
/// [`OneValidationVotePerRound`] and [`OneConfirmationVotePerRound`], or be added to this table.
///
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::create_timeout_vote`]: crate::manager::ChainManager::create_timeout_vote
/// [`ChainManager::vote_fallback`]: crate::manager::ChainManager::vote_fallback
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`create_vote`]: crate::manager::ChainManager::create_vote
/// [`create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`create_timeout_vote`]: crate::manager::ChainManager::create_timeout_vote
/// [`vote_fallback`]: crate::manager::ChainManager::vote_fallback
/// [`Validated`]: crate::types::CertificateKind::Validated
/// [`Confirmed`]: crate::types::CertificateKind::Confirmed
/// [`Timeout`]: crate::types::CertificateKind::Timeout
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`ValidatorVote`]: crate::data_types::proof::objects::ValidatorVote
/// [`Vote::new`]: crate::data_types::Vote::new
/// [`Vote::new_with_unlocking_round`]: crate::data_types::Vote::new_with_unlocking_round
/// [`Vote::new_with_first_round`]: crate::data_types::Vote::new_with_first_round
/// [`OneValidationVotePerRound`]: crate::manager::proof::locking::OneValidationVotePerRound
/// [`OneConfirmationVotePerRound`]: crate::manager::proof::locking::OneConfirmationVotePerRound
pub trait VoteConstructionSites: CorrectValidator {}

/// **Lemma (Proposal gate).** A correct validator reaches [`ChainManager::create_vote`] for a
/// proposal `p` only after [`ChainManager::check_proposed_block`] returned
/// [`Outcome::Accept`] for `p` against the same manager state.
///
/// Similarly, it reaches [`ChainManager::create_final_vote`] for a certificate `c` only after
/// [`ChainManager::check_validated_block`] returned [`Outcome::Accept`] for `c`, and after
/// `c.check(committee)` succeeded.
///
/// *Proof.* Both methods have exactly one caller in the workspace outside tests, in
/// `linera_core::chain_worker::state`:
///
/// * `create_vote` is called at the end of `ChainWorkerState::try_handle_block_proposal`, which
///   earlier `match`es on `chain.manager.check_proposed_block(&proposal)` and returns without
///   voting on both non-`Accept` arms — `Ok(Outcome::Skip)` returns the unchanged chain info,
///   and `Err(_)` returns the error (after, at most, recording the proposal via
///   [`ChainManager::update_signed_proposal`], which casts no vote).
/// * `create_final_vote` is called at the end of `ChainWorkerState::process_validated_block`,
///   which earlier evaluates `certificate.check(&committee)?` and then
///   `should_skip_validated_block()?`, a closure wrapping
///   `chain.manager.check_validated_block(&certificate)`. A `Skip` outcome returns early; an
///   `Err` propagates via `?`. Only `Ok(Accept)` falls through.
///
/// By [`SerializedChainState`] no other transition on this instance interleaves, so the state
/// the guard inspected is the state `create_vote` / `create_final_vote` then mutates. ∎
///
/// **Where this is fragile.** The guards are at the call sites, not inside the signing methods:
/// `create_final_vote` in particular re-checks only [`ChainManager::current_round`], and would
/// happily sign a second confirmation in the same round if invoked directly. A new caller must
/// replicate the guards. This is the single largest gap between "the module is correct" and "the
/// module cannot be misused".
///
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`ChainManager::check_validated_block`]: crate::manager::ChainManager::check_validated_block
/// [`ChainManager::update_signed_proposal`]: crate::manager::ChainManager::update_signed_proposal
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`Outcome::Accept`]: crate::manager::Outcome::Accept
pub trait ProposalGate: SerializedChainState {}

/// **Lemma (Validation rounds strictly increase).** If a correct validator's
/// [`validated_vote`] holds a vote in round `s`, it casts no further validation vote in any
/// round `≤ s` while that field still holds that vote.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | [`ChainManager::check_proposed_block`] |
/// | reads | [`proposed`], [`validated_vote`], [`locking_block`], [`confirmed_vote`], [`current_round`], [`ownership`] |
/// | writes | nothing |
/// | precondition | none |
/// | establishes | this lemma, [`UnlockingRequiresHigherCertificate`] |
///
/// *Proof.* By [`ProposalGate`] a validation vote in round `r` requires
/// [`ChainManager::check_proposed_block`] to have returned `Accept` for a proposal in round `r`.
/// That method contains
///
/// ```text
/// if let Some(vote) = self.validated_vote() {
///     ensure!(new_round > vote.round, ChainError::InsufficientRoundStrict(vote.round));
/// }
/// ```
///
/// so `Accept` with `validated_vote == Some(_, s)` requires `r > s`. ∎
///
/// Note the qualifier "while that field still holds that vote":
/// [`ChainManager::create_final_vote`] clears [`validated_vote`], so this lemma alone does not
/// give a per-round bound over the whole life of an instance. [`CastValidationRoundFloor`]
/// supplies what is missing.
///
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`validated_vote`]: field@crate::manager::ChainManager::validated_vote
/// [`confirmed_vote`]: field@crate::manager::ChainManager::confirmed_vote
/// [`current_round`]: field@crate::manager::ChainManager::current_round
/// [`locking_block`]: crate::manager::ChainManager::locking_block
/// [`proposed`]: crate::manager::ChainManager::proposed
/// [`ownership`]: crate::manager::ChainManager::ownership
/// [`UnlockingRequiresHigherCertificate`]: self::UnlockingRequiresHigherCertificate
/// [`CastValidationRoundFloor`]: crate::manager::proof::locking::CastValidationRoundFloor
pub trait ValidationRoundStrictlyIncreases: ProposalGate {}

/// **Lemma (A validation vote past a lock needs a higher certificate).** Suppose a correct
/// validator casts a validation vote for block `B` in round `r`, and let `(A, p)` be the value
/// of its [`confirmed_vote`] immediately before. Then `p` is defined only if the proposal is a
/// retry — carrying a [`ValidatedBlockCertificate`], or an [`OwnerAuthorization`] from
/// [`Round::Fast`] *strictly below* the proposal's own round — and:
///
/// * if the proposal is a **regular retry** carrying a certificate `c` (necessarily a valid
///   [`ValidatedBlockCertificate`] for `B`, in a round `c.round < r`), then
///   `p ≤ c.round` when `A` matches `B`'s proposal, and `p < c.round` otherwise;
/// * if the proposal is a **fast retry**, then `p` is [`Round::Fast`] and `A` matches `B`'s
///   proposal;
/// * a **fresh** proposal is rejected outright.
///
/// This is the hinge of the safety argument: it says a correct validator abandons a block it has
/// confirmed only when shown a quorum that validated the new block in a round *strictly above*
/// its own confirmation.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | [`ChainManager::check_proposed_block`], final `ensure!` |
/// | reads | [`confirmed_vote`], `proposal.validated_certificate`, `proposal.owner_authorization` |
/// | writes | nothing |
/// | precondition | the proposal passed `check_invariants`, `check_signature` and — for a regular retry — `certificate.check(committee)`, all in `try_handle_block_proposal` |
///
/// *Proof.* By [`ProposalGate`], `Accept` was returned, so the final `ensure!` of
/// [`ChainManager::check_proposed_block`] held. With `vote = (A, p)` it evaluates
///
/// ```text
/// match proposal.validated_certificate.as_ref() {
///     Some(certificate) =>
///         if vote.value().matches_proposed_block(new_block) {
///             vote.round <= certificate.round
///         } else {
///             vote.round < certificate.round
///         },
///     None =>
///         vote.round.is_fast()
///             && proposal.owner_authorization
///                 .is_some_and(|a| a.round.is_fast() && a.round < new_round)
///             && vote.value().matches_proposed_block(new_block),
/// }
/// ```
///
/// which is the case distinction claimed: a fresh proposal carries neither field, so both arms
/// evaluate to `false`. That the retried certificate is *valid* and certifies exactly `B` comes
/// from the caller: `try_handle_block_proposal` calls
/// `certificate.check(&committee)?` when one is carried, and
/// [`BlockProposal::check_invariants`] — also called there — requires
/// `certificate.check_value(&ValidatedBlock::new(outcome.with(block)))`, i.e. the certificate
/// certifies the very block being proposed, and `content.round > certificate.round`. ∎
///
/// Note `matches_proposed_block` compares the [`ProposedBlock`] only, not the execution outcome;
/// [`FastRetryPreservesBlock`] is where that gap is closed.
///
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`confirmed_vote`]: field@crate::manager::ChainManager::confirmed_vote
/// [`OwnerAuthorization`]: crate::data_types::OwnerAuthorization
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`BlockProposal::check_invariants`]: crate::data_types::BlockProposal::check_invariants
/// [`ProposedBlock`]: crate::data_types::ProposedBlock
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`FastRetryPreservesBlock`]: crate::manager::proof::safety::FastRetryPreservesBlock
pub trait UnlockingRequiresHigherCertificate: ProposalGate {}

/// **Lemma (No validation vote in the fast round).** A correct validator never casts a
/// [`Validated`](crate::types::CertificateKind::Validated) vote in
/// [`Round::Fast`](linera_base::data_types::Round::Fast).
///
/// *Proof.* By [`VoteConstructionSites`] validation votes are produced only in the `else` branch
/// of `if round.is_fast()` in [`ChainManager::create_vote`], where `round` is the vote's round.
/// ∎
///
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
pub trait NoValidationInFastRound: VoteConstructionSites {}

/// **Lemma (A fast confirmation requires an empty lock and no prior vote).** If a correct
/// validator casts a confirmation vote in [`Round::Fast`], then immediately before that vote its
/// [`locking_block`], [`validated_vote`] and [`confirmed_vote`] were all `None`; and immediately
/// after, [`locking_block`] holds a [`LockingBlock::Fast`] for the very block confirmed.
///
/// *Proof.* By [`VoteConstructionSites`] such a vote comes from the `if round.is_fast()` branch
/// of [`ChainManager::create_vote`], so the proposal's round is [`Round::Fast`]. By
/// [`ProposalGate`], [`ChainManager::check_proposed_block`] returned `Accept` for it. Since
/// [`Round::Fast`] is the minimum of the round order ([`RoundOrder`]):
///
/// * the [`locking_block`] guard `ensure!(locking_block.round() < new_round)` is unsatisfiable
///   for `new_round == Round::Fast`, so [`locking_block`] was `None`;
/// * the [`validated_vote`] guard `ensure!(new_round > vote.round)` is likewise unsatisfiable,
///   so [`validated_vote`] was `None`;
/// * for [`confirmed_vote`], [`BlockProposal::check_invariants`] forces a fast-round proposal to
///   be a *retry* of neither: [`BlockProposal::check_invariants`] requires
///   `content.round ≥ authorization.round`, so at `content.round == Round::Fast` — the minimum
///   — any [`OwnerAuthorization`] is for the fast round itself, i.e. it authorizes this very
///   proposal rather than an earlier one; and a certificate would require
///   `content.round > certificate.round ≥ Round::Fast`, which is unsatisfiable. So the `ensure!`
///   of [`UnlockingRequiresHigherCertificate`] takes the certificate-less arm, whose
///   `authorization.round < new_round` conjunct is then false, and would reject. Hence
///   [`confirmed_vote`] was `None`.
///
/// For the post-state: with no certificate carried and `round.is_fast()`, the second arm of
/// the `if`/`else` chain in [`ChainManager::create_vote`] runs `update_locking(LockingBlock::Fast(
/// proposal.clone()), …)` under `self.locking_block.get().is_none()`, which we just established,
/// so the lock is installed on the proposal being confirmed. ∎
///
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`BlockProposal::check_invariants`]: crate::data_types::BlockProposal::check_invariants
/// [`OwnerAuthorization`]: crate::data_types::OwnerAuthorization
/// [`LockingBlock::Fast`]: crate::manager::LockingBlock::Fast
/// [`locking_block`]: crate::manager::ChainManager::locking_block
/// [`validated_vote`]: field@crate::manager::ChainManager::validated_vote
/// [`confirmed_vote`]: field@crate::manager::ChainManager::confirmed_vote
/// [`RoundOrder`]: crate::manager::proof::model::RoundOrder
pub trait FastConfirmationNeedsEmptyLock:
    VoteConstructionSites + ProposalGate + UnlockingRequiresHigherCertificate
{
}

/// **Lemma (A non-fast confirmation requires a validated certificate in the same round).** If a
/// correct validator casts a confirmation vote for block `A` in a round `r` other than
/// [`Round::Fast`](linera_base::data_types::Round::Fast), then a [`ValidatedBlockCertificate`]
/// for `A` in round `r`, valid for the committee, existed at that moment.
///
/// *Code correspondence.*
///
/// | | |
/// |---|---|
/// | transition | [`ChainManager::create_final_vote`] |
/// | reads | [`locking_block`], [`current_round`], [`ownership`] |
/// | writes | [`locking_block`], [`locking_blobs`], [`current_round`], [`round_timeout`], [`confirmed_vote`], [`validated_vote`] |
/// | precondition | `certificate.check(committee)` and [`ChainManager::check_validated_block`] both succeeded ([`ProposalGate`]) |
/// | preserves | [`LockRoundMonotone`], [`ConfirmedVoteRoundMonotone`], [`CastValidationRoundFloor`] |
///
/// *Proof.* By [`VoteConstructionSites`] a confirmation vote in a non-fast round comes from
/// [`ChainManager::create_final_vote`], whose vote round is `validated.round` for its
/// [`ValidatedBlockCertificate`] argument `validated`, and whose voted value is
/// `ConfirmedBlock::new(validated.inner().block().clone())` — the same block. By
/// [`ProposalGate`] the caller verified `certificate.check(&committee)` before passing it in. ∎
///
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::check_validated_block`]: crate::manager::ChainManager::check_validated_block
/// [`locking_block`]: crate::manager::ChainManager::locking_block
/// [`locking_blobs`]: crate::manager::ChainManager::locking_blobs
/// [`current_round`]: field@crate::manager::ChainManager::current_round
/// [`round_timeout`]: crate::manager::ChainManager::round_timeout
/// [`ownership`]: crate::manager::ChainManager::ownership
/// [`confirmed_vote`]: field@crate::manager::ChainManager::confirmed_vote
/// [`validated_vote`]: field@crate::manager::ChainManager::validated_vote
/// [`LockRoundMonotone`]: crate::manager::proof::locking::LockRoundMonotone
/// [`ConfirmedVoteRoundMonotone`]: crate::manager::proof::locking::ConfirmedVoteRoundMonotone
/// [`CastValidationRoundFloor`]: crate::manager::proof::locking::CastValidationRoundFloor
pub trait ConfirmationNeedsValidatedCertificate: VoteConstructionSites + ProposalGate {}

/// **Lemma (A non-fast confirmation happens only in the current round).** When
/// [`ChainManager::create_final_vote`] casts a confirmation vote in round `r`,
/// [`ChainManager::current_round`] equals `r` at that moment — and it had already been raised to
/// at least `r` earlier in the same call.
///
/// *Proof.* [`ChainManager::create_final_vote`] executes, in order:
/// `update_locking(LockingBlock::Regular(validated), blobs)`, which by [`RoundFloor`] leaves the
/// lock at a round `≥ r`; then `update_current_round(local_time)`, which by the same result
/// leaves [`ChainManager::current_round`] at least the lock's round, hence `≥ r`; then
///
/// ```text
/// if self.current_round() != round { return Ok(()); }
/// ```
///
/// so the vote is cast only when the two are equal. ∎
///
/// This ordering is what makes the guard safe against a stale
/// [`current_round`](field@crate::manager::ChainManager::current_round): the lock is folded back
/// into the round *before* the comparison, so a manager whose round register was reset below its
/// lock cannot be induced to confirm in the lower round. [`SafetyStateRecovery`] uses this.
///
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`RoundFloor`]: crate::manager::proof::rounds::RoundFloor
/// [`SafetyStateRecovery`]: crate::manager::proof::locking::SafetyStateRecovery
pub trait ConfirmationOnlyInCurrentRound:
    VoteConstructionSites + crate::manager::proof::rounds::RoundFloor
{
}
