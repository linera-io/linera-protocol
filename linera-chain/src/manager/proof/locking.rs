// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Locking invariants: what a correct validator's committed-to state can look like.
//!
//! These are *protocol invariants*: each is proved by induction over the transitions of one
//! consensus instance ([`ConsensusInstance`]), and each proof discharges the induction by
//! enumerating every method that writes the relevant field. The enumerations are the
//! load-bearing part — a new writer invalidates them — so each invariant names its writers
//! explicitly.
//!
//! The writer sets, for reference:
//!
//! | field | writers |
//! |---|---|
//! | [`locking_block`] | private `update_locking` (from [`create_vote`] ×3 and [`create_final_vote`]); [`ManagerSafetySnapshot::restore`]; [`reset`] |
//! | [`confirmed_vote`] | [`create_vote`] (fast branch); [`create_final_vote`]; [`ManagerSafetySnapshot::restore`]; [`reset`] |
//! | [`validated_vote`] | [`create_vote`] (both branches); [`create_final_vote`]; [`ManagerSafetySnapshot::restore`]; [`reset`] |
//! | [`timeout_vote`] | [`create_timeout_vote`]; [`ManagerSafetySnapshot::restore`]; [`reset`] |
//! | [`fallback_vote`] | [`vote_fallback`]; [`ManagerSafetySnapshot::restore`]; [`reset`] |
//! | [`timeout`] | [`handle_timeout_certificate`]; [`reset`] |
//! | [`proposed`] | private `update_proposed` (from [`create_vote`]); [`reset`] |
//! | [`signed_proposal`] | [`update_signed_proposal`]; private `update_proposed` (clears it); [`reset`] |
//!
//! [`ConsensusInstance`]: crate::manager::proof::model::ConsensusInstance
//! [`locking_block`]: crate::manager::ChainManager::locking_block
//! [`confirmed_vote`]: field@crate::manager::ChainManager::confirmed_vote
//! [`validated_vote`]: field@crate::manager::ChainManager::validated_vote
//! [`timeout_vote`]: crate::manager::ChainManager::timeout_vote
//! [`fallback_vote`]: crate::manager::ChainManager::fallback_vote
//! [`timeout`]: crate::manager::ChainManager::timeout
//! [`proposed`]: crate::manager::ChainManager::proposed
//! [`signed_proposal`]: crate::manager::ChainManager::signed_proposal
//! [`create_vote`]: crate::manager::ChainManager::create_vote
//! [`create_final_vote`]: crate::manager::ChainManager::create_final_vote
//! [`create_timeout_vote`]: crate::manager::ChainManager::create_timeout_vote
//! [`vote_fallback`]: crate::manager::ChainManager::vote_fallback
//! [`handle_timeout_certificate`]: crate::manager::ChainManager::handle_timeout_certificate
//! [`update_signed_proposal`]: crate::manager::ChainManager::update_signed_proposal
//! [`reset`]: crate::manager::ChainManager::reset
//! [`ManagerSafetySnapshot::restore`]: crate::manager::ManagerSafetySnapshot::restore

use crate::{
    data_types::proof::quorum::{
        CertificateCarriesCorrectVote, CertificateEmbedsQuorum, CorrectSignerCastItsVote,
        CorrectValidatorInIntersection,
    },
    manager::proof::{
        model::{ConsensusInstance, DurablePersistence, EpochAgreement, SerializedChainState},
        rounds::{CurrentRoundMonotone, RoundFloor, VoteRoundBelowCurrentRound},
        voting::{
            ConfirmationNeedsValidatedCertificate, ConfirmationOnlyInCurrentRound,
            FastConfirmationNeedsEmptyLock, NoValidationInFastRound, ProposalGate,
            ValidationRoundStrictlyIncreases, VoteConstructionSites,
        },
    },
};

/// **Definition (Lock).** The *lock* of an instance is [`ChainManager::locking_block`], and its
/// *lock round* is [`LockingBlock::round`] of that value, or `⊥` when the field is `None`, with
/// `⊥` below every round.
///
/// A lock records the most recent block this validator may already have helped confirm: either a
/// [`LockingBlock::Regular`] — a [`ValidatedBlockCertificate`], proof that a quorum validated
/// that block in that round — or a [`LockingBlock::Fast`], the super owner's original fast-round
/// proposal. Both are re-proposable: a client reads the lock out of
/// [`ChainManagerInfo::requested_locking`] and re-proposes it in a higher round.
///
/// A [`LockingBlock::Fast`] may be stored directly, from a fast-round proposal, or rebuilt from a
/// later proposal's [`OwnerAuthorization`]; [`FastLockReconstruction`] shows the rebuilt one is
/// equally genuine.
///
/// [`OwnerAuthorization`]: crate::data_types::OwnerAuthorization
/// [`FastLockReconstruction`]: crate::manager::proof::voting::FastLockReconstruction
///
/// [`ChainManager::locking_block`]: crate::manager::ChainManager::locking_block
/// [`LockingBlock::round`]: crate::manager::LockingBlock::round
/// [`LockingBlock::Regular`]: crate::manager::LockingBlock::Regular
/// [`LockingBlock::Fast`]: crate::manager::LockingBlock::Fast
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`ChainManagerInfo::requested_locking`]: crate::manager::ChainManagerInfo::requested_locking
pub trait Lock {}

/// **Invariant (The lock round never decreases).** Within one consensus instance, the lock round
/// is non-decreasing.
///
/// *Proof.* By the writer table in the [module documentation](self), [`locking_block`] is
/// written only by the private `update_locking`, by [`ManagerSafetySnapshot::restore`], and by
/// [`reset`] (which by [`ConsensusInstance`] ends the instance).
///
/// `update_locking` begins with
///
/// ```text
/// if let Some(old_locked) = self.locking_block.get() {
///     if old_locked.round() >= locking.round() { return Ok(()); }
/// }
/// ```
///
/// so it writes only a strictly higher round. All four call sites go through it: the two
/// `update_locking` calls in the `Regular` and `Fast` arms of [`create_vote`]'s `match`, the one
/// in the `None` arm, and the one in [`create_final_vote`].
///
/// [`ManagerSafetySnapshot::restore`] is handled by [`SafetyStateRecovery`]. ∎
///
/// [`locking_block`]: crate::manager::ChainManager::locking_block
/// [`create_vote`]: crate::manager::ChainManager::create_vote
/// [`create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`reset`]: crate::manager::ChainManager::reset
/// [`ManagerSafetySnapshot::restore`]: crate::manager::ManagerSafetySnapshot::restore
/// [`ConsensusInstance`]: crate::manager::proof::model::ConsensusInstance
pub trait LockRoundMonotone: Lock + ConsensusInstance {}

/// **Invariant (The confirmed-vote round never decreases).** Within one consensus instance, if
/// [`confirmed_vote`] is `Some` with round `p`, it never later holds a vote with round `< p`.
///
/// *Proof.* By the writer table in the [module documentation](self) the field has three
/// in-instance writers.
///
/// *[`create_final_vote`] at round `r`.* By [`ConfirmationOnlyInCurrentRound`] it writes only
/// when `current_round == r`. If a previous confirmation vote had round `p`, then by
/// [`VoteRoundBelowCurrentRound`] the current round was `≥ p` at that time, and by
/// [`CurrentRoundMonotone`] it still is. Hence `r = current_round ≥ p`.
///
/// *[`create_vote`], fast branch.* Writes a vote in [`Round::Fast`], the minimum round — but by
/// [`FastConfirmationNeedsEmptyLock`] this branch is reachable only when [`confirmed_vote`] was
/// `None`, so there is no earlier round to undercut.
///
/// *[`ManagerSafetySnapshot::restore`].* See [`SafetyStateRecovery`]. ∎
///
/// [`confirmed_vote`]: field@crate::manager::ChainManager::confirmed_vote
/// [`create_vote`]: crate::manager::ChainManager::create_vote
/// [`create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`ManagerSafetySnapshot::restore`]: crate::manager::ManagerSafetySnapshot::restore
pub trait ConfirmedVoteRoundMonotone:
    ConfirmationOnlyInCurrentRound
    + FastConfirmationNeedsEmptyLock
    + VoteRoundBelowCurrentRound
    + CurrentRoundMonotone
{
}

/// **Invariant (Cast validation rounds leave a floor).** If a correct validator has ever cast a
/// validation vote in round `s` during an instance, then from that moment on
///
/// ```text
/// max(validated_vote.round, lock round) ≥ s
/// ```
///
/// where an absent field contributes `⊥`.
///
/// This is the invariant that survives [`create_final_vote`] clearing [`validated_vote`], and it
/// is what makes [`OneValidationVotePerRound`] hold over a whole instance rather than only
/// between confirmations.
///
/// *Proof.* Induction over the transitions of the instance.
///
/// *Base.* Immediately after the vote, [`validated_vote`] is `Some(_, s)` — the non-fast branch
/// of [`create_vote`] ends with `self.validated_vote.get_mut().insert(vote)`.
///
/// *Step.* Only two transitions can lower either side of the maximum. The lock round is
/// non-decreasing by [`LockRoundMonotone`], so only [`validated_vote`] can fall, and by the
/// writer table it is set to `None` in exactly two places:
///
/// * [`create_vote`], fast branch (`self.validated_vote.set(None)`). Unreachable here: that
///   branch requires [`ChainManager::check_proposed_block`] to have accepted a proposal in
///   [`Round::Fast`] ([`ProposalGate`]), whose [`validated_vote`] guard
///   `ensure!(new_round > vote.round)` is unsatisfiable for the minimum round unless
///   [`validated_vote`] is already `None`; and if it is `None`, the induction hypothesis is
///   carried by the lock, which this branch does not lower.
/// * [`create_final_vote`] at some round `r`. It clears the field only after
///   `update_locking(LockingBlock::Regular(validated), blobs)` and only on the branch where a
///   vote is cast, which by [`ConfirmationOnlyInCurrentRound`] requires `current_round == r`. By
///   [`VoteRoundBelowCurrentRound`] and [`CurrentRoundMonotone`], `current_round ≥ s`, so
///   `r ≥ s`. By [`RoundFloor`] and [`LockRoundMonotone`], after `update_locking` the lock round
///   is `≥ r ≥ s`. So the maximum is preserved by the lock. ∎
///
/// [`validated_vote`]: field@crate::manager::ChainManager::validated_vote
/// [`create_vote`]: crate::manager::ChainManager::create_vote
/// [`create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
pub trait CastValidationRoundFloor:
    LockRoundMonotone
    + ProposalGate
    + ConfirmationOnlyInCurrentRound
    + VoteRoundBelowCurrentRound
    + CurrentRoundMonotone
    + RoundFloor
{
}

/// **Lemma (One validation vote per round).** A correct validator casts at most one validation
/// vote per round of an instance.
///
/// *Proof.* Suppose it casts validation votes for `B₁` and then `B₂` in the same round `s`. By
/// [`ProposalGate`] the second was preceded by [`ChainManager::check_proposed_block`] returning
/// `Accept` for a proposal in round `s`, so both of its round guards held at that moment:
///
/// * `ensure!(new_round > vote.round)` for [`validated_vote`], and
/// * `ensure!(locking_block.round() < new_round)` for the lock.
///
/// Together these give `max(validated_vote.round, lock round) < s`, contradicting
/// [`CastValidationRoundFloor`], which the first vote established. (Where "the same vote" is
/// re-submitted rather than a different block, [`ChainManager::check_proposed_block`] returns
/// [`Outcome::Skip`] on its first branch and nothing is signed.)
///
/// The argument needs the two votes to be observed by the *same* manager state, which is
/// [`DurablePersistence`] — a validator that lost its state across a crash could sign twice —
/// and [`SerializedChainState`], which rules out two concurrent handlers each seeing the
/// pre-vote state. ∎
///
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`validated_vote`]: field@crate::manager::ChainManager::validated_vote
/// [`Outcome::Skip`]: crate::manager::Outcome::Skip
/// [`SerializedChainState`]: crate::manager::proof::model::SerializedChainState
pub trait OneValidationVotePerRound:
    ProposalGate
    + CastValidationRoundFloor
    + ValidationRoundStrictlyIncreases
    + DurablePersistence
    + SerializedChainState
{
}

/// **Lemma (One confirmation vote per round).** A correct validator casts at most one
/// confirmation vote per round of an instance.
///
/// *Proof.* Two cases on the round `r`.
///
/// *`r` is [`Round::Fast`].* By [`FastConfirmationNeedsEmptyLock`], such a vote requires the lock
/// to be `None` beforehand and installs a [`LockingBlock::Fast`] — of round [`Round::Fast`] —
/// afterwards. A second fast confirmation would again require the lock to be `None`,
/// contradicting [`LockRoundMonotone`]. (Directly: the lock guard
/// `ensure!(locking_block.round() < Round::Fast)` in [`ChainManager::check_proposed_block`] is
/// unsatisfiable.)
///
/// *`r` is not [`Round::Fast`].* By [`ConfirmationNeedsValidatedCertificate`] the vote comes from
/// [`create_final_vote`], which is guarded by [`ChainManager::check_validated_block`]
/// ([`ProposalGate`]). That guard contains
///
/// ```text
/// if let Some(locking) = self.locking_block.get() {
///     ensure!(new_round > locking.round(), ChainError::InsufficientRoundStrict(locking.round()));
/// }
/// ```
///
/// The first confirmation at round `r` ran `update_locking(LockingBlock::Regular(validated), …)`
/// with `validated.round == r`, so afterwards the lock round is `≥ r` by [`RoundFloor`] and
/// [`LockRoundMonotone`]. A second certificate in round `r` therefore fails `new_round >
/// locking.round()` and no second vote is cast. (If the second certificate is for the same block
/// and round, [`ChainManager::check_validated_block`] returns [`Outcome::Skip`] on its first
/// branch instead.)
///
/// As in [`OneValidationVotePerRound`], the argument consumes [`DurablePersistence`] and
/// [`SerializedChainState`]. ∎
///
/// **Where this is fragile.** In the non-fast case the guard lives in
/// [`ChainManager::check_validated_block`], i.e. at the *call site*, not inside
/// [`create_final_vote`] — which re-checks only [`ChainManager::current_round`] and would sign
/// again for a different block certified in the same round. See [`ProposalGate`].
///
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`LockingBlock::Fast`]: crate::manager::LockingBlock::Fast
/// [`create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`ChainManager::check_validated_block`]: crate::manager::ChainManager::check_validated_block
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`Outcome::Skip`]: crate::manager::Outcome::Skip
/// [`SerializedChainState`]: crate::manager::proof::model::SerializedChainState
pub trait OneConfirmationVotePerRound:
    ProposalGate
    + FastConfirmationNeedsEmptyLock
    + ConfirmationNeedsValidatedCertificate
    + LockRoundMonotone
    + RoundFloor
    + DurablePersistence
    + SerializedChainState
{
}

/// **Lemma (At most one validated block per round).** For a given chain and height, all
/// valid [`ValidatedBlockCertificate`]s certified in the same round certify the same block.
///
/// *Proof.* Let two such certificates certify `B₁` and `B₂` in round `s`. By
/// [`EpochAgreement`] they are judged against the same committee, so by
/// [`CertificateEmbedsQuorum`] their signer sets are two quorums of it, and by
/// [`CorrectValidatorInIntersection`] some correct validator `v` signed both. By
/// [`CorrectSignerCastItsVote`], `v` cast validation votes for `B₁` and for `B₂`, both in
/// round `s`. By [`OneValidationVotePerRound`], `B₁ = B₂`. ∎
///
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`CertificateEmbedsQuorum`]: crate::data_types::proof::quorum::CertificateEmbedsQuorum
pub trait UniqueValidatedBlockPerRound:
    OneValidationVotePerRound
    + CorrectValidatorInIntersection
    + CertificateEmbedsQuorum
    + CorrectSignerCastItsVote
    + EpochAgreement
{
}

/// **Lemma (No validated block certificate in the fast round).** No valid
/// [`ValidatedBlockCertificate`] is certified in
/// [`Round::Fast`](linera_base::data_types::Round::Fast).
///
/// *Proof.* By [`CertificateCarriesCorrectVote`] such a certificate would require a correct
/// validator to have cast a validation vote in [`Round::Fast`](linera_base::data_types::Round::Fast),
/// which [`NoValidationInFastRound`] forbids. ∎
///
/// This is why the fast round is not a "round" in the ordinary sense: it has a confirmation step
/// but no validation step, and therefore no certificate a later round could be unlocked by.
///
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
pub trait NoValidatedBlockInFastRound:
    NoValidationInFastRound + CertificateCarriesCorrectVote
{
}

/// **Lemma (Safety state survives a local reset).** After
/// `ChainWorkerState::reset_and_reexecute_chain`, the manager's lock and cast votes are at least
/// what they were before the reset, and no invariant of this module is broken.
///
/// This is the one transition that writes [`locking_block`], [`confirmed_vote`] and
/// [`validated_vote`] without going through the voting path, so [`LockRoundMonotone`],
/// [`ConfirmedVoteRoundMonotone`] and [`CastValidationRoundFloor`] each owe it an argument.
///
/// *Proof.* The procedure captures [`ManagerSafetySnapshot::capture`] *before* wiping storage,
/// replays the chain's confirmed blocks from storage, and then calls
/// [`ManagerSafetySnapshot::restore`] — but only under the explicit guard
/// `new_tip_height == tip_height`, so the restored fields belong to the same pending height and
/// hence the same consensus instance in the sense of [`ConsensusInstance`].
///
/// Replay only feeds [`ConfirmedBlockCertificate`]s to `process_confirmed_block`, which casts no
/// vote and calls [`reset`] once per replayed height; so at the moment of restore the five
/// snapshot fields are exactly what the final `reset` left them — `None`. The restore therefore
/// re-installs the pre-reset values rather than overwriting newer ones, and each field returns to
/// a value it genuinely held. The three invariants above are stated over the values a correct
/// validator has committed to, so restoring them re-establishes rather than violates them.
///
/// The subtle point is [`current_round`], which the snapshot does **not** capture or restore: it
/// is left at [`ChainOwnership::first_round`] by the last [`reset`], potentially far below the
/// restored lock. That cannot be exploited, because every path that could act on the lower round
/// re-derives the round from the lock first:
///
/// * [`create_final_vote`] calls `update_locking` then `update_current_round` before comparing
///   against `round`, so by [`RoundFloor`] the comparison sees a round `≥` the restored lock
///   round — this is exactly [`ConfirmationOnlyInCurrentRound`];
/// * [`ChainManager::check_proposed_block`] rejects any proposal not strictly above the lock
///   round, regardless of [`current_round`].
///
/// So the restored lock, not the round register, is what binds. ∎
///
/// **Residual obligation.** [`ManagerSafetySnapshot`] records no height of its own; the
/// correspondence between snapshot and instance rests entirely on the caller's
/// `new_tip_height == tip_height` guard. A future caller of
/// [`ManagerSafetySnapshot::restore`] must reproduce it.
///
/// [`locking_block`]: crate::manager::ChainManager::locking_block
/// [`confirmed_vote`]: field@crate::manager::ChainManager::confirmed_vote
/// [`validated_vote`]: field@crate::manager::ChainManager::validated_vote
/// [`current_round`]: field@crate::manager::ChainManager::current_round
/// [`create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`reset`]: crate::manager::ChainManager::reset
/// [`ManagerSafetySnapshot`]: crate::manager::ManagerSafetySnapshot
/// [`ManagerSafetySnapshot::capture`]: crate::manager::ManagerSafetySnapshot::capture
/// [`ManagerSafetySnapshot::restore`]: crate::manager::ManagerSafetySnapshot::restore
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`ConsensusInstance`]: crate::manager::proof::model::ConsensusInstance
/// [`ChainOwnership::first_round`]: linera_base::ownership::ChainOwnership::first_round
pub trait SafetyStateRecovery:
    ConsensusInstance + LockRoundMonotone + RoundFloor + ConfirmationOnlyInCurrentRound
{
}

/// **Remark (Votes a correct validator may hold simultaneously).** Nothing above forbids a
/// correct validator from holding a [`confirmed_vote`] and a [`validated_vote`] at once — it
/// does, whenever it validates in a round above its last confirmation. What the invariants
/// forbid is *two of the same kind in the same round*. The reporting projection
/// [`ChainManagerInfo::pending`] picks whichever is higher, which is why a client observing a
/// validator sees a single "pending" vote even though two are stored.
///
/// Similarly, [`timeout_vote`] and [`fallback_vote`] may both be set: they are votes on the same
/// [`Timeout`] value in different rounds ([`FallbackVote`]), so they are not conflicting votes in
/// the sense of [`OneValidationVotePerRound`] — and if their rounds coincide, so do their
/// payloads, and hence their signatures.
///
/// [`confirmed_vote`]: field@crate::manager::ChainManager::confirmed_vote
/// [`validated_vote`]: field@crate::manager::ChainManager::validated_vote
/// [`timeout_vote`]: crate::manager::ChainManager::timeout_vote
/// [`fallback_vote`]: crate::manager::ChainManager::fallback_vote
/// [`ChainManagerInfo::pending`]: crate::manager::ChainManagerInfo::pending
/// [`Timeout`]: crate::block::Timeout
/// [`FallbackVote`]: crate::manager::proof::timeouts::FallbackVote
pub trait SimultaneousVotes: VoteConstructionSites {}
