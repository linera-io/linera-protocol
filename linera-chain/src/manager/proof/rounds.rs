// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The round register: what [`current_round`] means, and why it only ever grows.
//!
//! These are the first genuinely inductive results: they quantify over all reachable states of
//! one consensus instance ([`ConsensusInstance`]). They are used by both the locking invariants
//! and the pacemaker, so they are stated once, here.
//!
//! [`current_round`]: field@crate::manager::ChainManager::current_round
//! [`ConsensusInstance`]: crate::manager::proof::model::ConsensusInstance

use crate::manager::proof::{
    model::ConsensusInstance,
    voting::{ProposalGate, VoteConstructionSites},
};

/// **Definition (Current round).** The *current round* of an instance is
/// [`ChainManager::current_round`], the lowest round in which the validator is still willing to
/// vote. It is the round to which [`round_timeout`] applies.
///
/// It is a *derived* quantity: the register caches the maximum of the evidence the validator has
/// seen that the chain has entered a round — a timeout certificate for the previous round, a
/// locking block, or an authenticated proposal — floored at
/// [`ChainOwnership::first_round`]. `RoundFloor` states exactly that.
///
/// [`ChainManager::current_round`]: method@crate::manager::ChainManager::current_round
/// [`round_timeout`]: crate::manager::ChainManager::round_timeout
/// [`ChainOwnership::first_round`]: linera_base::ownership::ChainOwnership::first_round
pub trait CurrentRound {}

/// **Lemma (Round floor).** Let `M` denote
///
/// ```text
/// M = max( { ownership.first_round() }
///        ∪ { ownership.next_round(timeout.round)  (or Round::Validator(u32::MAX) if none) }
///        ∪ { locking_block.round() }
///        ∪ { proposed.content.round }
///        ∪ { signed_proposal.content.round } )
/// ```
///
/// over whichever of the four optional fields are `Some`. Then the private
/// `ChainManager::update_current_round` sets [`current_round`] to `max(current_round, M)`, and
/// resets [`round_timeout`] from [`ChainOwnership::round_timeout`] exactly when that raises the
/// value.
///
/// *Proof.* Direct reading of the method: it builds an iterator over the four optional fields,
/// mapping the timeout certificate through `ownership.next_round(..).unwrap_or(Round::Validator(
/// u32::MAX))`, takes `.max()`, applies `.unwrap_or_default()` — and `Round::default()` is
/// [`Round::Fast`], the minimum ([`RoundOrder`]), so this adds nothing — and then
/// `.max(self.ownership.get().first_round())`. That is `M`. It then returns early on
/// `current_round <= self.current_round()`, and otherwise sets both
/// [`round_timeout`] and [`current_round`]. ∎
///
/// [`current_round`]: field@crate::manager::ChainManager::current_round
/// [`round_timeout`]: crate::manager::ChainManager::round_timeout
/// [`ChainOwnership::round_timeout`]: linera_base::ownership::ChainOwnership::round_timeout
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`RoundOrder`]: crate::manager::proof::model::RoundOrder
pub trait RoundFloor {}

/// **Invariant (The current round never decreases).** Within one consensus instance,
/// [`current_round`] is non-decreasing over time.
///
/// *Proof.* The register has exactly two writers in the crate:
///
/// * `ChainManager::update_current_round`, which by [`RoundFloor`] assigns
///   `max(current_round, M) ≥ current_round`;
/// * [`ChainManager::reset`], which by [`ConsensusInstance`] ends the instance and begins the
///   next one, so it is outside the scope of this invariant.
///
/// Every other mutation of the manager reaches the register only through
/// `update_current_round`: it is called from [`ChainManager::create_vote`],
/// [`ChainManager::create_final_vote`], [`ChainManager::handle_timeout_certificate`],
/// [`ChainManager::update_signed_proposal`], and nowhere else. Note in particular that
/// [`ManagerSafetySnapshot::restore`] does **not** write it — see [`SafetyStateRecovery`] for
/// why that is nonetheless safe. ∎
///
/// [`current_round`]: field@crate::manager::ChainManager::current_round
/// [`ChainManager::reset`]: crate::manager::ChainManager::reset
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::handle_timeout_certificate`]: crate::manager::ChainManager::handle_timeout_certificate
/// [`ChainManager::update_signed_proposal`]: crate::manager::ChainManager::update_signed_proposal
/// [`ManagerSafetySnapshot::restore`]: crate::manager::ManagerSafetySnapshot::restore
/// [`SafetyStateRecovery`]: crate::manager::proof::locking::SafetyStateRecovery
pub trait CurrentRoundMonotone: RoundFloor + ConsensusInstance {}

/// **Invariant (A cast vote never exceeds the current round).** Immediately after a correct
/// validator casts a validation or confirmation vote in round `r`, its [`current_round`] is at
/// least `r`; and by [`CurrentRoundMonotone`] it stays at least `r` for the rest of the
/// instance.
///
/// *Proof.* By [`VoteConstructionSites`] there are three cases.
///
/// *Validation vote in round `r` (non-fast branch of [`ChainManager::create_vote`]).* Before
/// signing, the method calls `update_proposed(proposal.clone(), blobs)` and then
/// `update_current_round(local_time)`. After `update_proposed`, [`proposed`] is `Some` with a
/// round `≥ r`: either it was `None` or held a lower round, and the proposal was stored with
/// round `r`; or it already held a round `≥ r` and the write was skipped. By [`RoundFloor`],
/// `update_current_round` then raises [`current_round`] to at least `proposed.content.round ≥ r`.
///
/// *Confirmation vote in [`Round::Fast`] (fast branch of [`ChainManager::create_vote`]).* Same
/// call sequence; and [`Round::Fast`] is the minimum round ([`RoundOrder`]), so the claim is
/// immediate.
///
/// *Confirmation vote in round `r` ([`ChainManager::create_final_vote`]).* Immediate from
/// [`ConfirmationOnlyInCurrentRound`], which gives `current_round == r`. ∎
///
/// [`current_round`]: field@crate::manager::ChainManager::current_round
/// [`proposed`]: crate::manager::ChainManager::proposed
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`RoundOrder`]: crate::manager::proof::model::RoundOrder
/// [`ConfirmationOnlyInCurrentRound`]: crate::manager::proof::voting::ConfirmationOnlyInCurrentRound
pub trait VoteRoundBelowCurrentRound:
    VoteConstructionSites + ProposalGate + RoundFloor + CurrentRoundMonotone
{
}
