// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The safety proof: at most one block is ever committed per chain and height.
//!
//! The argument has one non-trivial step, [`LockPreservation`], an induction over rounds showing
//! that once a block is committed no *later* round can validate anything else. Everything before
//! it is local implementation properties and per-round uniqueness; everything after it is
//! bookkeeping.
//!
//! Nothing in this module depends on synchrony, on message delivery, or on any validator being
//! responsive. Safety holds in every execution permitted by
//! [`MaxByzantineWeight`](crate::manager::proof::model::MaxByzantineWeight), including ones where
//! the protocol makes no progress at all.

use crate::{
    data_types::proof::quorum::{
        CertificateEmbedsQuorum, CorrectSignerCastItsVote, CorrectValidatorInIntersection,
    },
    manager::proof::{
        commit::{CommitRestsOnValidation, TipAdvancesOnlyOnValidCertificate},
        locking::{
            CastValidationRoundFloor, ConfirmedVoteRoundMonotone, NoValidatedBlockInFastRound,
            OneConfirmationVotePerRound, OneValidationVotePerRound, SafetyStateRecovery,
            UniqueValidatedBlockPerRound,
        },
        model::{ConflictingBlocks, DeterministicExecution, EpochAgreement},
        rounds::{CurrentRoundMonotone, VoteRoundBelowCurrentRound},
        voting::{
            ConfirmationNeedsValidatedCertificate, FastConfirmationNeedsEmptyLock,
            UnlockingRequiresHigherCertificate,
        },
    },
};

/// **Lemma (A fast retry cannot change the block).** Let a block `A` be confirmed in
/// [`Round::Fast`], and let a correct validator later cast a validation vote for a block `B` on a
/// proposal whose [`OriginalProposal::Fast`] retries `A`'s proposal. Then `B = A`.
///
/// *Proof.* By [`UnlockingRequiresHigherCertificate`], the fast-retry arm of
/// [`ChainManager::check_proposed_block`] accepts only if the validator's stored confirmation
/// vote is in the fast round and its value satisfies `matches_proposed_block(new_block)`. That
/// predicate compares the [`ProposedBlock`] components only — chain, epoch, transactions,
/// height, timestamp, authenticated owner, parent hash — so it leaves open that `A` and `B`
/// share a proposal but differ in [`BlockExecutionOutcome`], which by [`ConflictingBlocks`]
/// would make them conflicting blocks.
///
/// That gap is closed by [`DeterministicExecution`]. The retry re-executes the proposal
/// (`try_handle_block_proposal` takes the `else` branch of `if let Some(outcome) = outcome`,
/// since a fast retry carries no outcome), at the same height with the same parent, so the only
/// input that differs from the original fast execution is the round argument
/// [`Round::multi_leader`]. A block accepted in the fast round recorded no oracle responses —
/// `try_handle_block_proposal` rejects one that did with `WorkerError::FastBlockUsingOracles` —
/// and the round is observable only as [`OracleResponse::Round`]. An execution that never
/// queried the round therefore cannot branch on it, so by determinism the two executions agree
/// and `B = A`. ∎
///
/// **Residual obligation.** The no-oracle check is applied when the *proposal's* round is fast,
/// not when a fast block is retried, so this step relies on determinism of the execution engine
/// rather than on a runtime check at the retry. An execution engine that made an outcome depend
/// on the round without recording an [`OracleResponse::Round`] would break it. This is the one
/// place in the safety argument that reaches outside consensus into execution.
///
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`Round::multi_leader`]: linera_base::data_types::Round::multi_leader
/// [`OriginalProposal::Fast`]: crate::data_types::OriginalProposal::Fast
/// [`ChainManager::check_proposed_block`]: crate::manager::ChainManager::check_proposed_block
/// [`ProposedBlock`]: crate::data_types::ProposedBlock
/// [`BlockExecutionOutcome`]: crate::data_types::BlockExecutionOutcome
/// [`OracleResponse::Round`]: linera_base::data_types::OracleResponse::Round
pub trait FastRetryPreservesBlock:
    UnlockingRequiresHigherCertificate + DeterministicExecution + ConflictingBlocks
{
}

/// **Lemma (Unlocking justification).** Let a correct validator cast a validation vote for `B` in
/// round `s`, and let `(A, p)` be its stored confirmation vote immediately before, with `A ≠ B`.
/// Then a valid [`ValidatedBlockCertificate`] for `B` exists in some round `t` with
/// `p < t < s`.
///
/// *Proof.* By [`UnlockingRequiresHigherCertificate`], with a stored confirmation vote present
/// the proposal must carry an [`OriginalProposal`], and:
///
/// * a fresh proposal (`None`) is rejected;
/// * a fast retry requires `A` to match `B`'s proposal, which by [`FastRetryPreservesBlock`]
///   forces `A = B`, contradicting the hypothesis;
/// * a regular retry carries a certificate `c` which — by the caller's `certificate.check(&
///   committee)` and [`BlockProposal::check_invariants`] — is a valid
///   [`ValidatedBlockCertificate`] for exactly `B`, with `c.round < s`; and since `A` does not
///   match `B`, the accepted branch is `vote.round < certificate.round`, i.e. `p < c.round`.
///
/// Take `t = c.round`. ∎
///
/// This is the hinge of [`LockPreservation`]: a correct validator abandons a block it confirmed
/// only in exchange for a quorum that validated the replacement *strictly above* its own
/// confirmation — which lets the induction step down into a strictly smaller round.
///
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`OriginalProposal`]: crate::data_types::OriginalProposal
/// [`BlockProposal::check_invariants`]: crate::data_types::BlockProposal::check_invariants
pub trait UnlockingJustification:
    UnlockingRequiresHigherCertificate + FastRetryPreservesBlock
{
}

/// **Theorem (Lock preservation).** Suppose a valid [`ConfirmedBlockCertificate`] for a block `A`
/// is certified in round `r`, at some height of some chain. Then for every round `s ≥ r`, every
/// valid [`ValidatedBlockCertificate`] at that height and round `s` certifies `A`.
///
/// *Proof.* Strong induction on `s ≥ r`. Assume the claim for all `t` with `r ≤ t < s`, and let
/// `C'` be a valid [`ValidatedBlockCertificate`] for `B` in round `s`. By [`EpochAgreement`] the
/// confirmed certificate and `C'` are judged against the same committee; by
/// [`CertificateEmbedsQuorum`] both signer sets are quorums of it, so by
/// [`CorrectValidatorInIntersection`] they share a correct validator `v`, and by
/// [`CorrectSignerCastItsVote`] `v` itself cast both votes:
/// a confirmation vote for `A` in round `r` (call it **(a)**) and a validation vote for `B` in
/// round `s` (call it **(b)**).
///
/// **Case `s = r`.** By [`NoValidatedBlockInFastRound`], `s` is not the fast round, so `r` is
/// not either; by [`CommitRestsOnValidation`] a valid [`ValidatedBlockCertificate`] for `A` in
/// round `r` exists. By [`UniqueValidatedBlockPerRound`] applied to it and `C'`, `B = A`.
///
/// **Case `s > r`.** Consider the order of **(a)** and **(b)** in `v`'s execution.
///
/// *Suppose **(b)** preceded **(a)**.* By [`VoteRoundBelowCurrentRound`] and
/// [`CurrentRoundMonotone`], from **(b)** onwards `v`'s current round is `≥ s > r`. If `r` is not
/// the fast round, **(a)** comes from [`ChainManager::create_final_vote`], which by
/// [`ConfirmationNeedsValidatedCertificate`] and [`ConfirmationOnlyInCurrentRound`] casts a vote
/// only when the current round *equals* `r` — impossible. If `r` is the fast round, **(a)** comes
/// from the fast branch of [`ChainManager::create_vote`], which by
/// [`FastConfirmationNeedsEmptyLock`] requires an empty lock and an absent validation vote — but
/// [`CastValidationRoundFloor`], established by **(b)**, forces
/// `max(validated_vote.round, lock round) ≥ s > Round::Fast`. Also impossible. So **(a)**
/// preceded **(b)**.
///
/// *So **(a)** preceded **(b)**.* Let `(A', p)` be `v`'s stored confirmation vote immediately
/// before **(b)**. It is present, since **(a)** stored one; and by
/// [`ConfirmedVoteRoundMonotone`], `p ≥ r`.
///
/// * If `A' ≠ B`, then [`UnlockingJustification`] yields a valid
///   [`ValidatedBlockCertificate`] for `B` in a round `t` with `p < t < s`. Then `r ≤ p < t < s`,
///   so the induction hypothesis applies at `t` and gives `B = A`.
/// * If `A' = B`, then `v` confirmed `B` in round `p ≥ r`.
///   * If `p = r`: `v` also confirmed `A` in round `r` by **(a)**, so [`OneConfirmationVotePerRound`]
///     gives `A = B`.
///   * If `p > r`: then `p` is not the fast round (it exceeds `r ≥ Round::Fast`), so by
///     [`ConfirmationNeedsValidatedCertificate`] a valid [`ValidatedBlockCertificate`] for `B` in
///     round `p` existed. Moreover [`UnlockingRequiresHigherCertificate`] applied to **(b)** — in
///     the branch where the stored vote's value matches the proposed block — gives
///     `p ≤ c.round < s` for the certificate `c` the proposal carries, hence `p < s`. So
///     `r < p < s`, the induction hypothesis applies at `p`, and `B = A`. ∎
///
/// The induction is well founded because rounds are totally ordered and every appeal to the
/// hypothesis is at a round strictly between `r` and `s`.
///
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ConfirmationOnlyInCurrentRound`]: crate::manager::proof::voting::ConfirmationOnlyInCurrentRound
pub trait LockPreservation:
    UnlockingJustification
    + CommitRestsOnValidation
    + UniqueValidatedBlockPerRound
    + NoValidatedBlockInFastRound
    + OneConfirmationVotePerRound
    + ConfirmedVoteRoundMonotone
    + CastValidationRoundFloor
    + FastConfirmationNeedsEmptyLock
    + ConfirmationNeedsValidatedCertificate
    + VoteRoundBelowCurrentRound
    + CurrentRoundMonotone
    + CorrectValidatorInIntersection
    + CertificateEmbedsQuorum
    + CorrectSignerCastItsVote
    + EpochAgreement
{
}

/// **Theorem (Commit agreement).** For a given chain and height, all valid
/// [`ConfirmedBlockCertificate`]s certify the same block. Equivalently: no two conflicting blocks
/// ([`ConflictingBlocks`]) are ever both committed.
///
/// *Proof.* Let valid confirmed certificates for `A` in round `r` and for `B` in round `s`
/// exist, with `r ≤ s` after renaming.
///
/// * If `r = s`: by [`EpochAgreement`] both are judged against the same committee, and by
///   [`CertificateEmbedsQuorum`] their signer sets are quorums of it, so by
///   [`CorrectValidatorInIntersection`] a correct validator `v` signed both. By
///   [`CorrectSignerCastItsVote`], `v` cast confirmation votes for `A` and for `B` in round `r`.
///   By [`OneConfirmationVotePerRound`], `A = B`.
/// * If `r < s`: then `s` is not [`Round::Fast`], so [`CommitRestsOnValidation`] gives a valid
///   [`ValidatedBlockCertificate`] for `B` in round `s`. By [`LockPreservation`], applied to the
///   commit of `A` in round `r` and to `s > r`, that certificate certifies `A`. Hence `B = A`. ∎
///
/// *In observable terms.* Combining with [`TipAdvancesOnlyOnValidCertificate`]: if any correct
/// validator's [`ChainTipState`] records a block hash at height `h`, then no correct validator
/// ever records a different hash at `h` — whatever the network does, and whatever the faulty
/// validators sign.
///
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`Round::Fast`]: linera_base::data_types::Round::Fast
/// [`ChainTipState`]: crate::ChainTipState
pub trait CommitAgreement:
    LockPreservation
    + CommitRestsOnValidation
    + OneConfirmationVotePerRound
    + CorrectValidatorInIntersection
    + CertificateEmbedsQuorum
    + CorrectSignerCastItsVote
    + ConflictingBlocks
    + EpochAgreement
    + TipAdvancesOnlyOnValidCertificate
{
}

/// **Corollary (The committed chain is unique).** For each chain there is at most one sequence of
/// committed blocks: the committed blocks at heights `0, 1, 2, …` form a single hash-linked list,
/// and any two correct validators' [`block_hashes`](crate::ChainStateView) agree wherever both
/// are defined. In particular the committed prefixes observed by correct validators are always
/// compatible — one is a prefix of the other.
///
/// *Proof.* Induction on the height `h`.
///
/// At each height, [`CommitAgreement`] gives uniqueness of the committed block, *provided*
/// [`EpochAgreement`] holds there. That proviso is what the induction supplies: the chain's epoch
/// and committee at height `h` are functions of the execution state after height `h − 1`, which
/// by the induction hypothesis (uniqueness below `h`) and [`DeterministicExecution`] is unique.
/// The base case `h = 0` is the genesis configuration, which is agreed by construction. Applying
/// [`CommitAgreement`] at `h` closes the step.
///
/// Linkage: by [`TipAdvancesOnlyOnValidCertificate`] a correct validator records a hash at `h`
/// only for a certified block, and `ChainTipState::verify_block_chaining` requires a proposal's
/// `previous_block_hash` to equal the tip's hash, so the unique committed block at `h` has the
/// unique committed block at `h − 1` as its parent. ∎
///
/// This is the point where the specification's per-instance scoping
/// ([`ConsensusInstance`](crate::manager::proof::model::ConsensusInstance)) is discharged: each
/// consensus instance decides one height, and the heights compose into a chain.
pub trait UniqueChain:
    CommitAgreement + EpochAgreement + DeterministicExecution + TipAdvancesOnlyOnValidCertificate
{
}

/// **Corollary (Agreement failure is attributable).** The converse of [`CommitAgreement`]: when
/// it fails, the failure is not silent. Two conflicting confirmed certificates are self-contained
/// evidence convicting validators of at least
/// [`validity_threshold`](linera_execution::committee::Committee::validity_threshold) weight, and
/// no correct validator is ever convictable.
///
/// This is stated and proved in [`crate::justification::proof`] rather than here, because its
/// assumption base is deliberately *weaker*: it must hold precisely when
/// [`MaxByzantineWeight`](crate::manager::proof::model::MaxByzantineWeight) has failed, which is
/// the one regime this module says nothing about. See
/// [`AccountableSafety`](crate::justification::proof::AccountableSafety) for the theorem and
/// [`AccountabilityScope`](crate::justification::proof::AccountabilityScope) for what it excludes
/// — notably that incorrect block execution is *not* attributable.
///
/// It is worth stating here nonetheless, because it explains why certificates carry an unlocking
/// round ([`UnlockingRound`]) and a justification chain at all, given that [`CommitAgreement`]
/// uses neither: they buy accountability, not agreement.
///
/// [`UnlockingRound`]: crate::data_types::proof::objects::UnlockingRound
pub trait Accountability {}

/// **Remark (What safety does not claim).** Three exclusions are worth stating explicitly,
/// because each is a property a reader may expect [`CommitAgreement`] to carry and it does not.
///
/// * **Nothing is claimed about faulty validators' state.** A faulty validator may record any
///   block at any height. [`CommitAgreement`] constrains which *certificates* can exist; the
///   observable consequence in [`TipAdvancesOnlyOnValidCertificate`] is about correct validators.
/// * **Nothing is claimed about progress.** An execution in which no block is ever committed
///   satisfies every result in this module. In particular a super owner that issues two
///   conflicting fast proposals can split the vote and wedge the height permanently, and that is
///   a liveness failure, not a safety one — see `linera_core::proof::liveness`.
/// * **Nothing is claimed when [`MaxByzantineWeight`] fails.** Above the fault bound,
///   [`CorrectValidatorInIntersection`] fails and conflicting commits become possible. What
///   remains is [`Accountability`].
///
/// [`MaxByzantineWeight`]: crate::manager::proof::model::MaxByzantineWeight
pub trait SafetyScope: CommitAgreement + SafetyStateRecovery + OneValidationVotePerRound {}
