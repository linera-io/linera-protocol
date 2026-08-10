// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Accountability: convicting the validators responsible when agreement fails.
//!
//! [`CommitAgreement`] holds only while [`MaxByzantineWeight`] does. This module proves what
//! happens when it does not: a violation leaves *self-contained evidence* naming validators of at
//! least [`Committee::validity_threshold`] weight — more than the fault bound permits, so the
//! conviction set is itself a proof that the assumption was broken.
//!
//! Two properties, deliberately independent:
//!
//! * **Soundness** ([`ProofSoundness`]) — a proof that [`EquivocationProof::check`] accepts names
//!   a genuinely faulty validator. No correct validator is ever convictable.
//! * **Completeness** ([`ConflictCompleteness`]) — two conflicting confirmed certificates yield
//!   enough accepted proofs, from the certificates alone.
//!
//! **Neither depends on [`MaxByzantineWeight`]**, which is the point: both must hold precisely in
//! the regime where the fault bound has failed. Soundness is per-validator and rests only on
//! [`UnforgeableSignatures`]; completeness needs only [`Intersection`], which in turn needs only
//! [`ThresholdArithmetic`]. Accountability therefore sits on a strictly weaker assumption base
//! than the safety theorem it backstops.
//!
//! [`EquivocationProof::check`]: crate::justification::EquivocationProof::check
//! [`CommitAgreement`]: crate::manager::proof::safety::CommitAgreement
//! [`MaxByzantineWeight`]: crate::manager::proof::model::MaxByzantineWeight
//! [`UnforgeableSignatures`]: crate::manager::proof::model::UnforgeableSignatures
//! [`Intersection`]: crate::data_types::proof::quorum::Intersection
//! [`ThresholdArithmetic`]: crate::data_types::proof::quorum::ThresholdArithmetic
//! [`Committee::validity_threshold`]: linera_execution::committee::Committee::validity_threshold

use crate::{
    data_types::proof::quorum::{CertificateEmbedsQuorum, Intersection, ThresholdArithmetic},
    manager::proof::{
        commit::{CertifiedBlockWasExecuted, CommitRestsOnValidation},
        locking::{
            ConfirmedVoteRoundMonotone, OneConfirmationVotePerRound, OneValidationVotePerRound,
            SafetyStateRecovery,
        },
        model::{ConsensusInstance, DeterministicExecution, UnforgeableSignatures},
        pacemaker::LeaderEligibility,
        rounds::{CurrentRoundMonotone, RoundFloor, VoteRoundBelowCurrentRound},
        safety::CommitAgreement,
        voting::{
            ConfirmationOnlyInCurrentRound, FastConfirmationNeedsEmptyLock,
            UnlockingRequiresHigherCertificate, VoteConstructionSites,
        },
    },
};

/// **Definition (Proof of misbehaviour).** A *proof of misbehaviour* against validator `v` is an
/// [`EquivocationProof`] naming `v` for which [`EquivocationProof::check`] returns `Ok` against a
/// committee. There are four shapes, and each exhibits a pair of `v`'s own signatures — or, for
/// [`InvalidJustification`], a single signature plus the opening it commits to.
///
/// **The committee matters.** [`EquivocationProof::check`] judges the exhibited signatures against
/// whatever [`Committee`] the auditor supplies, and [`InvalidJustification`] in particular asks
/// whether an opening was a quorum *of that committee*. A vote is honest relative to the committee
/// of the epoch it was cast in, so throughout this module a proof is understood to be adjudicated
/// against that committee. Judging a vote against a different epoch's committee could convict a
/// correct validator, and nothing in [`EquivocationProof::check`] prevents an auditor from doing
/// so — the epoch is not carried in the proof.
///
/// [`EquivocationProof`]: crate::justification::EquivocationProof
/// [`EquivocationProof::check`]: crate::justification::EquivocationProof::check
/// [`InvalidJustification`]: crate::justification::EquivocationProof::InvalidJustification
/// [`Committee`]: linera_execution::committee::Committee
pub trait MisbehaviourProof {}

/// **Lemma (Double votes are never honest).** No correct validator is named by an accepted
/// [`DoubleVote`] proof.
///
/// *Proof.* An accepted proof exhibits two signatures by `v` over [`VoteValue`]s that agree on
/// round and kind, whose headers share a chain and height, and whose hashes differ; by
/// [`UnforgeableSignatures`] only `v` could have produced them, so a correct `v` cast both votes.
/// Sharing a chain and height means both votes belong to the same [`ConsensusInstance`] — a reset
/// changes the height, and [`SafetyStateRecovery`] shows the one path that recreates an instance
/// at an unchanged height preserves the votes rather than forgetting them. Then:
///
/// * `kind = Validated` contradicts [`OneValidationVotePerRound`];
/// * `kind = Confirmed` contradicts [`OneConfirmationVotePerRound`];
/// * `kind = Timeout` is not realizable at all: the proof carries [`BlockHeader`]s and checks the
///   signature over `CryptoHash::new(header)`, whereas a timeout vote signs the hash of a
///   [`Timeout`] value, so an accepted timeout-kind proof would require a block header colliding
///   with a `Timeout` — excluded by [`UnforgeableSignatures`]. ∎
///
/// [`DoubleVote`]: crate::justification::EquivocationProof::DoubleVote
/// [`VoteValue`]: crate::data_types::VoteValue
/// [`BlockHeader`]: crate::block::BlockHeader
/// [`Timeout`]: crate::block::Timeout
pub trait DoubleVoteSoundness:
    UnforgeableSignatures
    + OneValidationVotePerRound
    + OneConfirmationVotePerRound
    + ConsensusInstance
    + SafetyStateRecovery
{
}

/// **Lemma (First-round attestations are never honestly contradicted).** No correct validator is
/// named by an accepted [`FirstRoundViolation`] proof.
///
/// *Proof.* An accepted proof exhibits `v`'s confirmation vote at round `a` carrying the
/// attestation, and `v`'s confirmation vote at a round `b < a` on the same chain and height; by
/// [`UnforgeableSignatures`] a correct `v` cast both, in the same instance.
///
/// Both sites that set the attestation — the fast branch of [`ChainManager::create_vote`] and
/// [`ChainManager::create_final_vote`] ([`VoteConstructionSites`]) — compute it as
/// `round == self.ownership.get().first_round()`. The `ownership` register has exactly one
/// writer, [`ChainManager::reset`], which by [`ConsensusInstance`] begins the instance, so
/// `first_round()` is a constant `φ` throughout. The attestation at `a` therefore gives `a = φ`.
///
/// Now consider the vote at `b < a = φ`. By [`VoteConstructionSites`] it is either:
///
/// * [`ChainManager::create_final_vote`], which by [`ConfirmationOnlyInCurrentRound`] requires
///   `current_round == b`. But [`RoundFloor`] makes `current_round ≥ φ = a > b` at all times.
///   Contradiction.
/// * the fast branch, so `b = Round::Fast`. Since `φ > b`, `φ` is not `Round::Fast`, which by
///   [`ChainOwnership::first_round`] means `super_owners` is empty. But a fast-round proposal is
///   rejected with `WorkerError::InvalidOwner` unless its proposer is a super owner
///   ([`LeaderEligibility`]: `can_propose` returns `false` for `Round::Fast` for everyone else),
///   so `v` never reaches the fast branch. Contradiction. ∎
///
/// [`FirstRoundViolation`]: crate::justification::EquivocationProof::FirstRoundViolation
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ChainManager::reset`]: crate::manager::ChainManager::reset
/// [`ChainOwnership::first_round`]: linera_base::ownership::ChainOwnership::first_round
pub trait FirstRoundSoundness:
    UnforgeableSignatures
    + VoteConstructionSites
    + ConfirmationOnlyInCurrentRound
    + RoundFloor
    + LeaderEligibility
    + ConsensusInstance
{
}

/// **Lemma (Lock violations are never honest).** No correct validator is named by an accepted
/// [`LockViolation`] proof — subject to the residual obligation below.
///
/// *Proof.* An accepted proof exhibits `v`'s confirmation vote for `X` at round `r` and `v`'s
/// validation vote for `Y` at round `s`, with `hash(X) ≠ hash(Y)`, the same chain and height,
/// `r < s`, and a signed unlocking round `u` with `u ≤ r` (or `u = None`). Suppose `v` correct.
///
/// *The confirmation came first.* Otherwise, after the validation at `s`,
/// [`VoteRoundBelowCurrentRound`] and [`CurrentRoundMonotone`] pin `v`'s current round at `≥ s`.
/// A later confirmation at `r < s` is then impossible: via [`ChainManager::create_final_vote`] it
/// would need `current_round == r` ([`ConfirmationOnlyInCurrentRound`]); via the fast branch it
/// would need `r = Round::Fast` and, by [`FastConfirmationNeedsEmptyLock`], an empty lock and no
/// validation vote — contradicting the floor `≥ s` that the validation at `s` established.
///
/// *So let `(X', p)` be `v`'s stored confirmation vote just before the validation at `s`.* It is
/// present, and by [`ConfirmedVoteRoundMonotone`], `p ≥ r`. Apply
/// [`UnlockingRequiresHigherCertificate`] to the validation vote, by the shape of its proposal:
///
/// * *Fresh proposal.* Rejected outright — `v` would not have voted.
/// * *Regular retry with certificate `c`, and `X'` not matching `Y`'s proposal.* The guard is
///   `p < c.round`, and `c.round` is exactly the signed `u`. With `u ≤ r ≤ p` this gives
///   `p < u ≤ p`. Contradiction.
/// * *Regular retry with `X'` matching `Y`'s proposal.* The guard is `p ≤ u`, so
///   `p ≤ u ≤ r ≤ p` forces `p = u = r`.
/// * *Fast retry.* The signed `u` is `None`, and the guard forces `p = Round::Fast` and `X'`
///   matching `Y`'s proposal; with `Round::Fast` minimal and `p ≥ r`, again `p = r`.
///
/// The last two cases coincide: `v` confirmed `X` at round `r` and its stored confirmation at the
/// same round `r = p` is `X'`, so [`OneConfirmationVotePerRound`] gives `X = X'`. Hence `X`
/// matches `Y`'s [`ProposedBlock`] while `hash(X) ≠ hash(Y)` — the two blocks share a proposal and
/// differ only in [`BlockExecutionOutcome`]. Sharing a proposal means sharing a
/// `previous_block_hash`, so by [`UnforgeableSignatures`] (collision resistance) they have the
/// same parent and hence the same ancestry and the same pre-state, and
/// [`DeterministicExecution`] makes the outcome a function of that pre-state and the proposal.
/// So `X = Y`, contradicting `hash(X) ≠ hash(Y)`. ∎
///
/// **Residual obligation.** Only the first two cases are unconditional; the last two are closed by
/// [`DeterministicExecution`], the same hinge as [`FastRetryPreservesBlock`]. The ancestry
/// argument avoids circularity — it follows `previous_block_hash` down rather than appealing to
/// [`UniqueChain`] — but an execution engine whose outcome depends on the round without recording
/// an `OracleResponse::Round` would make a correct validator convictable here.
///
/// [`LockViolation`]: crate::justification::EquivocationProof::LockViolation
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`ProposedBlock`]: crate::data_types::ProposedBlock
/// [`BlockExecutionOutcome`]: crate::data_types::BlockExecutionOutcome
/// [`FastRetryPreservesBlock`]: crate::manager::proof::safety::FastRetryPreservesBlock
/// [`UniqueChain`]: crate::manager::proof::safety::UniqueChain
pub trait LockViolationSoundness:
    UnforgeableSignatures
    + UnlockingRequiresHigherCertificate
    + ConfirmedVoteRoundMonotone
    + ConfirmationOnlyInCurrentRound
    + FastConfirmationNeedsEmptyLock
    + OneConfirmationVotePerRound
    + VoteRoundBelowCurrentRound
    + CurrentRoundMonotone
    + DeterministicExecution
{
}

/// **Lemma (Attested justifications are never honestly invalid).** No correct validator is named
/// by an accepted [`InvalidJustification`] proof, when the proof is adjudicated against the
/// committee of the vote's own epoch ([`MisbehaviourProof`]).
///
/// *Proof.* An accepted proof exhibits `v`'s signature over a [`VoteValue`] whose justification
/// commitment is `opening.commitment()`, together with an `opening` on which `check_cited_quorum`
/// fails. By [`UnforgeableSignatures`] a correct `v` produced that signature, so it is one of the
/// five sites of [`VoteConstructionSites`]. Take them in turn.
///
/// * [`ChainManager::create_timeout_vote`], [`ChainManager::vote_fallback`], and the fast branch
///   of [`ChainManager::create_vote`] all sign a commitment of `None`. A proof requires
///   `Some(opening.commitment())`, so its signature check fails and it is not accepted.
/// * *The non-fast branch of [`ChainManager::create_vote`], on a regular retry.* It signs
///   `unlocking_round = Some(c.round)` and `Some(c.full_justification_commitment())`, whose
///   opening is `c`'s own quorum. `check_cited_quorum` then asks exactly the four things that
///   verifying `c` already established: that the opening's `value_hash` is the voted block's hash
///   (given by [`BlockProposal::check_invariants`], which binds `c` to the proposed block); that
///   `unlocking_round == Some(opening.round)` and `opening.round < round` (the first by
///   construction, the second by `check_invariants`' `content.round > certificate.round`); that
///   the opening's own unlocking round and previous commitment are both present or both absent
///   (from [`LiteCertificate::check`], where a `Validated` certificate's `unlocking_round` equals
///   its chain's top and its commitment is `None` exactly when the chain is empty); and that the
///   opening's signatures form a quorum over the reconstructed `Validated` payload — which is
///   verbatim the check [`LiteCertificate::check`] performed on `c`, the `first_round` component
///   being `false` for every [`ValidatedBlockCertificate`]. A fresh or fast-retry proposal signs
///   `None` and is covered by the first case.
/// * *[`ChainManager::create_final_vote`].* It signs `unlocking_round = None`,
///   `Some(validated.full_justification_commitment())` — or `None` in the chain's first round,
///   again covered above — in the round `validated.round`. For `kind = Confirmed`
///   `check_cited_quorum` requires `opening.round == round`, which holds since the vote's round
///   *is* `validated.round`; the remaining conditions are as in the previous case, `validated`
///   having been verified by the caller ([`ConfirmationNeedsValidatedCertificate`]). ∎
///
/// [`InvalidJustification`]: crate::justification::EquivocationProof::InvalidJustification
/// [`VoteValue`]: crate::data_types::VoteValue
/// [`ChainManager::create_timeout_vote`]: crate::manager::ChainManager::create_timeout_vote
/// [`ChainManager::vote_fallback`]: crate::manager::ChainManager::vote_fallback
/// [`ChainManager::create_vote`]: crate::manager::ChainManager::create_vote
/// [`ChainManager::create_final_vote`]: crate::manager::ChainManager::create_final_vote
/// [`BlockProposal::check_invariants`]: crate::data_types::BlockProposal::check_invariants
/// [`LiteCertificate::check`]: crate::types::LiteCertificate::check
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`ConfirmationNeedsValidatedCertificate`]: crate::manager::proof::voting::ConfirmationNeedsValidatedCertificate
pub trait InvalidJustificationSoundness:
    UnforgeableSignatures + VoteConstructionSites + MisbehaviourProof
{
}

/// **Theorem (Soundness — no correct validator is convictable).** If
/// [`EquivocationProof::check`] accepts a proof naming `v` against the committee of the epoch its
/// votes were cast in, then `v` is faulty in the sense of [`CorrectValidator`].
///
/// *Proof.* By cases on the four variants: [`DoubleVoteSoundness`], [`FirstRoundSoundness`],
/// [`LockViolationSoundness`] and [`InvalidJustificationSoundness`]. ∎
///
/// Note what this does *not* assume: no [`MaxByzantineWeight`], no synchrony, no bound on how many
/// other validators misbehaved. Soundness is a statement about one validator's own signatures, so
/// it survives arbitrary corruption of everyone else — which is what makes a conviction meaningful
/// in the regime where accountability is invoked.
///
/// [`EquivocationProof::check`]: crate::justification::EquivocationProof::check
/// [`CorrectValidator`]: crate::manager::proof::model::CorrectValidator
/// [`MaxByzantineWeight`]: crate::manager::proof::model::MaxByzantineWeight
pub trait ProofSoundness:
    DoubleVoteSoundness + FirstRoundSoundness + LockViolationSoundness + InvalidJustificationSoundness
{
}

/// **Definition (Sound justification chain).** A [`JustificationChain`] carried by a confirmed
/// certificate for block `B` is *sound* when every link's signatures form a quorum over the
/// `Validated` payload reconstructed for that link — the payload with `B`'s hash, the link's
/// round, the previous link's round as unlocking round, and the previous link's commitment.
///
/// [`audit_confirmation`] returns an empty list exactly when the chain is sound: it reconstructs
/// each link's payload in order and calls `check_signatures` on it, returning at the first
/// failure. Soundness is *not* implied by the certificate verifying:
/// [`LiteCertificate::check`] deliberately skips the links, relying instead on the attestation
/// carried by the quorum above them, which is what makes [`ChainAuditability`] the fallback.
///
/// [`JustificationChain`]: crate::justification::JustificationChain
/// [`audit_confirmation`]: crate::justification::audit_confirmation
/// [`LiteCertificate::check`]: crate::types::LiteCertificate::check
pub trait SoundChain {}

/// **Lemma (A sound chain tiles every round below the confirmation).** Let a valid
/// [`ConfirmedBlockCertificate`] in round `s` carry a non-empty chain with link rounds
/// `ρ₀ < ρ₁ < … < ρₖ`. Then `ρₖ = s`, link `i` was cast under unlocking round `ρᵢ₋₁` (and link `0`
/// under `None`), and the half-open windows
///
/// ```text
/// [⊥, ρ₀),  [ρ₀, ρ₁),  …,  [ρₖ₋₁, ρₖ)
/// ```
///
/// partition the rounds strictly below `s`. In particular every round `r < s` lies in exactly one
/// link's window.
///
/// *Proof.* [`JustificationChain::verify`] rejects unless the rounds strictly increase, and
/// [`LiteCertificate::check`] on a `Confirmed` certificate with a non-empty chain requires
/// `top == self.round`, i.e. `ρₖ = s`. [`JustificationChain::commitment`] folds the chain from the
/// bottom, setting each link's `unlocking_round` to the round of the link below and `None` for the
/// first — so the reconstructed payload of link `i` carries unlocking round `ρᵢ₋₁`, which is the
/// window's lower bound; the upper bound `ρᵢ` is where the link's own votes were cast. Consecutive
/// windows abut and the first is unbounded below, so their union is `[⊥, ρₖ) = [⊥, s)`. ∎
///
/// This is what makes the chain walk in [`extract_equivocations`] exhaustive rather than
/// best-effort: a lower confirmation cannot slip between two links.
///
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`JustificationChain::verify`]: crate::justification::JustificationChain::verify
/// [`JustificationChain::commitment`]: crate::justification::JustificationChain::commitment
/// [`LiteCertificate::check`]: crate::types::LiteCertificate::check
/// [`extract_equivocations`]: crate::justification::extract_equivocations
pub trait ChainTilesRounds {}

/// **Theorem (Completeness — a conflict convicts a validity threshold).** Let two valid
/// [`ConfirmedBlockCertificate`]s for conflicting blocks at the same chain and height, valid for
/// the *same* committee and carrying sound chains ([`SoundChain`]), be certified in rounds
/// `r ≤ s`. Then [`extract_equivocations`] applied to their [`JustifiedConfirmation`]s returns
/// proofs that [`EquivocationProof::check`] accepts, naming validators of total weight at least
/// [`Committee::validity_threshold`].
///
/// *Proof.* By [`CertificateEmbedsQuorum`] each certificate's confirmation signatures form a
/// quorum, and by [`ChainTilesRounds`] so does each link of a sound chain. Three cases, which are
/// exactly the three the implementation tries.
///
/// * **`r = s`.** `double_confirm` walks the intersection of the two confirmation quorums, which
///   by [`Intersection`] has weight at least `f⁺`, emitting a [`DoubleVote`] for each member. Each
///   is accepted: the blocks differ, the chain and height agree, and both signatures were taken
///   from verified quorums.
/// * **`r < s` and the higher certificate carries a chain.** By [`ChainTilesRounds`] some link's
///   window contains `r`, i.e. `link.round > r` and its unlocking round is `≤ r` — precisely
///   `walk_chain`'s guard. That link is a quorum, so its intersection with the lower confirmation
///   quorum has weight at least `f⁺` by [`Intersection`], and each member gets a
///   [`LockViolation`]. Each is accepted: `check` re-derives the same window condition
///   `confirmed_round < validated_round` and `validated_unlocking_round ≤ confirmed_round`.
/// * **`r < s` and the higher certificate carries no chain.** Then [`LiteCertificate::check`]
///   accepted it only because its `first_round` attestation is set. `first_round_violation` walks
///   the intersection of the two confirmation quorums — weight at least `f⁺` — emitting a
///   [`FirstRoundViolation`] for each, accepted since `earlier_round = r < s = attested_round`.
///
/// In every case the blamed set is a full quorum intersection. ∎
///
/// *Depends on the shared-committee hypothesis.* [`Intersection`] compares quorums of one
/// committee; [`extract_equivocations`] checks only the chain and height, not the epoch, so two
/// certificates declaring different epochs could yield no intersection at all. An auditor must
/// establish that both certificates are valid for the same committee before drawing the
/// conclusion.
///
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`extract_equivocations`]: crate::justification::extract_equivocations
/// [`JustifiedConfirmation`]: crate::justification::JustifiedConfirmation
/// [`EquivocationProof::check`]: crate::justification::EquivocationProof::check
/// [`Committee::validity_threshold`]: linera_execution::committee::Committee::validity_threshold
/// [`DoubleVote`]: crate::justification::EquivocationProof::DoubleVote
/// [`LockViolation`]: crate::justification::EquivocationProof::LockViolation
/// [`FirstRoundViolation`]: crate::justification::EquivocationProof::FirstRoundViolation
/// [`LiteCertificate::check`]: crate::types::LiteCertificate::check
pub trait ConflictCompleteness:
    ChainTilesRounds + SoundChain + Intersection + CertificateEmbedsQuorum + ThresholdArithmetic
{
}

/// **Lemma (An unsound chain convicts its attesters).** If a confirmed certificate's chain is not
/// sound ([`SoundChain`]), [`audit_confirmation`] returns a non-empty list of proofs that
/// [`EquivocationProof::check`] accepts.
///
/// *Proof.* [`audit_confirmation`] walks the links upward and stops at the lowest one whose
/// reconstructed payload fails `check_signatures`. Every validator at the level immediately above
/// — the next link, or the confirmation quorum if the bad link is the top one — signed a payload
/// whose justification commitment is that link's `CommittedQuorum` hash, so each receives an
/// [`InvalidJustification`] carrying that signature and that opening. Each is accepted:
/// [`EquivocationProof::check`] verifies the signature against the reconstructed payload and then
/// requires `check_cited_quorum` to fail, which it does, the opening's signatures not forming a
/// quorum. ∎
///
/// **Weaker than [`ConflictCompleteness`], deliberately.** The blamed set is a quorum only when
/// the bad link is the top one, where the accusers are the certificate's own — verified —
/// confirmation quorum. Lower down, the accusers are the next link, whose own signatures the audit
/// has not yet reached, so they may be fewer than a quorum. Each individual proof is still
/// accepted, and an unsound level above is itself auditable one step further up; what is not
/// guaranteed is a `f⁺`-weight blame set from a single pass. Repairing that would mean verifying
/// links during certificate checking, which is the cost the attestation scheme exists to avoid.
///
/// [`audit_confirmation`]: crate::justification::audit_confirmation
/// [`EquivocationProof::check`]: crate::justification::EquivocationProof::check
/// [`InvalidJustification`]: crate::justification::EquivocationProof::InvalidJustification
pub trait ChainAuditability: SoundChain + CertificateEmbedsQuorum {}

/// **Lemma (Two validated blocks in one round convict a validity threshold).** If two valid
/// [`ValidatedBlockCertificate`]s for conflicting blocks are certified in the *same* round and are
/// valid for the same committee, [`extract_double_validations`] returns accepted [`DoubleVote`]
/// proofs naming validators of total weight at least [`Committee::validity_threshold`].
///
/// *Proof.* By [`CertificateEmbedsQuorum`] both signature sets are quorums; by [`Intersection`]
/// their intersection has weight at least `f⁺`; `double_vote` emits a proof for each member, with
/// `kind = Validated` and the round both share. Each is accepted, the blocks differing and the
/// chain and height agreeing. ∎
///
/// This is the accountability counterpart of
/// [`UniqueValidatedBlockPerRound`](crate::manager::proof::locking::UniqueValidatedBlockPerRound):
/// that lemma says the situation cannot arise below the fault bound, this one says it is
/// attributable if it does. Note the round equality is required — validating conflicting blocks in
/// *different* rounds is legitimate, which is exactly what locks exist to regulate.
///
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`extract_double_validations`]: crate::justification::extract_double_validations
/// [`DoubleVote`]: crate::justification::EquivocationProof::DoubleVote
/// [`Committee::validity_threshold`]: linera_execution::committee::Committee::validity_threshold
pub trait DoubleValidationCompleteness:
    Intersection + CertificateEmbedsQuorum + ThresholdArithmetic
{
}

/// **Theorem (Accountable safety).** For every chain and height, one of the following holds:
///
/// 1. at most one block is committed there ([`CommitAgreement`]); or
/// 2. two conflicting confirmed certificates exist, and then — from those certificates alone,
///    with no further observation of the network — validators of total weight at least
///    [`Committee::validity_threshold`] are convictable by proofs that
///    [`EquivocationProof::check`] accepts, every one of them genuinely faulty
///    ([`ProofSoundness`]); or
/// 3. a certificate's justification chain is unsound, and then its attesters are convictable
///    ([`ChainAuditability`]) with no conflict required at all.
///
/// *Proof.* Case 1 is [`CommitAgreement`], which holds whenever
/// [`MaxByzantineWeight`](crate::manager::proof::model::MaxByzantineWeight) does. If it fails,
/// there are conflicting confirmed certificates; if both carry sound chains, case 2 is
/// [`ConflictCompleteness`] together with [`ProofSoundness`], and otherwise case 3 is
/// [`ChainAuditability`]. ∎
///
/// Case 2 convicts strictly more weight than the fault bound permits — `f⁺` against a permitted
/// `f⁺ − 1` — so a conviction set is itself evidence that the assumption underpinning
/// [`CommitAgreement`] was violated, rather than merely that some validator misbehaved.
///
/// [`CommitAgreement`]: crate::manager::proof::safety::CommitAgreement
/// [`EquivocationProof::check`]: crate::justification::EquivocationProof::check
/// [`Committee::validity_threshold`]: linera_execution::committee::Committee::validity_threshold
pub trait AccountableSafety:
    ProofSoundness + ConflictCompleteness + ChainAuditability + CommitAgreement
{
}

/// **Remark (What accountability does not cover).** Four exclusions, the third being the
/// substantive one.
///
/// * **There is no adjudicator.** [`EquivocationProof`] is constructed and verified nowhere
///   outside this module and its tests: no slashing operation consumes one, and no chain records
///   one. [`AccountableSafety`] establishes *convictability* — that the evidence exists, is
///   self-contained and verifies — not any protocol consequence. Wiring an adjudicator in would
///   need a place to submit proofs and a stake to forfeit, neither of which exists today.
///
/// * **Only equivocation is attributable, not silence.** A validator that simply stops voting, or
///   answers some clients and not others, produces no contradictory signature and is unconvictable.
///   Liveness faults are outside the scheme by construction, which is why
///   `linera_core::proof::assumptions::CorrectValidatorAvailability` is an assumption rather than
///   something enforced.
///
/// * **Incorrect execution is not attributable.** Nothing in [`EquivocationProof`] relates a
///   block's [`ProposedBlock`] to its [`BlockExecutionOutcome`]. A validator that votes for
///   exactly one block per round, with a sound chain, but whose block carries a fabricated
///   `state_hash`, yields no proof at all. What the implementation has instead is *local
///   detection*: `ChainWorkerState::execute_contiguous_block` re-executes the block and rejects a
///   mismatch with [`ChainError::CorruptedChainState`]. That is unilateral — the detecting node
///   holds nothing transferable, and any peer must redo the work — and it is incomplete in three
///   ways: the certificate's `oracle_responses` are *replayed* into the re-execution rather than
///   re-derived, so a fabricated oracle answer reproduces the same state hash and is never caught;
///   `preprocess_certified_block` does not execute at all; and the `execution_state_cache` hit
///   path skips re-execution. The property that does protect against a bad outcome is
///   [`CertifiedBlockWasExecuted`], and unlike everything else in this module it needs
///   [`MaxByzantineWeight`](crate::manager::proof::model::MaxByzantineWeight): validity degrades
///   above the fault bound with no forensic residue, whereas agreement degrades with one.
///
/// * **The blame set is a threshold, not a census.** [`ConflictCompleteness`] names *a* quorum
///   intersection; other validators may have equivocated without appearing in it, and running
///   [`extract_equivocations`] on further certificate pairs may name more.
///
/// [`EquivocationProof`]: crate::justification::EquivocationProof
/// [`ProposedBlock`]: crate::data_types::ProposedBlock
/// [`BlockExecutionOutcome`]: crate::data_types::BlockExecutionOutcome
/// [`ChainError::CorruptedChainState`]: crate::ChainError::CorruptedChainState
/// [`extract_equivocations`]: crate::justification::extract_equivocations
pub trait AccountabilityScope:
    AccountableSafety
    + DoubleValidationCompleteness
    + CertifiedBlockWasExecuted
    + CommitRestsOnValidation
{
}
