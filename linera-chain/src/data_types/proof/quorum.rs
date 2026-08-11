// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Quorum properties: what a quorum of validator signatures buys us.
//!
//! Throughout this module, fix a [`Committee`] and write
//!
//! * `N` for [`Committee::total_votes`],
//! * `q` for [`Committee::quorum_threshold`],
//! * `f⁺` for [`Committee::validity_threshold`],
//! * `w(S)` for the sum of [`Committee::weight`] over a set `S` of validators.
//!
//! These are the only results in the specification that reason about weights arithmetically.
//! Everything above this module consumes them through [`CorrectValidatorInIntersection`],
//! [`CorrectSignerCastItsVote`], [`CertificateCarriesCorrectVote`] and
//! [`CorrectValidatorsFormQuorum`].
//!
//! [`Committee`]: linera_execution::committee::Committee
//! [`Committee::total_votes`]: linera_execution::committee::Committee::total_votes
//! [`Committee::quorum_threshold`]: linera_execution::committee::Committee::quorum_threshold
//! [`Committee::validity_threshold`]: linera_execution::committee::Committee::validity_threshold
//! [`Committee::weight`]: linera_execution::committee::Committee::weight

use crate::manager::proof::model::{CorrectValidator, MaxByzantineWeight, UnforgeableSignatures};

/// **Definition (Quorum).** A *quorum* of a committee is a set `S` of pairwise distinct
/// committee members with `w(S) ≥ q`.
///
/// Note that this is a statement about *weight*, not cardinality: [`Committee::weight`] returns
/// `0` for a non-member, so a set containing non-members is never helped by them.
///
/// [`Committee::weight`]: linera_execution::committee::Committee::weight
pub trait Quorum {}

/// **Lemma (Threshold arithmetic).** `f⁺ = ⌈N/3⌉` and `2·q ≥ N + f⁺`.
///
/// *Proof.* [`Committee::new`] computes `validity_threshold = total_votes.div_ceil(3)`, which is
/// `⌈N/3⌉` by definition of `div_ceil`, and
/// `quorum_threshold = (total_votes + validity_threshold).div_ceil(2)`, i.e.
/// `q = ⌈(N + f⁺)/2⌉ ≥ (N + f⁺)/2`. Multiplying the second by two gives the claim.
///
/// Both fields are stored rather than recomputed on use, so the proof needs them to be
/// trustworthy on a committee that arrived over the network. They are: the `Deserialize` impl
/// for [`Committee`] recomputes both from the validator weights and rejects the committee unless
/// they agree with the serialized values (`CommitteeFull`'s `TryFrom` in
/// `linera_execution::committee`). Hence every [`Committee`] reaching consensus code satisfies
/// the two identities above. ∎
///
/// [`Committee`]: linera_execution::committee::Committee
/// [`Committee::new`]: linera_execution::committee::Committee::new
pub trait ThresholdArithmetic {}

/// **Lemma (Quorum intersection).** Any two quorums `S₁`, `S₂` of the same committee satisfy
/// `w(S₁ ∩ S₂) ≥ f⁺`.
///
/// *Proof.* `S₁ ∪ S₂` is a set of committee members, so `w(S₁ ∪ S₂) ≤ N`. Weights are additive
/// over disjoint sets, so inclusion–exclusion gives
///
/// ```text
/// w(S₁ ∩ S₂) = w(S₁) + w(S₂) − w(S₁ ∪ S₂) ≥ q + q − N ≥ (N + f⁺) − N = f⁺,
/// ```
///
/// where the first inequality is [`Quorum`] applied to `S₁` and `S₂`, and the second is
/// [`ThresholdArithmetic`]. ∎
pub trait Intersection: ThresholdArithmetic {}

/// **Corollary (A correct validator lies in every quorum intersection).** Any two quorums of the
/// same committee share at least one correct validator.
///
/// *Proof.* By [`Intersection`] the intersection has weight at least `f⁺`. By
/// [`MaxByzantineWeight`] the total weight of faulty validators is at most `f⁺ − 1 < f⁺`, so the
/// intersection cannot consist of faulty validators alone. ∎
pub trait CorrectValidatorInIntersection: Intersection + MaxByzantineWeight {}

/// **Lemma (A valid certificate embeds a quorum).** If [`LiteCertificate::check`] returns `Ok`
/// for a certificate `c` against a committee, then the signers of `c.signatures` form a
/// [`Quorum`] of that committee.
///
/// *Proof.* [`LiteCertificate::check`] passes `c.signatures` to the crate-private helper
/// `check_signatures`, which (1) rejects a repeated signer with
/// [`ChainError::CertificateValidatorReuse`], establishing pairwise distinctness; (2) rejects any
/// signer whose [`Committee::weight`] is `0` with [`ChainError::InvalidSigner`], establishing
/// committee membership; and (3) accumulates the signers' weights and rejects a total below
/// [`Committee::quorum_threshold`] with [`ChainError::CertificateRequiresQuorum`], establishing
/// `w(S) ≥ q`. Those three are precisely [`Quorum`]. ∎
///
/// The signatures themselves are [`CertificateSignaturesVerify`].
///
/// [`LiteCertificate::check`]: crate::types::LiteCertificate::check
/// [`ChainError::CertificateValidatorReuse`]: crate::ChainError::CertificateValidatorReuse
/// [`ChainError::InvalidSigner`]: crate::ChainError::InvalidSigner
/// [`ChainError::CertificateRequiresQuorum`]: crate::ChainError::CertificateRequiresQuorum
/// [`Committee::weight`]: linera_execution::committee::Committee::weight
/// [`Committee::quorum_threshold`]: linera_execution::committee::Committee::quorum_threshold
pub trait CertificateEmbedsQuorum {}

/// **Lemma (Every signature on a valid certificate verifies).** If [`LiteCertificate::check`]
/// returns `Ok` for a certificate `c`, then *each individual* signer of `c.signatures` holds a
/// signature that verifies over the single [`SignedVotePayload`]
///
/// ```text
/// (c.value.value_hash, c.round, c.value.kind, c.unlocking_round, c.first_round,
///  c.justification_commitment)
/// ```
///
/// *Proof.* [`LiteCertificate::check`] constructs exactly that
/// [`VoteValue`](crate::data_types::VoteValue) and passes it, with `c.signatures`, to
/// `check_signatures`, whose final step calls `ValidatorSignature::verify_batch` and propagates
/// any failure. For [`ValidatorSignature`] — an alias of `Secp256k1Signature` — that function is
/// a loop of individual verifications returning on the first failure:
///
/// ```text
/// for (author, signature) in votes {
///     signature.verify_inner::<T>(prehash, *author)?;
/// }
/// ```
///
/// so acceptance of the whole is acceptance of each. ∎
///
/// **This is a property of the signature scheme, not of `check_signatures`.** The inference "the
/// batch verified, therefore this particular signature verified" is a tautology only for a loop.
/// `Ed25519Signature::verify_batch` — present in the crate, and marked unused in consensus — is a
/// genuine batch scheme that differs in two ways:
///
/// * its soundness is *probabilistic*: it draws a random 128-bit scalar per signature and checks
///   one aggregate equation, so an invalid signature survives with negligible but nonzero
///   probability;
/// * it does not accept the same set as single verification. `VerifyingKey::verify_strict`
///   rejects small-order components explicitly and the batch path does not, and `ed25519-dalek`'s
///   own documentation describes a malleability under which a mutated signature passes the batch
///   though it "will not pass single signature verification".
///
/// Switching [`ValidatorSignature`] to such a scheme would leave
/// [`CertificateCarriesCorrectVote`] standing — it needs only that *some* signature was produced
/// by a correct validator — while silently invalidating this lemma and everything pointwise built
/// on it. [`ConflictCompleteness`] is the sharp end: it extracts signatures out of certificates
/// and re-verifies them *individually* through [`EquivocationProof::check`], so a batch/single
/// divergence would make [`extract_equivocations`] emit proofs that verification rejects,
/// breaking accountability completeness exactly where it is needed.
///
/// [`LiteCertificate::check`]: crate::types::LiteCertificate::check
/// [`SignedVotePayload`]: super::objects::SignedVotePayload
/// [`ValidatorSignature`]: linera_base::crypto::ValidatorSignature
/// [`ConflictCompleteness`]: crate::justification::proof::ConflictCompleteness
/// [`EquivocationProof::check`]: crate::justification::EquivocationProof::check
/// [`extract_equivocations`]: crate::justification::extract_equivocations
pub trait CertificateSignaturesVerify {}

/// **Lemma (A correct signer of a valid certificate cast its vote).** If a certificate `c` is
/// valid for a committee and a *correct* validator `v` is among its signers, then `v` itself cast
/// a vote with `c`'s exact signed payload.
///
/// The validator is identified by other means — in practice by
/// [`CorrectValidatorInIntersection`]. [`CertificateCarriesCorrectVote`] is the existential form.
///
/// *Proof.* By [`CertificateSignaturesVerify`], `v`'s signature verifies over `c`'s payload. By
/// [`UnforgeableSignatures`] no party without `v`'s secret key produces a signature that verifies
/// for `v`, so `v` produced it. By [`CorrectValidator`], a correct validator's signatures are
/// produced only by this code driven through its public entry points, so producing that signature
/// means casting that vote. ∎
///
/// Note what this does *not* need: no [`MaxByzantineWeight`], no [`ThresholdArithmetic`], no
/// [`Quorum`]. The hypothesis already hands us the validator, so none of the weight machinery is
/// involved — which is why this form, unlike the existential one, is available to
/// [`crate::justification::proof`], where the fault bound is assumed *not* to hold.
pub trait CorrectSignerCastItsVote:
    CertificateSignaturesVerify + UnforgeableSignatures + CorrectValidator
{
}

/// **Corollary (Every valid certificate carries a correct validator's vote).** For every
/// certificate valid for its committee there is at least one *correct* validator that itself cast
/// a vote with that certificate's exact signed payload.
///
/// This turns "a certificate exists" into "a correct validator executed the code path that
/// produces this vote", and thereby lets the local implementation properties of
/// [`crate::manager::proof::voting`] constrain what certificates can exist at all. An argument
/// about a *particular* validator needs [`CorrectSignerCastItsVote`].
///
/// *Proof.* By [`CertificateEmbedsQuorum`] the signers form a quorum, of weight at least `q`. By
/// [`ThresholdArithmetic`], `q ≥ (N + f⁺)/2 ≥ f⁺`, using `N ≥ f⁺ = ⌈N/3⌉`. By
/// [`MaxByzantineWeight`] faulty validators hold at most `f⁺ − 1`, so some signer is correct.
/// Apply [`CorrectSignerCastItsVote`] to it. ∎
pub trait CertificateCarriesCorrectVote:
    CorrectSignerCastItsVote + CertificateEmbedsQuorum + ThresholdArithmetic + MaxByzantineWeight
{
}

/// **Lemma (The correct validators alone form a quorum).** The set of all correct validators of
/// a committee is a [`Quorum`].
///
/// This is the availability counterpart of [`Intersection`]: it is what lets a correct proposer
/// collect a certificate without any cooperation from faulty validators, and so it is a
/// dependency of every progress result rather than of any safety result.
///
/// *Proof.* By [`MaxByzantineWeight`] the correct validators hold weight at least `N − f⁺ + 1`,
/// so it suffices to show `N − f⁺ + 1 ≥ q`. Write `N = 3k + i` with `i ∈ {0, 1, 2}`; by
/// [`ThresholdArithmetic`], `f⁺ = ⌈N/3⌉`.
///
/// ```text
/// i = 0:  f⁺ = k,      q = ⌈(4k)/2⌉     = 2k,     N − f⁺ + 1 = 2k + 1 ≥ q.
/// i = 1:  f⁺ = k + 1,  q = ⌈(4k+2)/2⌉   = 2k + 1, N − f⁺ + 1 = 2k + 1 ≥ q.
/// i = 2:  f⁺ = k + 1,  q = ⌈(4k+3)/2⌉   = 2k + 2, N − f⁺ + 1 = 2k + 2 ≥ q.
/// ```
///
/// In each case the correct validators reach the quorum threshold. ∎
pub trait CorrectValidatorsFormQuorum: ThresholdArithmetic + MaxByzantineWeight {}

/// **Remark (Where quorums are assembled, and why that is not load-bearing).** Certificates are
/// built in two places, with different guarantees:
///
/// * [`SignatureAggregator::append`] performs the same three membership/distinctness/threshold
///   checks as [`CertificateEmbedsQuorum`] incrementally, and verifies each signature as it is
///   added, so a certificate it emits is valid by construction.
/// * [`LiteCertificate::try_from_votes`] does **not** verify signatures, and instead requires
///   its caller to have done so. Its one production caller,
///   `linera_core::Client::communicate_chain_action`, verifies each vote against the responding
///   validator's public key (in `linera_core::updater`) before aggregating, and groups incoming
///   votes by their *full* signed payload — all six components of
///   [`SignedVotePayload`](super::objects::SignedVotePayload) — so votes that disagree on the
///   unlocking round, the first-round attestation or the justification commitment are never
///   merged into one certificate.
///
/// Neither fact is load-bearing for safety. A certificate that a correct validator *acts on* is
/// re-verified through [`LiteCertificate::check`] at every entry point that consumes one, so
/// [`CertificateEmbedsQuorum`] applies to it however it was assembled; a malformed certificate
/// that never passes `check` never influences a correct validator's state. The assembly path
/// matters only for liveness, where a correct proposer needs its own certificates to be
/// well-formed.
///
/// [`SignatureAggregator::append`]: crate::data_types::SignatureAggregator::append
/// [`LiteCertificate::try_from_votes`]: crate::types::LiteCertificate::try_from_votes
/// [`LiteCertificate::check`]: crate::types::LiteCertificate::check
pub trait QuorumAssembly {}
