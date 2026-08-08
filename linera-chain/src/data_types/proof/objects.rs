// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Protocol objects: the definitions the rest of the specification is phrased in.
//!
//! These are definitions, not claims, so they carry no proof and no dependency edges. Each one
//! pins a piece of specification vocabulary to the Rust item that realizes it, so that later
//! statements can talk about "a validation vote in round `r`" while still being checkable
//! against the implementation.

/// **Definition (Signed vote payload).** A validator never signs a bare value; it signs a
/// [`VoteValue`], the six-tuple
///
/// ```text
/// (value_hash, round, kind, unlocking_round, first_round, justification_commitment)
/// ```
///
/// where `kind` is a [`CertificateKind`]. [`VoteValue`] implements
/// [`BcsSignable`](linera_base::crypto::BcsSignable), so the signed byte string is the BCS
/// encoding of that tuple prefixed by the type name. Two payloads differing in any component are
/// distinct signed messages, and a signature over one is not a signature over the other.
///
/// Throughout the specification, "a validator signed *X*" means: it produced a
/// [`ValidatorSignature`](linera_base::crypto::ValidatorSignature) over the [`VoteValue`] whose
/// components are *X*.
///
/// [`VoteValue`]: crate::data_types::VoteValue
/// [`CertificateKind`]: crate::types::CertificateKind
pub trait SignedVotePayload {}

/// **Definition (Vote).** A *vote* by validator `v` is a [`Vote<T>`], or equivalently its
/// projection [`LiteVote`], which keeps every signed field and drops only the value body. Its
/// *round* is [`Vote::round`] and its *kind* is `T::KIND`. We say:
///
/// * a **validation vote**, when the kind is [`CertificateKind::Validated`] — the voter asserts
///   the block is valid at this height;
/// * a **confirmation vote**, when the kind is [`CertificateKind::Confirmed`] — the voter
///   asserts the block is final;
/// * a **timeout vote**, when the kind is [`CertificateKind::Timeout`] — the voter asserts its
///   round timer for this height expired.
///
/// Votes are produced by exactly three constructors — [`Vote::new`],
/// [`Vote::new_with_unlocking_round`] and [`Vote::new_with_first_round`] — each signing the
/// [`SignedVotePayload`] built from its arguments. Which of them a correct validator may call,
/// and when, is pinned down by [`VoteConstructionSites`].
///
/// [`Vote<T>`]: crate::data_types::Vote
/// [`LiteVote`]: crate::data_types::LiteVote
/// [`Vote::round`]: crate::data_types::Vote::round
/// [`Vote::new`]: crate::data_types::Vote::new
/// [`Vote::new_with_unlocking_round`]: crate::data_types::Vote::new_with_unlocking_round
/// [`Vote::new_with_first_round`]: crate::data_types::Vote::new_with_first_round
/// [`CertificateKind::Validated`]: crate::types::CertificateKind::Validated
/// [`CertificateKind::Confirmed`]: crate::types::CertificateKind::Confirmed
/// [`CertificateKind::Timeout`]: crate::types::CertificateKind::Timeout
/// [`VoteConstructionSites`]: crate::manager::proof::voting::VoteConstructionSites
pub trait ValidatorVote {}

/// **Definition (Proposal).** A *proposal* is a [`BlockProposal`]: an owner's signature over a
/// [`ProposalContent`] — a [`ProposedBlock`], a [`Round`](linera_base::data_types::Round) and an
/// optional [`BlockExecutionOutcome`] — plus an optional [`OriginalProposal`] recording which
/// earlier attempt it retries. The three retry shapes are:
///
/// * `original_proposal == None`: a **fresh proposal**, carrying no outcome;
/// * [`OriginalProposal::Fast`]: a **fast retry**, re-proposing a block first proposed in
///   [`Round::Fast`](linera_base::data_types::Round::Fast), carrying the super owner's original
///   signature and no outcome;
/// * [`OriginalProposal::Regular`]: a **regular retry**, re-proposing a block that already
///   carries a [`ValidatedBlockCertificate`], carrying that certificate and the outcome it
///   certifies.
///
/// [`BlockProposal::check_invariants`] enforces that exactly these three shapes are well-formed,
/// that a retry's round is *strictly greater* than the round it retries, and that a regular
/// retry's certificate certifies exactly the block and outcome being re-proposed. The
/// specification uses all three facts.
///
/// [`BlockProposal`]: crate::data_types::BlockProposal
/// [`BlockProposal::check_invariants`]: crate::data_types::BlockProposal::check_invariants
/// [`ProposalContent`]: crate::data_types::ProposalContent
/// [`ProposedBlock`]: crate::data_types::ProposedBlock
/// [`BlockExecutionOutcome`]: crate::data_types::BlockExecutionOutcome
/// [`OriginalProposal`]: crate::data_types::OriginalProposal
/// [`OriginalProposal::Fast`]: crate::data_types::OriginalProposal::Fast
/// [`OriginalProposal::Regular`]: crate::data_types::OriginalProposal::Regular
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
pub trait SignedProposal {}

/// **Definition (Certificate).** A *certificate* is a [`GenericCertificate<T>`] together with
/// the [`JustificationChain`] its wrapper carries. The three instantiations are
/// [`ValidatedBlockCertificate`], [`ConfirmedBlockCertificate`] and [`TimeoutCertificate`].
///
/// A certificate is **valid for a committee** when [`LiteCertificate::check`] returns `Ok` for
/// it. Both block certificate types delegate their `check` to that one function, so it is the
/// single place where a certificate's signatures, its justification chain, and the binding
/// between the two are verified. Unless said otherwise, "certificate" in this specification
/// means one that is valid for the committee of its epoch; what that buys us is
/// [`CertificateEmbedsQuorum`] and [`CertificateCarriesCorrectVote`].
///
/// [`GenericCertificate<T>`]: crate::types::GenericCertificate
/// [`JustificationChain`]: crate::justification::JustificationChain
/// [`ValidatedBlockCertificate`]: crate::types::ValidatedBlockCertificate
/// [`ConfirmedBlockCertificate`]: crate::types::ConfirmedBlockCertificate
/// [`TimeoutCertificate`]: crate::types::TimeoutCertificate
/// [`LiteCertificate::check`]: crate::types::LiteCertificate::check
/// [`CertificateEmbedsQuorum`]: super::quorum::CertificateEmbedsQuorum
/// [`CertificateCarriesCorrectVote`]: super::quorum::CertificateCarriesCorrectVote
pub trait Certificate {}

/// **Definition (Unlocking round).** The *unlocking round* of a validation vote,
/// [`Vote::unlocking_round`], is the round a validator signs to assert: *"I have not cast a
/// confirmation vote for a block other than this one in any round at or above this one."*
/// `None` denotes the strongest form of that claim, covering every round.
///
/// The unlocking round exists for **fault attribution**, not for agreement: it makes a validator
/// that breaks its lock convictable from the certificates alone, via
/// [`extract_equivocations`]. The agreement argument in [`crate::manager::proof::safety`]
/// deliberately does *not* rely on it, and reasons instead about the state a correct validator
/// keeps in its [`ChainManager`] — so that agreement holds even where the attribution machinery
/// is only as strong as the honest-construction obligations recorded in
/// [`crate::justification`].
///
/// [`Vote::unlocking_round`]: crate::data_types::Vote::unlocking_round
/// [`extract_equivocations`]: crate::justification::extract_equivocations
/// [`ChainManager`]: crate::manager::ChainManager
pub trait UnlockingRound {}

/// **Definition (Justification commitment).** The *justification commitment* of a vote is the
/// hash of the [`CommittedQuorum`] it cites, which — because that struct embeds the commitment
/// of the quorum below it — transitively commits to the entire chain of validated quorums
/// underneath. It is one of the six components of the [`SignedVotePayload`], so votes citing
/// different chains are votes on different payloads and never aggregate into one certificate.
///
/// [`CommittedQuorum`]: crate::justification::CommittedQuorum
pub trait JustificationCommitment {}
