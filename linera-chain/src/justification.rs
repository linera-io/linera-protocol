// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Fault attributability
//!
//! When two `ConfirmedBlock` certificates exist for different blocks at the same height, the
//! protocol must be able to *attribute* the fault: name validators that provably misbehaved,
//! using only data the certificates carry. This module provides that data — the
//! [`JustificationChain`] — and the algorithm that extracts a proof from it.
//!
//! Each `ValidatedBlock` vote signs an unlocking round (see [`VoteValue`]), asserting that the
//! voter has not voted to confirm a *different* block in any round at or above it. A vote only
//! counts if its unlocking round is `0` (no justification needed) or it is justified by a quorum
//! of `ValidatedBlock` votes for the same block in a round at or above the unlocking round and
//! below `r`. That quorum is itself justified, so the justifications form a chain of quorums with
//! strictly increasing rounds, rising from the round where the block was first validated (where
//! the unlocking round is `0`, represented as `None`) up to the certifying round.
//!
//! A confirmation in a chain's *first* round needs no chain: its `ConfirmedBlock` votes instead
//! carry a first-round attestation (see [`VoteValue`]), asserting that no lower round exists at
//! this height. A vote in a lower round together with an attestation in a higher one is therefore
//! itself an attributable fault.
//!
//! Because every certificate carries this chain (or attestation), two conflicting certificates
//! are self-contained evidence: walking one block's chain against the other block's confirmation
//! quorum reaches validators whose unlocking-round claims are contradicted by their own
//! confirmation votes, or the two confirmation quorums intersect in validators that contradicted
//! themselves. See [`extract_equivocations`].
//!
//! [`VoteValue`]: crate::data_types::VoteValue

use std::collections::BTreeMap;

use allocative::Allocative;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::Round,
    ensure,
};
use linera_execution::committee::Committee;
use serde::{Deserialize, Serialize};

use crate::{
    block::BlockHeader,
    data_types::{check_signatures, VoteValue},
    types::CertificateKind,
    ChainError,
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_interval, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The number of links in a justification chain a node verified. A chain grows by one link
    /// per round a block had to fight through, so a rising tail here signals contention on some
    /// height before it becomes a certificate-size or finalization problem.
    pub static JUSTIFICATION_CHAIN_LENGTH: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "justification_chain_length",
            "Number of links in a verified justification chain",
            &[],
            exponential_bucket_interval(1.0, 1024.0),
        )
    });
}

/// One link in a justification chain: a quorum of validators that all voted to validate the
/// same block in `round`.
#[derive(Clone, Debug, Serialize, Deserialize, Allocative)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct JustificationLink {
    /// The round in which these `ValidatedBlock` votes were cast.
    pub round: Round,
    /// The validators' signatures over the corresponding [`VoteValue`].
    pub signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
}

/// The chain of `ValidatedBlock` quorums that justifies a validated or confirmed block, from
/// the round where the block was first validated up to the certifying round.
///
/// Links are ordered by **strictly increasing** round. The quorum in link `i` was cast with
/// unlocking round `links[i - 1].round` — i.e. it is justified by the previous, lower link — and
/// the first link (index `0`) was cast with unlocking round `0` (`None`), the fresh proposal that
/// grounds the chain. An empty chain means the block was confirmed in the fast round, which needs
/// no validation.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Allocative)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct JustificationChain {
    links: Vec<JustificationLink>,
}

impl JustificationChain {
    /// Creates a justification chain from its links, ordered by increasing round.
    pub fn new(links: Vec<JustificationLink>) -> Self {
        Self { links }
    }

    /// Returns the links, ordered by increasing round.
    pub fn links(&self) -> &[JustificationLink] {
        &self.links
    }

    /// Returns a new chain with the given quorum appended as a new top link in `round`,
    /// i.e. the highest, certifying round. The existing links must all be in lower rounds.
    pub fn append(
        &self,
        round: Round,
        signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        let mut links = self.links.clone();
        links.push(JustificationLink { round, signatures });
        Self { links }
    }

    /// Returns the unlocking round that the quorum in link `index` signed: the round of the
    /// previous, lower link, or `None` if `index` is the first (grounding) link.
    fn unlocking_round_at(&self, index: usize) -> Option<Round> {
        index.checked_sub(1).map(|prev| self.links[prev].round)
    }

    /// Returns the unlocking round that a certificate sitting on top of this chain signed: the
    /// round of the highest link, or `None` if the chain is empty.
    pub fn top_unlocking_round(&self) -> Option<Round> {
        self.links.last().map(|link| link.round)
    }

    /// Verifies that every link is a quorum of `ValidatedBlock` votes for `value_hash` and
    /// that the rounds strictly increase, so each link is properly justified by the previous one.
    ///
    /// The chain length needs no explicit cap: strictly increasing rounds mean a chain of `n`
    /// links spans `n` distinct rounds, and each link must carry a genuine quorum, so a longer
    /// chain necessarily reaches a higher round and cannot be inflated cheaply. The observed
    /// length is recorded as a metric so real contention shows up in monitoring.
    pub fn verify(&self, value_hash: CryptoHash, committee: &Committee) -> Result<(), ChainError> {
        #[cfg(with_metrics)]
        metrics::JUSTIFICATION_CHAIN_LENGTH
            .with_label_values(&[])
            .observe(self.links.len() as f64);
        for (index, link) in self.links.iter().enumerate() {
            if let Some(next) = self.links.get(index + 1) {
                ensure!(
                    link.round < next.round,
                    ChainError::JustificationRoundsNotIncreasing
                );
            }
            check_signatures(
                value_hash,
                CertificateKind::Validated,
                link.round,
                self.unlocking_round_at(index),
                false,
                &link.signatures,
                committee,
            )?;
        }
        Ok(())
    }
}

/// A confirmed block's *header* together with the justification that makes it self-contained
/// evidence: the round and quorum of `ConfirmedBlock` votes that finalized it, and the chain of
/// `ValidatedBlock` quorums for the same block, with its top link in the round the block was
/// confirmed. Only the header travels, never the block body. The header hashes to the value the
/// votes sign (`CryptoHash::new(&header)`) and carries the chain ID and height that scope the
/// fault. This is the shape a `ConfirmedBlockCertificate` reduces to for fault attribution.
#[derive(Clone, Debug)]
pub struct JustifiedConfirmation {
    /// The header of the confirmed block. Its hash is what the chain's `ValidatedBlock` and
    /// `ConfirmedBlock` votes sign (`ValidatedBlock` and `ConfirmedBlock` wrap the same block).
    pub header: BlockHeader,
    /// The round in which the block was confirmed.
    pub round: Round,
    /// The first-round attestation the `ConfirmedBlock` votes signed (see [`VoteValue`]).
    pub first_round: bool,
    /// The quorum of `ConfirmedBlock` votes finalizing the block.
    pub confirmed_signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    /// The `ValidatedBlock` justification chain (empty iff confirmed in the chain's first round).
    pub justification: JustificationChain,
}

impl JustifiedConfirmation {
    /// The hash of the confirmed block, which is what its votes sign.
    pub fn block_hash(&self) -> CryptoHash {
        CryptoHash::new(&self.header)
    }
}

/// A quorum of `ValidatedBlock` votes for one block, cast in one round under one unlocking round.
/// This is the top of a `ValidatedBlockCertificate`; comparing two of them in the same round
/// attributes a double-validation fault.
#[derive(Clone, Debug)]
pub struct ValidatedQuorum {
    /// The header of the validated block. Its hash is what the votes sign, and it carries the
    /// chain ID and height that scope the fault.
    pub header: BlockHeader,
    /// The round in which the block was validated.
    pub round: Round,
    /// The unlocking round these `ValidatedBlock` votes signed.
    pub unlocking_round: Option<Round>,
    /// The quorum of `ValidatedBlock` votes.
    pub signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
}

/// A self-contained proof that a single validator misbehaved.
#[derive(Clone, Debug)]
pub enum EquivocationProof {
    /// The validator voted to validate one block under an unlocking round while having voted to
    /// confirm a *different* block in a round at or above it — contradicting its own
    /// unlocking-round claim.
    LockViolation {
        /// The misbehaving validator.
        validator: ValidatorPublicKey,
        /// The header of the block the validator voted to confirm. Its hash, chain ID and height
        /// are read from here; it must share the chain and height of `validated_header`.
        confirmed_header: BlockHeader,
        /// The round in which it voted to confirm.
        confirmed_round: Round,
        /// The first-round attestation the confirmation vote signed (see [`VoteValue`]).
        ///
        /// [`VoteValue`]: crate::data_types::VoteValue
        confirmed_attested: bool,
        /// Its `ConfirmedBlock` signature.
        confirmed_signature: ValidatorSignature,
        /// The header of the different block the validator voted to validate.
        validated_header: BlockHeader,
        /// The round in which it voted to validate.
        validated_round: Round,
        /// The unlocking round the validation vote signed, contradicted by the confirmation.
        validated_unlocking_round: Option<Round>,
        /// Its `ValidatedBlock` signature.
        validated_signature: ValidatorSignature,
    },
    /// The validator cast two votes of the same kind for different blocks in the same round:
    /// either two `ConfirmedBlock` votes, or two `ValidatedBlock` votes (illegal regardless of
    /// the unlocking rounds, since a validator may vote for at most one block per round and kind).
    DoubleVote {
        /// The misbehaving validator.
        validator: ValidatorPublicKey,
        /// The round both votes share.
        round: Round,
        /// The certificate kind both votes share.
        kind: CertificateKind,
        /// The header of the first block voted for. The two headers must share a chain and height.
        first_header: BlockHeader,
        /// The unlocking round the first vote signed (`None` for `ConfirmedBlock` votes).
        first_unlocking_round: Option<Round>,
        /// The first-round attestation the first vote signed (`false` for `ValidatedBlock` votes).
        first_attested: bool,
        /// The signature on the first vote.
        first_signature: ValidatorSignature,
        /// The header of the second, different block voted for.
        second_header: BlockHeader,
        /// The unlocking round the second vote signed (`None` for `ConfirmedBlock` votes).
        second_unlocking_round: Option<Round>,
        /// The first-round attestation the second vote signed (`false` for `ValidatedBlock` votes).
        second_attested: bool,
        /// The signature on the second vote.
        second_signature: ValidatorSignature,
    },
    /// The validator voted to confirm a block with the first-round attestation — asserting that
    /// no lower round exists at this height — while having also voted to confirm a block in a
    /// lower round. The two votes cannot both be honest, regardless of the blocks they are for.
    FirstRoundViolation {
        /// The misbehaving validator.
        validator: ValidatorPublicKey,
        /// The header of the block confirmed with the attestation. It must share the chain and
        /// height of `earlier_header`.
        attested_header: BlockHeader,
        /// The round the attestation asserts to be the chain's first.
        attested_round: Round,
        /// The `ConfirmedBlock` signature carrying the attestation.
        attested_signature: ValidatorSignature,
        /// The header of the block the earlier vote confirmed.
        earlier_header: BlockHeader,
        /// The round of the earlier vote — strictly below `attested_round`.
        earlier_round: Round,
        /// The first-round attestation the earlier vote signed.
        earlier_attested: bool,
        /// Its `ConfirmedBlock` signature.
        earlier_signature: ValidatorSignature,
    },
}

impl EquivocationProof {
    /// Returns the misbehaving validator.
    pub fn validator(&self) -> ValidatorPublicKey {
        match self {
            EquivocationProof::LockViolation { validator, .. }
            | EquivocationProof::DoubleVote { validator, .. }
            | EquivocationProof::FirstRoundViolation { validator, .. } => *validator,
        }
    }

    /// Verifies that this is a genuine proof of misbehavior: the two votes are for different
    /// blocks, are actually incompatible, and are both signed by the named validator.
    pub fn check(&self) -> Result<(), ChainError> {
        match self {
            EquivocationProof::LockViolation {
                validator,
                confirmed_header,
                confirmed_round,
                confirmed_attested,
                confirmed_signature,
                validated_header,
                validated_round,
                validated_unlocking_round,
                validated_signature,
            } => {
                let confirmed_block_hash = CryptoHash::new(confirmed_header);
                let validated_block_hash = CryptoHash::new(validated_header);
                ensure!(
                    confirmed_block_hash != validated_block_hash,
                    ChainError::EquivocationProofSameBlock
                );
                // The two votes must concern the same height on the same chain; otherwise there
                // is no lock relationship between them — a validator may freely confirm a block at
                // one height and validate a different one at another height or on another chain.
                ensure!(
                    confirmed_header.chain_id == validated_header.chain_id
                        && confirmed_header.height == validated_header.height,
                    ChainError::EquivocationProofDifferentChainOrHeight
                );
                // The unlocking-round claim — "no confirmation of a different block in any round
                // at or above the unlocking round" — is made while validating in
                // `validated_round`, so it covers only the rounds the voter had already acted in:
                // the window `[unlocking_round, validated_round)` (an unlocking round of `None`
                // means `0`). The confirmation contradicts it only if it falls in that window,
                // i.e. `unlocking_round ≤ confirmed_round < validated_round`. A confirmation at or
                // after `validated_round` is a legitimate later switch, not a violation.
                ensure!(
                    *confirmed_round < *validated_round
                        && validated_unlocking_round
                            .is_none_or(|unlocking_round| *confirmed_round >= unlocking_round),
                    ChainError::EquivocationProofNoLockViolation
                );
                let confirmed = VoteValue(
                    confirmed_block_hash,
                    *confirmed_round,
                    CertificateKind::Confirmed,
                    None,
                    *confirmed_attested,
                );
                confirmed_signature.check(&confirmed, *validator)?;
                let validated = VoteValue(
                    validated_block_hash,
                    *validated_round,
                    CertificateKind::Validated,
                    *validated_unlocking_round,
                    false,
                );
                validated_signature.check(&validated, *validator)?;
                Ok(())
            }
            EquivocationProof::DoubleVote {
                validator,
                round,
                kind,
                first_header,
                first_unlocking_round,
                first_attested,
                first_signature,
                second_header,
                second_unlocking_round,
                second_attested,
                second_signature,
            } => {
                let first_block_hash = CryptoHash::new(first_header);
                let second_block_hash = CryptoHash::new(second_header);
                ensure!(
                    first_block_hash != second_block_hash,
                    ChainError::EquivocationProofSameBlock
                );
                // Both votes must concern the same height on the same chain: a validator voting
                // for different blocks at different heights or on different chains in the same
                // round number is not double-voting.
                ensure!(
                    first_header.chain_id == second_header.chain_id
                        && first_header.height == second_header.height,
                    ChainError::EquivocationProofDifferentChainOrHeight
                );
                let first = VoteValue(
                    first_block_hash,
                    *round,
                    *kind,
                    *first_unlocking_round,
                    *first_attested,
                );
                first_signature.check(&first, *validator)?;
                let second = VoteValue(
                    second_block_hash,
                    *round,
                    *kind,
                    *second_unlocking_round,
                    *second_attested,
                );
                second_signature.check(&second, *validator)?;
                Ok(())
            }
            EquivocationProof::FirstRoundViolation {
                validator,
                attested_header,
                attested_round,
                attested_signature,
                earlier_header,
                earlier_round,
                earlier_attested,
                earlier_signature,
            } => {
                // Both votes must concern the same height on the same chain: the attestation only
                // asserts that no lower round exists at *this* height, so a vote at a lower round
                // number elsewhere does not contradict it. The blocks themselves may be equal —
                // the contradiction is between the rounds, not the blocks.
                ensure!(
                    attested_header.chain_id == earlier_header.chain_id
                        && attested_header.height == earlier_header.height,
                    ChainError::EquivocationProofDifferentChainOrHeight
                );
                ensure!(
                    *earlier_round < *attested_round,
                    ChainError::EquivocationProofNoFirstRoundViolation
                );
                let attested = VoteValue(
                    CryptoHash::new(attested_header),
                    *attested_round,
                    CertificateKind::Confirmed,
                    None,
                    true,
                );
                attested_signature.check(&attested, *validator)?;
                let earlier = VoteValue(
                    CryptoHash::new(earlier_header),
                    *earlier_round,
                    CertificateKind::Confirmed,
                    None,
                    *earlier_attested,
                );
                earlier_signature.check(&earlier, *validator)?;
                Ok(())
            }
        }
    }
}

/// Extracts proofs of equivocation from two `ConfirmedBlock` certificates that finalize
/// *different* blocks at the same height: one proof for every validator whose own signatures on
/// the two certificates (including their justification chains) contradict each other.
///
/// Returns an empty list only if the inputs do not actually conflict (same block, or different
/// chains or heights) or are malformed. For two genuinely conflicting, well-formed certificates
/// the proven validators always hold at least a third of the total weight, because each of the
/// following cases blames a full intersection of two quorums. With the lower confirmation in
/// round `r` and the higher in round `s`:
///
/// - `r == s`: every validator in the intersection of the two confirmation quorums double-voted.
/// - `r < s` and the higher certificate carries a justification chain: the chain's links tile
///   all rounds from its grounding round up to `s` with unlocking-round windows, and the
///   grounding link's window is unbounded below, so some link's window contains `r`. Every
///   validator in that link's intersection with the lower confirmation quorum violated its lock.
/// - `r < s` and the higher certificate instead carries the first-round attestation: every
///   validator in the intersection of the two confirmation quorums confirmed in `r` while
///   attesting that `s` is the chain's first round — a first-round violation.
pub fn extract_equivocations(
    a: &JustifiedConfirmation,
    b: &JustifiedConfirmation,
) -> Vec<EquivocationProof> {
    // A conflict is two *different* blocks at the *same* height on the *same* chain.
    if a.header.chain_id != b.header.chain_id || a.header.height != b.header.height {
        return Vec::new();
    }
    if a.block_hash() == b.block_hash() {
        return Vec::new(); // Not a conflict.
    }
    let mut proofs = BTreeMap::new();
    // Walk each block's justification chain against the other's confirmation quorum.
    walk_chain(a, b, &mut proofs);
    walk_chain(b, a, &mut proofs);
    // Both blocks were confirmed in the same round: two confirmation votes for different blocks
    // in that round.
    double_confirm(a, b, &mut proofs);
    // One block was confirmed with a first-round attestation and the other in a lower round: a
    // confirmation below the attested first round.
    first_round_violation(a, b, &mut proofs);
    first_round_violation(b, a, &mut proofs);
    proofs.into_values().collect()
}

/// Records a proof for every validator that confirmed `confirmer`'s block and also appears in
/// some link of `chained`'s justification chain with an unlocking round the confirmation
/// contradicts.
fn walk_chain(
    confirmer: &JustifiedConfirmation,
    chained: &JustifiedConfirmation,
    proofs: &mut BTreeMap<ValidatorPublicKey, EquivocationProof>,
) {
    let chain = &chained.justification;
    for (index, link) in chain.links().iter().enumerate() {
        let unlocking_round = chain.unlocking_round_at(index);
        // This link's votes (cast in `link.round`) claim no conflicting confirmation in any
        // round at or above the unlocking round, covering the window `[unlocking_round,
        // link.round)`. A confirmation in `confirmer.round` contradicts it only if it falls in
        // that window: the link must have been cast strictly after the confirmation
        // (`link.round > confirmer.round`) with an unlocking round reaching back over it
        // (`unlocking_round ≤ confirmer.round`). Otherwise it's a legitimate later switch; another
        // link may still straddle the confirmation, so keep scanning.
        if link.round <= confirmer.round
            || unlocking_round.is_some_and(|unlocking_round| confirmer.round < unlocking_round)
        {
            continue;
        }
        for (validator, validated_signature) in &link.signatures {
            if let Some(confirmed_signature) =
                signature_of(&confirmer.confirmed_signatures, validator)
            {
                proofs
                    .entry(*validator)
                    .or_insert_with(|| EquivocationProof::LockViolation {
                        validator: *validator,
                        confirmed_header: confirmer.header.clone(),
                        confirmed_round: confirmer.round,
                        confirmed_attested: confirmer.first_round,
                        confirmed_signature,
                        validated_header: chained.header.clone(),
                        validated_round: link.round,
                        validated_unlocking_round: unlocking_round,
                        validated_signature: *validated_signature,
                    });
            }
        }
    }
}

/// Extracts proofs that validators validated two *different* blocks in the same round, which
/// is illegal regardless of the locks: a validator may vote to validate at most one block per
/// round. One proof per validator that signed both quorums; empty if the quorums are for the
/// same block or different rounds (validating conflicting blocks in different rounds is not
/// itself a fault — only confirming is locked).
pub fn extract_double_validations(
    a: &ValidatedQuorum,
    b: &ValidatedQuorum,
) -> Vec<EquivocationProof> {
    let mut proofs = BTreeMap::new();
    if a.round == b.round {
        double_vote(
            a.round,
            CertificateKind::Validated,
            &a.header,
            a.unlocking_round,
            false,
            &a.signatures,
            &b.header,
            b.unlocking_round,
            false,
            &b.signatures,
            &mut proofs,
        );
    }
    proofs.into_values().collect()
}

/// Records a proof for every validator that confirmed both blocks in the same round.
fn double_confirm(
    a: &JustifiedConfirmation,
    b: &JustifiedConfirmation,
    proofs: &mut BTreeMap<ValidatorPublicKey, EquivocationProof>,
) {
    if a.round != b.round {
        return;
    }
    double_vote(
        a.round,
        CertificateKind::Confirmed,
        &a.header,
        None,
        a.first_round,
        &a.confirmed_signatures,
        &b.header,
        None,
        b.first_round,
        &b.confirmed_signatures,
        proofs,
    );
}

/// Records a proof for every validator that confirmed `attested`'s block with the first-round
/// attestation and also confirmed `earlier`'s block in a lower round — contradicting the
/// attestation's claim that no lower round exists at this height.
fn first_round_violation(
    attested: &JustifiedConfirmation,
    earlier: &JustifiedConfirmation,
    proofs: &mut BTreeMap<ValidatorPublicKey, EquivocationProof>,
) {
    if !attested.first_round || earlier.round >= attested.round {
        return;
    }
    for (validator, attested_signature) in &attested.confirmed_signatures {
        if let Some(earlier_signature) = signature_of(&earlier.confirmed_signatures, validator) {
            proofs
                .entry(*validator)
                .or_insert_with(|| EquivocationProof::FirstRoundViolation {
                    validator: *validator,
                    attested_header: attested.header.clone(),
                    attested_round: attested.round,
                    attested_signature: *attested_signature,
                    earlier_header: earlier.header.clone(),
                    earlier_round: earlier.round,
                    earlier_attested: earlier.first_round,
                    earlier_signature,
                });
        }
    }
}

/// Records an [`EquivocationProof::DoubleVote`] for every validator that signed both quorums.
#[allow(clippy::too_many_arguments)]
fn double_vote(
    round: Round,
    kind: CertificateKind,
    first_header: &BlockHeader,
    first_unlocking_round: Option<Round>,
    first_attested: bool,
    first_signatures: &[(ValidatorPublicKey, ValidatorSignature)],
    second_header: &BlockHeader,
    second_unlocking_round: Option<Round>,
    second_attested: bool,
    second_signatures: &[(ValidatorPublicKey, ValidatorSignature)],
    proofs: &mut BTreeMap<ValidatorPublicKey, EquivocationProof>,
) {
    // Only a conflict if the two votes are for different blocks at the same height on the same
    // chain.
    if first_header.chain_id != second_header.chain_id
        || first_header.height != second_header.height
        || CryptoHash::new(first_header) == CryptoHash::new(second_header)
    {
        return;
    }
    for (validator, first_signature) in first_signatures {
        if let Some(second_signature) = signature_of(second_signatures, validator) {
            proofs
                .entry(*validator)
                .or_insert_with(|| EquivocationProof::DoubleVote {
                    validator: *validator,
                    round,
                    kind,
                    first_header: first_header.clone(),
                    first_unlocking_round,
                    first_attested,
                    first_signature: *first_signature,
                    second_header: second_header.clone(),
                    second_unlocking_round,
                    second_attested,
                    second_signature,
                });
        }
    }
}

/// Returns the validator's signature in `signatures`, if present.
fn signature_of(
    signatures: &[(ValidatorPublicKey, ValidatorSignature)],
    validator: &ValidatorPublicKey,
) -> Option<ValidatorSignature> {
    signatures
        .iter()
        .find(|(key, _)| key == validator)
        .map(|(_, signature)| *signature)
}

#[cfg(test)]
#[path = "unit_tests/justification_tests.rs"]
mod justification_tests;
