// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Fault attributability
//!
//! When two `ConfirmedBlock` certificates exist for different blocks at the same height, the
//! protocol must be able to *attribute* the fault: name validators that provably misbehaved,
//! using only data the certificates carry. This module provides that data — the
//! [`JustificationChain`] — and the algorithm that extracts a proof from it.
//!
//! Each `ValidatedBlock` vote signs a lock round `ℓ` (see [`VoteValue`]), asserting that the
//! voter has not voted to confirm a *different* block in any round `≥ ℓ`. A vote only counts
//! if `ℓ = 0` (no justification needed) or it is justified by a quorum of `ValidatedBlock`
//! votes for the same block in a round in `[ℓ, r)`. That quorum is itself justified, so the
//! justifications form a chain of quorums with strictly decreasing rounds, descending to the
//! round where the block was first validated (where `ℓ = 0`, represented as `None`).
//!
//! Because every certificate carries this chain, two conflicting certificates are
//! self-contained evidence: walking one block's chain against the other block's confirmation
//! quorum reaches a validator whose lock claim is contradicted by its own confirmation vote.
//! See [`extract_equivocation`].
//!
//! [`VoteValue`]: crate::data_types::VoteValue

use allocative::Allocative;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::Round,
    ensure,
};
use linera_execution::committee::Committee;
use serde::{Deserialize, Serialize};

use crate::{
    data_types::{check_signatures, VoteValue},
    types::CertificateKind,
    ChainError,
};

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
/// the certifying round down to the round where the block was first validated.
///
/// Links are ordered by **strictly decreasing** round. The quorum in link `i` was cast with
/// lock `ℓᵢ = links[i + 1].round` — i.e. it is justified by the next, lower link — and the
/// last link was cast with `ℓ = 0` (`None`), the fresh proposal that grounds the chain. An
/// empty chain means the block was confirmed in the fast round, which needs no validation.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Allocative)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct JustificationChain {
    links: Vec<JustificationLink>,
}

impl JustificationChain {
    /// Creates a justification chain from its links, ordered by decreasing round.
    pub fn new(links: Vec<JustificationLink>) -> Self {
        Self { links }
    }

    /// Returns the links, ordered by decreasing round.
    pub fn links(&self) -> &[JustificationLink] {
        &self.links
    }

    /// Returns a new chain with the given quorum prepended as a new top link in `round`,
    /// i.e. the highest, certifying round. The existing links must all be in lower rounds.
    pub fn prepend(
        &self,
        round: Round,
        signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        let mut links = Vec::with_capacity(self.links.len() + 1);
        links.push(JustificationLink { round, signatures });
        links.extend(self.links.iter().cloned());
        Self { links }
    }

    /// Returns the lock `ℓ` that the quorum in link `index` signed: the round of the next,
    /// lower link, or `None` if `index` is the last (grounding) link.
    fn lock_at(&self, index: usize) -> Option<Round> {
        self.links.get(index + 1).map(|link| link.round)
    }

    /// Returns the lock `ℓ` that a certificate sitting on top of this chain signed: the round
    /// of the highest link, or `None` if the chain is empty.
    pub fn top_lock(&self) -> Option<Round> {
        self.links.first().map(|link| link.round)
    }

    /// Verifies that every link is a quorum of `ValidatedBlock` votes for `value_hash` and
    /// that the rounds strictly decrease, so each link is properly justified by the next.
    pub fn verify(&self, value_hash: CryptoHash, committee: &Committee) -> Result<(), ChainError> {
        for (index, link) in self.links.iter().enumerate() {
            if let Some(next) = self.links.get(index + 1) {
                ensure!(
                    next.round < link.round,
                    ChainError::JustificationRoundsNotDecreasing
                );
            }
            check_signatures(
                value_hash,
                CertificateKind::Validated,
                link.round,
                self.lock_at(index),
                &link.signatures,
                committee,
            )?;
        }
        Ok(())
    }
}

/// A confirmed block together with the justification that makes it self-contained evidence:
/// the chain of `ValidatedBlock` quorums for the same block, with its top link in the round
/// the block was confirmed. This is the shape a `ConfirmedBlockCertificate` will carry.
#[derive(Clone, Debug)]
pub struct JustifiedConfirmation {
    /// The hash of the confirmed block. Since `ValidatedBlock` and `ConfirmedBlock` wrap the
    /// same `Block`, this is also the hash the chain's `ValidatedBlock` votes sign.
    pub block_hash: CryptoHash,
    /// The round in which the block was confirmed.
    pub round: Round,
    /// The quorum of `ConfirmedBlock` votes finalizing the block.
    pub confirmed_signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    /// The `ValidatedBlock` justification chain (empty iff confirmed in the fast round).
    pub justification: JustificationChain,
}

/// A quorum of `ValidatedBlock` votes for one block, cast in one round under one lock. This is
/// the top of a `ValidatedBlockCertificate`; comparing two of them in the same round attributes
/// a double-validation fault.
#[derive(Clone, Debug)]
pub struct ValidatedQuorum {
    /// The hash of the validated block, which is also what the votes sign.
    pub block_hash: CryptoHash,
    /// The round in which the block was validated.
    pub round: Round,
    /// The lock `ℓ` these `ValidatedBlock` votes signed.
    pub lock: Option<Round>,
    /// The quorum of `ValidatedBlock` votes.
    pub signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
}

/// A self-contained proof that a single validator misbehaved.
#[derive(Clone, Debug)]
pub enum EquivocationProof {
    /// The validator voted to validate one block under a lock `ℓ` while having voted to
    /// confirm a *different* block in a round `≥ ℓ` — contradicting its own lock claim.
    LockViolation {
        /// The misbehaving validator.
        validator: ValidatorPublicKey,
        /// The block the validator voted to confirm.
        confirmed_block_hash: CryptoHash,
        /// The round in which it voted to confirm.
        confirmed_round: Round,
        /// Its `ConfirmedBlock` signature.
        confirmed_signature: ValidatorSignature,
        /// The different block the validator voted to validate.
        validated_block_hash: CryptoHash,
        /// The round in which it voted to validate.
        validated_round: Round,
        /// The lock `ℓ` the validation vote signed, contradicted by the confirmation.
        validated_lock: Option<Round>,
        /// Its `ValidatedBlock` signature.
        validated_signature: ValidatorSignature,
    },
    /// The validator cast two votes of the same kind for different blocks in the same round:
    /// either two `ConfirmedBlock` votes, or two `ValidatedBlock` votes (illegal regardless of
    /// the locks, since a validator may vote for at most one block per round and kind).
    DoubleVote {
        /// The misbehaving validator.
        validator: ValidatorPublicKey,
        /// The round both votes share.
        round: Round,
        /// The certificate kind both votes share.
        kind: CertificateKind,
        /// The first block voted for.
        first_block_hash: CryptoHash,
        /// The lock the first vote signed (`None` for `ConfirmedBlock` votes).
        first_lock: Option<Round>,
        /// The signature on the first vote.
        first_signature: ValidatorSignature,
        /// The second, different block voted for.
        second_block_hash: CryptoHash,
        /// The lock the second vote signed (`None` for `ConfirmedBlock` votes).
        second_lock: Option<Round>,
        /// The signature on the second vote.
        second_signature: ValidatorSignature,
    },
}

impl EquivocationProof {
    /// Returns the misbehaving validator.
    pub fn validator(&self) -> ValidatorPublicKey {
        match self {
            EquivocationProof::LockViolation { validator, .. }
            | EquivocationProof::DoubleVote { validator, .. } => *validator,
        }
    }

    /// Verifies that this is a genuine proof of misbehavior: the two votes are for different
    /// blocks, are actually incompatible, and are both signed by the named validator.
    pub fn check(&self) -> Result<(), ChainError> {
        match self {
            EquivocationProof::LockViolation {
                validator,
                confirmed_block_hash,
                confirmed_round,
                confirmed_signature,
                validated_block_hash,
                validated_round,
                validated_lock,
                validated_signature,
            } => {
                ensure!(
                    confirmed_block_hash != validated_block_hash,
                    ChainError::EquivocationProofSameBlock
                );
                // The lock claim — "no confirmation of a different block in any round `≥ ℓ`" —
                // is made while validating in `validated_round`, so it covers only the rounds the
                // voter had already acted in: the window `[ℓ, validated_round)` (`ℓ = None` means
                // `0`). The confirmation contradicts it only if it falls in that window, i.e.
                // `ℓ ≤ confirmed_round < validated_round`. A confirmation at or after
                // `validated_round` is a legitimate later switch, not a violation.
                ensure!(
                    *confirmed_round < *validated_round
                        && validated_lock.is_none_or(|lock| *confirmed_round >= lock),
                    ChainError::EquivocationProofNoLockViolation
                );
                let confirmed = VoteValue(
                    *confirmed_block_hash,
                    *confirmed_round,
                    CertificateKind::Confirmed,
                    None,
                );
                confirmed_signature.check(&confirmed, *validator)?;
                let validated = VoteValue(
                    *validated_block_hash,
                    *validated_round,
                    CertificateKind::Validated,
                    *validated_lock,
                );
                validated_signature.check(&validated, *validator)?;
                Ok(())
            }
            EquivocationProof::DoubleVote {
                validator,
                round,
                kind,
                first_block_hash,
                first_lock,
                first_signature,
                second_block_hash,
                second_lock,
                second_signature,
            } => {
                ensure!(
                    first_block_hash != second_block_hash,
                    ChainError::EquivocationProofSameBlock
                );
                let first = VoteValue(*first_block_hash, *round, *kind, *first_lock);
                first_signature.check(&first, *validator)?;
                let second = VoteValue(*second_block_hash, *round, *kind, *second_lock);
                second_signature.check(&second, *validator)?;
                Ok(())
            }
        }
    }
}

/// Extracts a proof of equivocation from two `ConfirmedBlock` certificates that finalize
/// *different* blocks at the same height.
///
/// Returns `None` only if the inputs do not actually conflict (same block) or are malformed;
/// for two genuinely conflicting, well-formed certificates a proof always exists.
pub fn extract_equivocation(
    a: &JustifiedConfirmation,
    b: &JustifiedConfirmation,
) -> Option<EquivocationProof> {
    if a.block_hash == b.block_hash {
        return None; // Not a conflict.
    }
    // Walk each block's justification chain against the other's confirmation quorum. The base
    // of a non-empty chain (lock `None`) always catches a validator in the intersection, so a
    // single non-empty chain suffices.
    walk_chain(a, b)
        .or_else(|| walk_chain(b, a))
        // Both blocks were confirmed in the fast round (no chain): the fault is then two
        // confirmation votes for different blocks in the same round.
        .or_else(|| double_confirm(a, b))
}

/// Looks for a validator that confirmed `confirmer`'s block and also appears in some link of
/// `chained`'s justification chain with a lock the confirmation contradicts.
fn walk_chain(
    confirmer: &JustifiedConfirmation,
    chained: &JustifiedConfirmation,
) -> Option<EquivocationProof> {
    let chain = &chained.justification;
    for (index, link) in chain.links().iter().enumerate() {
        let lock = chain.lock_at(index);
        // This link's votes (cast in `link.round`) claim no conflicting confirmation in any
        // round `≥ ℓ`, covering the window `[ℓ, link.round)`. A confirmation in
        // `confirmer.round` contradicts it only if it falls in that window: the link must have
        // been cast strictly after the confirmation (`link.round > confirmer.round`) with a lock
        // reaching back over it (`ℓ ≤ confirmer.round`). Otherwise it's a legitimate later
        // switch; another link may still straddle the confirmation, so keep scanning.
        if link.round <= confirmer.round || lock.is_some_and(|lock| confirmer.round < lock) {
            continue;
        }
        for (validator, validated_signature) in &link.signatures {
            if let Some(confirmed_signature) =
                signature_of(&confirmer.confirmed_signatures, validator)
            {
                return Some(EquivocationProof::LockViolation {
                    validator: *validator,
                    confirmed_block_hash: confirmer.block_hash,
                    confirmed_round: confirmer.round,
                    confirmed_signature,
                    validated_block_hash: chained.block_hash,
                    validated_round: link.round,
                    validated_lock: lock,
                    validated_signature: *validated_signature,
                });
            }
        }
    }
    None
}

/// Extracts a proof that a validator validated two *different* blocks in the same round, which
/// is illegal regardless of the locks: a validator may vote to validate at most one block per
/// round. Returns `None` if the quorums are for the same block or different rounds (validating
/// conflicting blocks in different rounds is not itself a fault — only confirming is locked).
pub fn extract_double_validation(
    a: &ValidatedQuorum,
    b: &ValidatedQuorum,
) -> Option<EquivocationProof> {
    if a.round != b.round {
        return None;
    }
    double_vote(
        a.round,
        CertificateKind::Validated,
        a.block_hash,
        a.lock,
        &a.signatures,
        b.block_hash,
        b.lock,
        &b.signatures,
    )
}

/// Looks for a validator that confirmed both blocks in the same round.
fn double_confirm(
    a: &JustifiedConfirmation,
    b: &JustifiedConfirmation,
) -> Option<EquivocationProof> {
    if a.round != b.round {
        return None;
    }
    double_vote(
        a.round,
        CertificateKind::Confirmed,
        a.block_hash,
        None,
        &a.confirmed_signatures,
        b.block_hash,
        None,
        &b.confirmed_signatures,
    )
}

/// Finds a validator that signed both quorums and builds the corresponding [`EquivocationProof::DoubleVote`].
#[allow(clippy::too_many_arguments)]
fn double_vote(
    round: Round,
    kind: CertificateKind,
    first_block_hash: CryptoHash,
    first_lock: Option<Round>,
    first_signatures: &[(ValidatorPublicKey, ValidatorSignature)],
    second_block_hash: CryptoHash,
    second_lock: Option<Round>,
    second_signatures: &[(ValidatorPublicKey, ValidatorSignature)],
) -> Option<EquivocationProof> {
    if first_block_hash == second_block_hash {
        return None;
    }
    for (validator, first_signature) in first_signatures {
        if let Some(second_signature) = signature_of(second_signatures, validator) {
            return Some(EquivocationProof::DoubleVote {
                validator: *validator,
                round,
                kind,
                first_block_hash,
                first_lock,
                first_signature: *first_signature,
                second_block_hash,
                second_lock,
                second_signature,
            });
        }
    }
    None
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
