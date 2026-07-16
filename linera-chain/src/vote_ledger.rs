// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Vote ledger
//!
//! A validator's durable record of every confirmation vote it signs, kept per epoch
//! and chain. The chain manager only holds the current height's vote and the
//! certificate it cites; both are overwritten as the chain advances, so without
//! this ledger a validator cannot reconstruct what it signed — in particular votes
//! for blocks that never got a quorum, or votes superseded by a justified re-vote
//! at the same height.
//!
//! When an epoch is revoked, the ledger is what the validator's *commitment* is
//! assembled from: per chain, the last confirmation-voted block — which covers all
//! earlier votes on that chain of blocks via the parent-hash links — plus every
//! superseded vote, each with the justification it cited.

use linera_base::{
    crypto::{CryptoError, CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::{BlockHeight, Epoch, Round},
};
use serde::{Deserialize, Serialize};

use crate::{
    data_types::{Vote, VoteValue},
    types::{CertificateKind, CertificateValue as _, ConfirmedBlock, ValidatedBlockCertificate},
};

/// A confirmation vote a validator signed: the voted block's height and hash, the
/// round the vote was cast in, the justification commitment it signed, and the
/// signature itself.
///
/// Height, round, block hash and justification commitment identify the vote's
/// entire signed payload (see [`VoteValue`]): a confirmation vote signs no
/// unlocking round, and its first-round attestation is `true` exactly when it
/// cites no quorum. The signature is kept so that votes for the same payload can
/// be aggregated into a regular certificate — in particular from a quorum of
/// commitments to the same block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRecord {
    /// The height of the voted block.
    pub height: BlockHeight,
    /// The round the vote was cast in.
    pub round: Round,
    /// The hash of the voted block.
    pub block_hash: CryptoHash,
    /// The justification commitment the vote signed: the hash of the validated
    /// quorum it cited, or `None` for a vote in the chain's first round (including
    /// the fast round), which cites none.
    pub justification_commitment: Option<CryptoHash>,
    /// The validator's signature on the vote.
    pub signature: ValidatorSignature,
}

impl VoteRecord {
    /// Creates the record of the given confirmation vote.
    pub fn new(vote: &Vote<ConfirmedBlock>) -> Self {
        VoteRecord {
            height: vote.value().block().header.height,
            round: vote.round,
            block_hash: vote.value().hash(),
            justification_commitment: vote.justification_commitment,
            signature: vote.signature,
        }
    }

    /// Verifies that this record's signature is the given validator's signature on
    /// the payload the record identifies.
    pub fn check_signature(&self, public_key: ValidatorPublicKey) -> Result<(), CryptoError> {
        let value = VoteValue(
            self.block_hash,
            self.round,
            CertificateKind::Confirmed,
            None,
            self.justification_commitment.is_none(),
            self.justification_commitment,
        );
        self.signature.check(&value, public_key)
    }
}

/// A confirmation vote together with the justification it cited. The
/// justification is what distinguishes a legitimate re-vote from double-signing,
/// so a vote must never be shown without it.
///
/// In a [`LedgerEntry`] this is a *superseded* vote — one for a block that lost
/// out at its height, either because the validator later cast a justified
/// confirmation vote for a different block there, or because a different block was
/// confirmed there without the validator's vote. The protocol allows both, and the
/// certificate the vote cited is not otherwise retained once the chain moves on.
/// In a commitment entry, the *committed* vote takes this form too.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct JustifiedVote {
    /// The vote.
    pub record: VoteRecord,
    /// The validated-block certificate the vote cited, or `None` for a vote cast in
    /// a chain's first round, which cites no validated quorum.
    pub justification: Option<ValidatedBlockCertificate>,
}

/// All confirmation votes a validator signed on one chain in one epoch: the latest
/// vote, which covers all earlier votes on the same chain of blocks via the
/// parent-hash links, and any superseded votes, which lie off that chain.
///
/// The latest vote's justification is needed too when the commitment is assembled,
/// but it is not stored here: it is recoverable at that point in each of the three
/// states the vote can be in. If the vote is still pending at the chain's tip, the
/// chain manager holds it together with the certificate it cited. If the voted
/// block was confirmed, its stored certificate carries the justification. And if a
/// different block was confirmed at the vote's height, the vote was recorded as
/// superseded — together with its justification — at that moment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct LedgerEntry {
    /// The latest confirmation vote on this chain.
    pub latest: VoteRecord,
    /// Votes for blocks that lost out at their height.
    pub superseded: Vec<JustifiedVote>,
}

/// The vote ledger's view of one confirmation vote being cast, as reported by the
/// chain manager: the new vote, and the same-height vote it displaced, if that
/// vote was for a different block. The worker persists this in the ledger before
/// saving the chain state, so that no vote can leave the validator without being
/// recorded.
#[derive(Debug, Clone)]
pub struct LedgerDelta {
    /// The epoch of the voted block.
    pub epoch: Epoch,
    /// The new confirmation vote.
    pub latest: VoteRecord,
    /// The superseded previous vote, if the new vote replaced one for a different
    /// block at the same height.
    pub superseded: Option<JustifiedVote>,
}
