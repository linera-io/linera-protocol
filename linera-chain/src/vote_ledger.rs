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
    crypto::CryptoHash,
    data_types::{BlockHeight, Round},
};
use serde::{Deserialize, Serialize};

use crate::types::ValidatedBlockCertificate;

/// A confirmation vote a validator signed: the voted block's height and hash, and
/// the round the vote was cast in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRecord {
    /// The height of the voted block.
    pub height: BlockHeight,
    /// The round the vote was cast in.
    pub round: Round,
    /// The hash of the voted block.
    pub block_hash: CryptoHash,
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
/// chain manager's locking certificate is exactly the cited justification. If the
/// voted block was confirmed, its stored certificate carries the justification. And
/// if a different block was confirmed at the vote's height, the vote was recorded
/// as superseded — together with its justification — at that moment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct LedgerEntry {
    /// The latest confirmation vote on this chain.
    pub latest: VoteRecord,
    /// Votes for blocks that lost out at their height.
    pub superseded: Vec<JustifiedVote>,
}
