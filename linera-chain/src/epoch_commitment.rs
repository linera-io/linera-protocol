// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Epoch commitments
//!
//! When an epoch is revoked, each of its validators commits to the confirmation
//! votes it signed in that epoch, assembled from its
//! [vote ledger](crate::vote_ledger). Per chain, the commitment holds the last
//! block the validator confirmation-signed — which *covers* all earlier blocks on
//! that chain of blocks via the parent-hash links — plus every superseded vote.
//! Each vote comes with the justification it cited, so that a justified re-vote
//! cannot be mistaken for double-signing and every entry can be attributed to the
//! individual validator. Once the commitment is published, any confirmation
//! signature from the epoch that is neither covered nor listed proves
//! double-signing.
//!
//! The entries are chunked into size-bounded blobs of type `EpochCommitment`,
//! identified by a [`CommitmentManifest`](linera_base::data_types::CommitmentManifest)
//! signed with the validator's key.

use linera_base::identifiers::ChainId;
use serde::{Deserialize, Serialize};

use crate::vote_ledger::JustifiedVote;

/// All confirmation votes one validator signed on one chain in one epoch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct CommitmentEntry {
    /// The chain the votes were cast on.
    pub chain_id: ChainId,
    /// The validator's latest confirmation vote on this chain, with the
    /// justification it cited. It covers every block up to and including the voted
    /// one.
    pub committed: JustifiedVote,
    /// Votes for blocks that lost out at their height; they lie off the covered
    /// chain of blocks.
    pub superseded: Vec<JustifiedVote>,
}

/// The payload of one `EpochCommitment` blob: a size-bounded chunk of the
/// commitment's entries. Entries are sorted by chain ID across the whole
/// commitment, each chunk continuing where the previous one ended.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct CommitmentChunk {
    /// The commitment entries in this chunk.
    pub entries: Vec<CommitmentEntry>,
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{CryptoHash, ValidatorKeypair, ValidatorSignature},
        data_types::{CommitmentManifest, Epoch, SignedCommitmentManifest},
    };

    #[test]
    fn test_signed_commitment_manifest() {
        let keypair = ValidatorKeypair::generate();
        let manifest = CommitmentManifest {
            epoch: Epoch::from(2),
            validator: keypair.public_key,
            blob_hashes: vec![CryptoHash::test_hash("commitment chunk")],
        };
        let signature = ValidatorSignature::new(&manifest, &keypair.secret_key);
        let signed = SignedCommitmentManifest {
            manifest,
            signature,
        };
        signed.check().unwrap();

        // A manifest naming a different validator fails verification.
        let other = ValidatorKeypair::generate();
        let mut forged = signed.clone();
        forged.manifest.validator = other.public_key;
        assert!(forged.check().is_err());
    }
}
