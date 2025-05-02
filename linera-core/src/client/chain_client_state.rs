// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeSet, sync::Arc};

use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight, Timestamp},
    ensure,
    identifiers::AccountOwner,
    ownership::ChainOwnership,
};
use linera_chain::data_types::ProposedBlock;
use tokio::sync::Mutex;

use super::{ChainClientError, PendingProposal};
use crate::data_types::ChainInfo;

/// The state of our interaction with a particular chain: how far we have synchronized it and
/// whether we are currently attempting to propose a new block.
pub struct ChainClientState {
    /// Latest block hash, if any.
    block_hash: Option<CryptoHash>,
    /// The earliest possible timestamp for the next block.
    timestamp: Timestamp,
    /// Sequence number that we plan to use for the next block.
    /// We track this value outside local storage mainly for security reasons.
    next_block_height: BlockHeight,
    /// The block we are currently trying to propose for the next height, if any.
    ///
    /// This is always at the same height as `next_block_height`.
    pending_proposal: Option<PendingProposal>,

    /// A mutex that is held whilst we are performing operations that should not be
    /// attempted by multiple clients at the same time.
    client_mutex: Arc<Mutex<()>>,
}

impl ChainClientState {
    pub fn new(
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        pending_proposal: Option<PendingProposal>,
    ) -> ChainClientState {
        ChainClientState {
            block_hash,
            timestamp,
            next_block_height,
            pending_proposal,
            client_mutex: Arc::default(),
        }
    }

    pub fn block_hash(&self) -> Option<CryptoHash> {
        self.block_hash
    }

    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub fn next_block_height(&self) -> BlockHeight {
        self.next_block_height
    }

    pub fn pending_proposal(&self) -> &Option<PendingProposal> {
        &self.pending_proposal
    }

    pub(super) fn set_pending_proposal(&mut self, block: ProposedBlock, blobs: Vec<Blob>) {
        if block.height == self.next_block_height {
            let blobs = Vec::from_iter(blobs);
            assert_eq!(
                block.published_blob_ids(),
                BTreeSet::from_iter(blobs.iter().map(Blob::id))
            );
            self.pending_proposal = Some(PendingProposal { block, blobs });
        } else {
            tracing::error!(
                "Not setting pending block at height {}, because next_block_height is {}.",
                block.height,
                self.next_block_height
            );
        }
    }

    /// Returns whether the given ownership includes anyone whose secret key we don't have.
    pub fn has_other_owners(
        &self,
        ownership: &ChainOwnership,
        preferred_owner: &Option<AccountOwner>,
    ) -> bool {
        ownership
            .all_owners()
            .any(|owner| Some(owner) != preferred_owner.as_ref())
    }

    pub(super) fn update_from_info(&mut self, info: &ChainInfo) {
        if info.next_block_height > self.next_block_height {
            self.next_block_height = info.next_block_height;
            self.clear_pending_proposal();
            self.block_hash = info.block_hash;
            self.timestamp = info.timestamp;
        }
    }

    pub(super) fn clear_pending_proposal(&mut self) {
        self.pending_proposal = None;
    }

    pub(super) fn client_mutex(&self) -> Arc<Mutex<()>> {
        self.client_mutex.clone()
    }

    /// Returns an error if the chain info does not match the block hash and height.
    pub(super) fn check_info_is_up_to_date(
        &self,
        info: &ChainInfo,
    ) -> Result<(), ChainClientError> {
        ensure!(
            self.block_hash() == info.block_hash
                && self.next_block_height() == info.next_block_height,
            ChainClientError::BlockProposalError("The chain is not synchronized.")
        );
        Ok(())
    }
}
