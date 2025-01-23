// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use linera_base::{
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{Blob, BlockHeight, Timestamp},
    ensure,
    identifiers::{BlobId, Owner},
    ownership::ChainOwnership,
};
use linera_chain::data_types::ProposedBlock;
use tokio::sync::Mutex;

use super::ChainClientError;
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
    pending_proposal: Option<ProposedBlock>,
    /// Known key pairs from present and past identities.
    known_key_pairs: BTreeMap<Owner, KeyPair>,

    /// This contains blobs belonging to our `pending_block` that may not even have
    /// been processed by (i.e. been proposed to) our own local chain manager yet.
    pending_blobs: BTreeMap<BlobId, Blob>,

    /// A mutex that is held whilst we are performing operations that should not be
    /// attempted by multiple clients at the same time.
    client_mutex: Arc<Mutex<()>>,
}

impl ChainClientState {
    pub fn new(
        known_key_pairs: Vec<KeyPair>,
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        pending_block: Option<ProposedBlock>,
        pending_blobs: BTreeMap<BlobId, Blob>,
    ) -> ChainClientState {
        let known_key_pairs = known_key_pairs
            .into_iter()
            .map(|kp| (Owner::from(kp.public()), kp))
            .collect();
        let mut state = ChainClientState {
            known_key_pairs,
            block_hash,
            timestamp,
            next_block_height,
            pending_proposal: None,
            pending_blobs,
            client_mutex: Arc::default(),
        };
        if let Some(block) = pending_block {
            state.set_pending_proposal(block);
        }
        state
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

    pub fn pending_proposal(&self) -> &Option<ProposedBlock> {
        &self.pending_proposal
    }

    pub(super) fn set_pending_proposal(&mut self, block: ProposedBlock) {
        if block.height == self.next_block_height {
            self.pending_proposal = Some(block);
        } else {
            tracing::error!(
                "Not setting pending block at height {}, because next_block_height is {}.",
                block.height,
                self.next_block_height
            );
        }
    }

    pub fn pending_blobs(&self) -> &BTreeMap<BlobId, Blob> {
        &self.pending_blobs
    }

    pub(super) fn insert_pending_blob(&mut self, blob: Blob) {
        self.pending_blobs.insert(blob.id(), blob);
    }

    pub fn known_key_pairs(&self) -> &BTreeMap<Owner, KeyPair> {
        &self.known_key_pairs
    }

    /// Returns whether the given ownership includes anyone whose secret key we don't have.
    pub fn has_other_owners(&self, ownership: &ChainOwnership) -> bool {
        ownership
            .all_owners()
            .any(|owner| !self.known_key_pairs.contains_key(owner))
    }

    pub(super) fn insert_known_key_pair(&mut self, key_pair: KeyPair) -> PublicKey {
        let new_public_key = key_pair.public();
        self.known_key_pairs.insert(new_public_key.into(), key_pair);
        new_public_key
    }

    pub(super) fn update_from_info(&mut self, info: &ChainInfo) {
        if info.next_block_height > self.next_block_height {
            self.next_block_height = info.next_block_height;
            self.clear_pending_block();
            self.block_hash = info.block_hash;
            self.timestamp = info.timestamp;
        }
    }

    pub(super) fn clear_pending_block(&mut self) {
        self.pending_proposal = None;
        self.pending_blobs.clear();
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
