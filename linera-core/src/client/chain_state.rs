// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use linera_base::{
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{Blob, BlockHeight, Timestamp},
    identifiers::{BlobId, ChainId, Owner},
};
use linera_chain::data_types::Block;
use linera_execution::committee::ValidatorName;
use tokio::sync::Mutex;

use crate::data_types::ChainInfo;

/// The state of our interaction with a particular chain: how far we have synchronized it and
/// whether we are currently attempting to propose a new block.
pub struct ChainState {
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
    pending_block: Option<Block>,
    /// Known key pairs from present and past identities.
    known_key_pairs: BTreeMap<Owner, KeyPair>,
    /// The ID of the admin chain.
    admin_id: ChainId,

    /// For each validator, up to which index we have synchronized their
    /// [`ChainStateView::received_log`].
    received_certificate_trackers: HashMap<ValidatorName, u64>,
    /// This contains blobs belonging to our `pending_block` that may not even have
    /// been processed by (i.e. been proposed to) our own local chain manager yet.
    pending_blobs: BTreeMap<BlobId, Blob>,

    /// A mutex that is held whilst we are preparing the next block, to ensure that no
    /// other client can begin preparing a block.
    preparing_block: Arc<Mutex<()>>,
}

impl ChainState {
    pub fn new(
        known_key_pairs: Vec<KeyPair>,
        admin_id: ChainId,
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        pending_block: Option<Block>,
        pending_blobs: BTreeMap<BlobId, Blob>,
    ) -> ChainState {
        let known_key_pairs = known_key_pairs
            .into_iter()
            .map(|kp| (Owner::from(kp.public()), kp))
            .collect();
        let mut state = ChainState {
            known_key_pairs,
            admin_id,
            block_hash,
            timestamp,
            next_block_height,
            pending_block: None,
            pending_blobs,
            received_certificate_trackers: HashMap::new(),
            preparing_block: Arc::default(),
        };
        if let Some(block) = pending_block {
            state.set_pending_block(block);
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

    pub fn admin_id(&self) -> ChainId {
        self.admin_id
    }

    pub fn pending_block(&self) -> &Option<Block> {
        &self.pending_block
    }

    pub fn set_pending_block(&mut self, block: Block) {
        if block.height == self.next_block_height {
            self.pending_block = Some(block);
        }
    }

    pub fn pending_blobs(&self) -> &BTreeMap<BlobId, Blob> {
        &self.pending_blobs
    }

    pub fn insert_pending_blob(&mut self, blob: Blob) {
        self.pending_blobs.insert(blob.id(), blob);
    }

    pub fn known_key_pairs(&self) -> &BTreeMap<Owner, KeyPair> {
        &self.known_key_pairs
    }

    pub fn insert_known_key_pair(&mut self, key_pair: KeyPair) -> PublicKey {
        let new_public_key = key_pair.public();
        self.known_key_pairs.insert(new_public_key.into(), key_pair);
        new_public_key
    }

    pub fn received_certificate_trackers(&self) -> &HashMap<ValidatorName, u64> {
        &self.received_certificate_trackers
    }

    pub fn update_received_certificate_tracker(&mut self, name: ValidatorName, tracker: u64) {
        self.received_certificate_trackers
            .entry(name)
            .and_modify(|t| {
                // Because several synchronizations could happen in parallel, we need to make
                // sure to never go backward.
                if tracker > *t {
                    *t = tracker;
                }
            })
            .or_insert(tracker);
    }

    pub fn update_from_info(&mut self, info: &ChainInfo) {
        if info.next_block_height > self.next_block_height {
            self.next_block_height = info.next_block_height;
            self.clear_pending_block();
            self.block_hash = info.block_hash;
            self.timestamp = info.timestamp;
        }
    }

    pub fn clear_pending_block(&mut self) {
        self.pending_block = None;
        self.pending_blobs.clear();
    }

    pub fn preparing_block(&self) -> Arc<Mutex<()>> {
        self.preparing_block.clone()
    }
}
