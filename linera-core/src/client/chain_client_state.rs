// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeSet, sync::Arc};

use linera_base::data_types::Blob;
use linera_chain::data_types::ProposedBlock;
use tokio::sync::Mutex;

use super::PendingProposal;
use crate::data_types::ChainInfo;

/// The state of our interaction with a particular chain: how far we have synchronized it and
/// whether we are currently attempting to propose a new block.
pub struct ChainClientState {
    /// The block we are currently trying to propose for the next height, if any.
    ///
    /// This is always at the same height as `next_block_height`.
    pending_proposal: Option<PendingProposal>,

    /// A mutex that is held whilst we are performing operations that should not be
    /// attempted by multiple clients at the same time.
    client_mutex: Arc<Mutex<()>>,
}

impl ChainClientState {
    pub fn new(pending_proposal: Option<PendingProposal>) -> ChainClientState {
        ChainClientState {
            pending_proposal,
            client_mutex: Arc::default(),
        }
    }

    pub fn pending_proposal(&self) -> &Option<PendingProposal> {
        &self.pending_proposal
    }

    pub(super) fn set_pending_proposal(&mut self, block: ProposedBlock, blobs: Vec<Blob>) {
        if self
            .pending_proposal
            .as_ref()
            .is_some_and(|pending| pending.block.height >= block.height)
        {
            tracing::error!(
                "Not setting pending block at {}, because we already have a pending proposal.",
                block.height
            );
            return;
        }
        let blobs = Vec::from_iter(blobs);
        assert_eq!(
            block.published_blob_ids(),
            BTreeSet::from_iter(blobs.iter().map(Blob::id))
        );
        self.pending_proposal = Some(PendingProposal { block, blobs });
    }

    pub(super) fn update_from_info(&mut self, info: &ChainInfo) {
        if self
            .pending_proposal
            .as_ref()
            .is_some_and(|pending| pending.block.height < info.next_block_height)
        {
            self.clear_pending_proposal();
        }
    }

    pub(super) fn clear_pending_proposal(&mut self) {
        self.pending_proposal = None;
    }

    pub(super) fn client_mutex(&self) -> Arc<Mutex<()>> {
        self.client_mutex.clone()
    }
}
