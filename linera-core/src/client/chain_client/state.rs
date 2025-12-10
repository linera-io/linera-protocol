// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeSet, sync::Arc};

use linera_base::data_types::Blob;
use linera_chain::data_types::ProposedBlock;
use tokio::sync::Mutex;

use super::super::PendingProposal;
use crate::data_types::ChainInfo;

/// The state of our interaction with a particular chain: how far we have synchronized it and
/// whether we are currently attempting to propose a new block.
pub struct State {
    /// The block we are currently trying to propose for the next height, if any.
    ///
    /// This is always at the same height as `next_block_height`.
    pending_proposal: Option<PendingProposal>,

    /// A mutex that is held whilst we are performing operations that should not be
    /// attempted by multiple clients at the same time.
    client_mutex: Arc<Mutex<()>>,

    /// If true, only download blocks for this chain without fetching manager values.
    /// Use this for chains we're interested in observing but don't intend to propose blocks for.
    follow_only: bool,
}

impl State {
    pub fn new(pending_proposal: Option<PendingProposal>, follow_only: bool) -> State {
        State {
            pending_proposal,
            client_mutex: Arc::default(),
            follow_only,
        }
    }

    /// Clones the state. This must only be used to update the state, and one of the two clones
    /// must be dropped.
    pub(crate) fn clone_for_update_unchecked(&self) -> State {
        State {
            pending_proposal: self.pending_proposal.clone(),
            client_mutex: Arc::clone(&self.client_mutex),
            follow_only: self.follow_only,
        }
    }

    /// Returns whether this chain is in follow-only mode.
    pub fn is_follow_only(&self) -> bool {
        self.follow_only
    }

    /// Sets whether this chain is in follow-only mode.
    pub fn set_follow_only(&mut self, follow_only: bool) {
        self.follow_only = follow_only;
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
        assert_eq!(
            block.published_blob_ids(),
            BTreeSet::from_iter(blobs.iter().map(Blob::id))
        );
        self.pending_proposal = Some(PendingProposal { block, blobs });
    }

    pub(crate) fn update_from_info(&mut self, info: &ChainInfo) {
        if let Some(pending) = &self.pending_proposal {
            if pending.block.height < info.next_block_height {
                tracing::debug!(
                    "Clearing pending proposal: a block was committed at height {}",
                    pending.block.height
                );
                self.clear_pending_proposal();
            }
        }
    }

    pub(super) fn clear_pending_proposal(&mut self) {
        self.pending_proposal = None;
    }

    pub(super) fn client_mutex(&self) -> Arc<Mutex<()>> {
        self.client_mutex.clone()
    }
}
