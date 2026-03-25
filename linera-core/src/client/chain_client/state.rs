// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::sync::Mutex;

use super::super::PendingProposal;

/// Per-chain state holding the proposal mutex.
///
/// The mutex serves two purposes:
/// 1. It serializes block proposals so the client never makes conflicting proposals
///    (which could brick the chain in the Fast consensus round).
/// 2. Its locked value holds the pending proposal, ensuring that reads and writes
///    to the pending proposal are always synchronized with the proposal flow.
pub struct ChainClientState {
    /// Mutex that serializes block proposals. The locked value is the pending proposal
    /// (if any) that we are currently trying to commit.
    proposal_mutex: Arc<Mutex<Option<PendingProposal>>>,
}

impl ChainClientState {
    pub fn new(pending_proposal: Option<PendingProposal>) -> ChainClientState {
        ChainClientState {
            proposal_mutex: Arc::new(Mutex::new(pending_proposal)),
        }
    }

    /// Returns the proposal mutex for this chain.
    pub(in crate::client) fn proposal_mutex(&self) -> Arc<Mutex<Option<PendingProposal>>> {
        Arc::clone(&self.proposal_mutex)
    }
}
