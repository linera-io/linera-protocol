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
pub struct State {
    /// Mutex that serializes block proposals. The locked value is the pending proposal
    /// (if any) that we are currently trying to commit.
    proposal_mutex: Arc<Mutex<Option<PendingProposal>>>,

    /// If true, only download blocks for this chain without fetching manager values.
    /// Use this for chains we're interested in observing but don't intend to propose blocks for.
    follow_only: bool,
}

impl State {
    pub fn new(pending_proposal: Option<PendingProposal>, follow_only: bool) -> State {
        State {
            proposal_mutex: Arc::new(Mutex::new(pending_proposal)),
            follow_only,
        }
    }

    /// Returns whether this chain is in follow-only mode.
    pub fn is_follow_only(&self) -> bool {
        self.follow_only
    }

    /// Returns a new `State` with the given `follow_only` value, sharing the same
    /// proposal mutex.
    pub(crate) fn with_follow_only(&self, follow_only: bool) -> State {
        State {
            proposal_mutex: Arc::clone(&self.proposal_mutex),
            follow_only,
        }
    }

    /// Returns the proposal mutex for this chain.
    pub(super) fn proposal_mutex(&self) -> Arc<Mutex<Option<PendingProposal>>> {
        Arc::clone(&self.proposal_mutex)
    }
}
