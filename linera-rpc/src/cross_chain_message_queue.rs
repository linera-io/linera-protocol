// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and functions common to the gRPC and simple implementations.

#![cfg(with_server)]

use linera_base::identifiers::ChainId;
use linera_core::data_types::CrossChainRequest;

use crate::config::ShardId;

/// An discriminant for message queues: messages with the same queue ID will be delivered
/// in order.
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) struct QueueId {
    sender: ChainId,
    recipient: ChainId,
    is_update: bool,
}

impl QueueId {
    /// Returns a discriminant for the message's queue.
    pub(crate) fn new(request: &CrossChainRequest) -> Self {
        let (sender, recipient, is_update) = match request {
            CrossChainRequest::UpdateRecipient {
                sender, recipient, ..
            } => (*sender, *recipient, true),
            CrossChainRequest::ConfirmUpdatedRecipient {
                sender, recipient, ..
            } => (*sender, *recipient, false),
        };
        QueueId {
            sender,
            recipient,
            is_update,
        }
    }
}

pub(crate) enum Action {
    Proceed { id: usize },
    Retry,
}

#[derive(Clone)]
pub(crate) struct Task {
    pub shard_id: ShardId,
    pub request: linera_core::data_types::CrossChainRequest,
}

#[derive(Clone)]
pub(crate) struct Job {
    pub id: usize,
    pub retries: u32,
    pub nickname: String,
    pub task: Task,
}
