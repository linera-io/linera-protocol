// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and functions common to the gRPC and simple implementations.

#![cfg(with_server)]

use linera_base::identifiers::ChainId;
use linera_core::data_types::CrossChainRequest;

/// An discriminant for message queues: messages with the same queue ID will be delivered
/// in order.
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CrossChainQueueId {
    sender: ChainId,
    recipient: ChainId,
    is_update: bool,
}

impl CrossChainQueueId {
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
        CrossChainQueueId {
            sender,
            recipient,
            is_update,
        }
    }
}
