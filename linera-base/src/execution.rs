// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Data-types used in the execution of Linera applications.

use crate::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Resources},
    doc_scalar,
    identifiers::{Account, ApplicationId, ChainId, Destination, MessageId, Owner},
};
use serde::{Deserialize, Serialize};

/// The context of an application when it is executing an operation.
#[derive(Clone, Copy, Debug)]
pub struct OperationContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: u32,
    /// The index of the next message to be created.
    pub next_message_index: u32,
}

impl OperationContext {
    /// Returns the [`Account`] that should receive the refund of a grant provided for the
    /// execution of this context.
    pub fn refund_grant_to(&self) -> Option<Account> {
        Some(Account {
            chain_id: self.chain_id,
            owner: self.authenticated_signer,
        })
    }

    /// Returns the next [`MessageId`] to use for the next message to be sent.
    pub fn next_message_id(&self) -> MessageId {
        MessageId {
            chain_id: self.chain_id,
            height: self.height,
            index: self.next_message_index,
        }
    }
}

/// The context of an application when it is executing an incoming message.
#[derive(Clone, Copy, Debug)]
pub struct MessageContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// Whether the message was rejected by the original receiver and is now bouncing back.
    pub is_bouncing: bool,
    /// The authenticated signer of the operation that created the message, if any.
    pub authenticated_signer: Option<Owner>,
    /// Where to send a refund for the unused part of each grant after execution, if any.
    pub refund_grant_to: Option<Account>,
    /// The current block height.
    pub height: BlockHeight,
    /// The hash of the remote certificate that created the message.
    pub certificate_hash: CryptoHash,
    /// The id of the message (based on the operation height and index in the remote
    /// certificate).
    pub message_id: MessageId,
    /// The index of the next message to be created.
    pub next_message_index: u32,
}

/// The context of an application when it is executing a cross-application call or a session call.
#[derive(Clone, Copy, Debug)]
pub struct CalleeContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<ApplicationId>,
}

/// The context of an application service when it is handling a query.
#[derive(Clone, Copy, Debug)]
pub struct QueryContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The height of the next block on this chain.
    pub next_block_height: BlockHeight,
}

/// A message together with routing information.
#[derive(Clone, Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct RawOutgoingMessage<Message, Grant = Resources> {
    /// The destination of the message.
    pub destination: Destination,
    /// Whether the message is authenticated.
    pub authenticated: bool,
    /// The grant needed for message execution, typically specified as an `Amount` or as
    /// [`Resources`].
    pub grant: Grant,
    /// The kind of outgoing message being sent.
    pub kind: MessageKind,
    /// The message itself.
    pub message: Message,
}

impl<Message, Grant> RawOutgoingMessage<Message, Grant>
where
    Message: Serialize,
{
    /// Serializes the internal `Message` type into raw bytes.
    pub fn serialize_message(self) -> RawOutgoingMessage<Vec<u8>, Grant> {
        let message = bcs::to_bytes(&self.message).expect("Failed to serialize message");

        RawOutgoingMessage {
            destination: self.destination,
            authenticated: self.authenticated,
            grant: self.grant,
            kind: self.kind,
            message,
        }
    }
}

/// The kind of outgoing message being sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum MessageKind {
    /// The message can be skipped or rejected. No receipt is requested.
    Simple,
    /// The message cannot be skipped nor rejected. No receipt is requested.
    /// This only concerns certain system messages that cannot fail.
    Protected,
    /// The message cannot be skipped but can be rejected. A receipt must be sent
    /// when the message is rejected in a block of the receiver.
    Tracked,
    /// This event is a receipt automatically created when the original event was rejected.
    Bouncing,
}

doc_scalar!(MessageKind, "The kind of outgoing message being sent");
