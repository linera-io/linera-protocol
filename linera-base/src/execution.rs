// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Data-types used in the execution of Linera applications.

use crate::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Resources},
    doc_scalar,
    identifiers::{Account, ApplicationId, ChainId, ChannelName, Destination, MessageId, Owner},
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

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct RawExecutionOutcome<Message, Grant = Resources> {
    /// The signer who created the messages.
    pub authenticated_signer: Option<Owner>,
    /// Where to send a refund for the unused part of each grant after execution, if any.
    pub refund_grant_to: Option<Account>,
    /// Sends messages to the given destinations, possibly forwarding the authenticated
    /// signer and including grant with the refund policy described above.
    pub messages: Vec<RawOutgoingMessage<Message, Grant>>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelName, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(ChannelName, ChainId)>,
}

impl<Message, Grant> Default for RawExecutionOutcome<Message, Grant> {
    fn default() -> Self {
        Self {
            authenticated_signer: None,
            refund_grant_to: None,
            messages: Vec::new(),
            subscribe: Vec::new(),
            unsubscribe: Vec::new(),
        }
    }
}

impl<Message, Grant> RawExecutionOutcome<Message, Grant> {
    /// Sets the `authenticated_signer` of this [`RawExecutionOutcome`].
    pub fn with_authenticated_signer(mut self, authenticated_signer: Option<Owner>) -> Self {
        self.authenticated_signer = authenticated_signer;
        self
    }

    /// Sets the `refund_grant_to` field of this [`RawExecutionOutcome`].
    pub fn with_refund_grant_to(mut self, refund_grant_to: Option<Account>) -> Self {
        self.refund_grant_to = refund_grant_to;
        self
    }

    /// Adds a `message` to this [`RawExecutionOutcome`].
    pub fn add_raw_message(&mut self, message: RawOutgoingMessage<Message, Grant>) -> &mut Self {
        self.messages.push(message);
        self
    }

    /// Adds a `message` to this [`RawExecutionOutcome`].
    pub fn with_raw_message(mut self, message: RawOutgoingMessage<Message, Grant>) -> Self {
        self.add_raw_message(message);
        self
    }
}

impl<Message, Grant> RawExecutionOutcome<Message, Grant>
where
    Grant: Default,
{
    /// Adds a `message` to be sent to `destination` to this [`RawExecutionOutcome`].
    pub fn add_message(
        &mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> &mut Self {
        self.add_raw_message(RawOutgoingMessage {
            destination: destination.into(),
            authenticated: false,
            kind: MessageKind::Simple,
            grant: Grant::default(),
            message,
        })
    }

    /// Adds a `message` to be sent to `destination` to this [`RawExecutionOutcome`].
    pub fn with_message(mut self, destination: impl Into<Destination>, message: Message) -> Self {
        self.add_message(destination, message);
        self
    }

    /// Adds an authenticated `message` to this [`RawExecutionOutcome`]. Authenticated messages can
    /// act on behalf of the user that created them.
    pub fn add_authenticated_message(
        &mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> &mut Self {
        self.add_raw_message(RawOutgoingMessage {
            destination: destination.into(),
            authenticated: true,
            kind: MessageKind::Simple,
            grant: Grant::default(),
            message,
        })
    }

    /// Adds an authenticated `message` to this [`RawExecutionOutcome`]. Authenticated messages can
    /// act on behalf of the user that created them.
    pub fn with_authenticated_message(
        mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> Self {
        self.add_authenticated_message(destination, message);
        self
    }

    /// Adds a tracked `message` to this [`RawExecutionOutcome`]. Tracked messages are bounced if
    /// rejected on the receiving end. To differentiate bounced messages from original
    /// messages, the entrypoint `handle_message` should check `context.is_bounced`.
    pub fn add_tracked_message(
        &mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> &mut Self {
        self.add_raw_message(RawOutgoingMessage {
            destination: destination.into(),
            authenticated: false,
            kind: MessageKind::Tracked,
            grant: Grant::default(),
            message,
        })
    }

    /// Adds a tracked `message` to this [`RawExecutionOutcome`]. Tracked messages are bounced if
    /// rejected on the receiving end. To differentiate bounced messages from original
    /// messages, the entrypoint `handle_message` should check `context.is_bounced`.
    pub fn with_tracked_message(
        mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> Self {
        self.add_tracked_message(destination, message);
        self
    }

    /// Adds a tracked and authenticated message to the execution result. Tracked messages
    /// are bounced if rejected on the receiving end. To differentiate bounced messages
    /// from original messages, the entrypoint `handle_message` should check
    /// `context.is_bounced`.
    pub fn add_tracked_authenticated_message(
        &mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> &mut Self {
        self.add_raw_message(RawOutgoingMessage {
            destination: destination.into(),
            authenticated: true,
            kind: MessageKind::Tracked,
            grant: Grant::default(),
            message,
        })
    }

    /// Adds a tracked and authenticated message to the execution result. Tracked messages
    /// are bounced if rejected on the receiving end. To differentiate bounced messages
    /// from original messages, the entrypoint `handle_message` should check
    /// `context.is_bounced`.
    pub fn with_tracked_authenticated_message(
        mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> Self {
        self.add_tracked_authenticated_message(destination, message);
        self
    }
}

impl<Message, Grant> RawExecutionOutcome<Message, Grant>
where
    Message: Serialize,
{
    /// Serializes the messages in this [`RawExecutionOutcome`].
    pub fn serialize_messages(self) -> RawExecutionOutcome<Vec<u8>, Grant> {
        let messages = self
            .messages
            .into_iter()
            .map(RawOutgoingMessage::serialize_message)
            .collect();

        RawExecutionOutcome {
            authenticated_signer: None,
            refund_grant_to: None,
            messages,
            subscribe: self.subscribe,
            unsubscribe: self.unsubscribe,
        }
    }
}

/// The result of calling into an application.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ApplicationCallOutcome<Message, Value, SessionState> {
    /// The return value, if any.
    pub value: Value,
    /// The externally-visible result.
    pub execution_outcome: RawExecutionOutcome<Message>,
    /// New sessions were created with the following new states.
    pub create_sessions: Vec<SessionState>,
}

impl<Message, Value, SessionState> Default for ApplicationCallOutcome<Message, Value, SessionState>
where
    Value: Default,
{
    fn default() -> Self {
        Self {
            value: Value::default(),
            execution_outcome: RawExecutionOutcome::default(),
            create_sessions: vec![],
        }
    }
}

impl<Message, Value, SessionState> ApplicationCallOutcome<Message, Value, SessionState> {
    /// Adds a `message` to this [`ApplicationCallOutcome`].
    pub fn with_message(mut self, message: RawOutgoingMessage<Message>) -> Self {
        self.execution_outcome.messages.push(message);
        self
    }

    /// Registers a new session to be created with the provided `session_state`.
    pub fn with_new_session(mut self, session_state: SessionState) -> Self {
        self.create_sessions.push(session_state);
        self
    }
}

impl<Message, Value, SessionState> ApplicationCallOutcome<Message, Value, SessionState>
where
    Message: Serialize,
    Value: Serialize,
    SessionState: Serialize,
{
    /// Serializes the internal `Message`, `Value` and `SessionState` types into raw bytes.
    pub fn serialize_contents(self) -> ApplicationCallOutcome<Vec<u8>, Vec<u8>, Vec<u8>> {
        let value = bcs::to_bytes(&self.value)
            .expect("Failed to serialize `ApplicationCallOutcome`'s `Value`");
        let create_sessions = self
            .create_sessions
            .into_iter()
            .map(|session_state| {
                bcs::to_bytes(&session_state).expect("Failed to serialize the session state")
            })
            .collect();

        ApplicationCallOutcome {
            value,
            execution_outcome: self.execution_outcome.serialize_messages(),
            create_sessions,
        }
    }
}
