// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides an SDK for developing Linera applications using Rust.
//!
//! A Linera application consists of two WebAssembly binaries: a contract and a service.
//! In both binaries, there should be a shared application state. The state is a type that
//! represents what the application would like to persist in storage across blocks, and
//! must implement the [`Contract`](crate::Contract) trait in the contract binary and the
//! [`Service`](crate::Service) trait in the service binary.
//!
//! The application can select between two storage backends to use. Selecting the storage
//! backend is done by specifying both the [`Contract::Storage`](crate::Contract::Storage)
//! and the [`Service::Storage`](crate::Service::Storage) associated types.
//!
//! The [`SimpleStateStorage`](crate::SimpleStateStorage) backend stores the application's
//! state type by serializing it into binary blob. This allows the entire contents of the
//! state to be persisted and made available to the application when it is executed.
//!
//! The [`ViewStateStorage`](crate::ViewStateStorage) backend stores the application's
//! state using the
//! [`linera-views`](https://docs.rs/linera-views/latest/linera_views/index.html), a
//! framework that allows loading selected parts of the state. This is useful if the
//! application's state is large and doesn't need to be loaded in its entirety for every
//! execution.
//!
//! The contract binary should use the [`contract!`](crate::contract!) macro to export the application's contract
//! endpoints implemented via the [`Contract`](crate::Contract) trait implementation.
//!
//! The service binary should use the [`service!`](crate::service!) macro to export the application's service
//! endpoints implemented via the [`Service`](crate::Service) trait implementation.
//!
//! # Examples
//!
//! The [`examples`](https://github.com/linera-io/linera-protocol/tree/main/examples)
//! directory contains some example applications.

#![deny(missing_docs)]

pub mod base;
pub mod contract;
mod extensions;
pub mod graphql;
mod log;
pub mod service;
#[cfg(feature = "test")]
#[cfg_attr(not(target_arch = "wasm32"), path = "./test/integration/mod.rs")]
#[cfg_attr(target_arch = "wasm32", path = "./test/unit/mod.rs")]
pub mod test;
pub mod util;
pub mod views;

use self::contract::ContractStateStorage;
use async_trait::async_trait;
use linera_base::{
    abi::{ContractAbi, ServiceAbi, WithContractAbi, WithServiceAbi},
    data_types::BlockHeight,
    identifiers::{ApplicationId, ChainId, ChannelName, Destination, MessageId, Owner},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{error::Error, fmt::Debug, sync::Arc};

pub use self::{
    contract::ContractRuntime,
    extensions::{FromBcsBytes, ToBcsBytes},
    log::{ContractLogger, ServiceLogger},
    service::{ServiceRuntime, ServiceStateStorage},
};
pub use linera_base::{abi, data_types::Resources, ensure, identifiers::SessionId};
#[doc(hidden)]
pub use wit_bindgen_guest_rust;

/// A simple state management runtime based on a single byte array.
pub struct SimpleStateStorage<A>(std::marker::PhantomData<A>);

/// A state management runtime based on [`linera_views`].
pub struct ViewStateStorage<A>(std::marker::PhantomData<A>);

/// The contract interface of a Linera application.
///
/// As opposed to the [`Service`] interface of an application, contract entry points
/// are triggered by the execution of blocks in a chain. Their execution may modify
/// storage and is gas-metered.
///
/// Below we use the word "transaction" to refer to the current operation or message being
/// executed.
#[async_trait]
pub trait Contract: WithContractAbi + ContractAbi + Send + Sized {
    /// The type used to report errors to the execution environment.
    ///
    /// Errors are not recoverable and always interrupt the current transaction. To return
    /// recoverable errors in the case of application calls and session calls, you may use
    /// the response types.
    type Error: Error + From<serde_json::Error> + From<bcs::Error> + 'static;

    /// The desired storage backend used to store the application's state.
    ///
    /// Currently, the two supported backends are [`SimpleStateStorage`] or
    /// [`ViewStateStorage`]. Accordingly, this associated type may be defined as `type
    /// Storage = SimpleStateStorage<Self>` or `type Storage = ViewStateStorage<Self>`.
    ///
    /// The first deployment on other chains will use the [`Default`] implementation of the application
    /// state if [`SimpleStateStorage`] is used, or the [`Default`] value of all sub-views in the
    /// state if the [`ViewStateStorage`] is used.
    type Storage: ContractStateStorage<Self> + Send + 'static;

    /// Initializes the application on the chain that created it.
    ///
    /// This is only called once when the application is created and only on the microchain that
    /// created the application.
    ///
    /// Returns an [`ExecutionOutcome`], which can contain subscription or unsubscription requests
    /// to channels and messages to be sent to this application on another chain.
    async fn initialize(
        &mut self,
        runtime: &mut ContractRuntime,
        argument: Self::InitializationArgument,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error>;

    /// Applies an operation from the current block.
    ///
    /// Operations are created by users and added to blocks, serving as the starting point for an
    /// application's execution.
    ///
    /// Returns an [`ExecutionOutcome`], which can contain subscription or unsubscription requests
    /// to channels and messages to be sent to this application on another chain.
    async fn execute_operation(
        &mut self,
        runtime: &mut ContractRuntime,
        operation: Self::Operation,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error>;

    /// Applies a message originating from a cross-chain message.
    ///
    /// Messages are messages sent across chains. These messages are created and received by
    /// the same application. Messages can be either single-sender and single-receiver, or
    /// single-sender and multiple-receivers. The former allows sending cross-chain messages to the
    /// application on some other specific chain, while the latter uses broadcast channels to
    /// send a message to multiple other chains where the application is subscribed to a
    /// sender channel on this chain.
    ///
    /// For a message to be executed, a user must mark it to be received in a block of the receiver
    /// chain.
    ///
    /// Returns an [`ExecutionOutcome`], which can contain messages to be sent to this application
    /// on another chain and subscription or unsubscription requests to channels.
    async fn execute_message(
        &mut self,
        runtime: &mut ContractRuntime,
        message: Self::Message,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error>;

    /// Handles a call from another application.
    ///
    /// Cross-application calls allow applications to interact inside a chain. An
    /// application can call any other application available on the chain the
    /// execution is taking place on.
    ///
    /// Use the `Self::call_application` method generated by the [`contract!`] macro to call
    /// another application.
    ///
    /// Returns an [`ApplicationCallOutcome`], which contains:
    ///
    /// - a return value sent to the caller application;
    /// - an [`ExecutionOutcome`] with messages to be sent to this application on other chains
    ///   and channel subscription and unsubscription requests.
    ///
    /// See [`Self::handle_session_call`] for more information on
    async fn handle_application_call(
        &mut self,
        runtime: &mut ContractRuntime,
        argument: Self::ApplicationCall,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallOutcome<Self::Message, Self::Response>, Self::Error>;

    /// Handles a call into a session created by this application.
    ///
    /// Sessions are another way for applications to interact inside a chain. Sessions
    /// are very similar to cross-application calls (see [`Self::handle_application_call`]), but
    /// each one of them has a separate state, in addition to the application state.
    ///
    /// Sessions allow representing transferrable objects (typically assets and
    /// liabilities) for the duration of a transaction. A session is initially *owned* by
    /// the application that made a call to create it. Ownership over a session can be
    /// transferred to other applications by passing them as session arguments. The
    /// execution environment keeps track of which application is the current owner of the
    /// session.
    ///
    /// The state of a session is only visible to the application that created it (and
    /// which handles the session calls). Other applications only see the [`SessionId`]
    /// and may only interact with the session through session calls.
    ///
    /// At the end of the execution of a block, no sessions should be alive. If a block's execution
    /// ends with any leaked sessions, the block is rejected. This means that all sessions should
    /// be called at least once in a way that the call terminates the session (by returning
    /// [`None`] as the updated session data in [`SessionCallOutcome::new_state`]).
    ///
    /// Use the `Self::call_session` method generated by the [`contract!`] macro to call
    /// a session.
    ///
    /// Returns a [`SessionCallOutcome`] after the call into the session has finished executing,
    /// which contains:
    ///
    /// - the updated session state, or `None` if the session should be terminated;
    /// - an [`ApplicationCallOutcome`], which contains:
    ///   - a return value sent to the caller application;
    ///   - an [`ExecutionOutcome`] with messages to be sent to this application on other
    ///     chains and channel subscription and unsubscription requests.
    async fn handle_session_call(
        &mut self,
        runtime: &mut ContractRuntime,
        session: Self::SessionState,
        argument: Self::SessionCall,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Self::Message, Self::Response, Self::SessionState>, Self::Error>;

    /// Finishes the execution of the current transaction.
    ///
    /// This is called once before a transaction ends, to allow all applications that participated
    /// in the transaction to perform any final operations, and optionally it may also cancel the
    /// transaction if there are any pendencies.
    ///
    /// The default implementation persists the state, so if this method is overriden, care must be
    /// taken to persist the state manually.
    async fn finalize(
        &mut self,
        _runtime: &mut ContractRuntime,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        Self::Storage::store(self).await;
        Ok(ExecutionOutcome::default())
    }

    /// Calls another application.
    fn call_application<A: ContractAbi + Send>(
        &mut self,
        authenticated: bool,
        application: ApplicationId<A>,
        call: &A::ApplicationCall,
    ) -> Result<A::Response, Self::Error> {
        let call_bytes = bcs::to_bytes(call)?;
        let response_bytes = crate::contract::system_api::call_application(
            authenticated,
            application.forget_abi(),
            &call_bytes,
        );
        let response = bcs::from_bytes(&response_bytes)?;
        Ok(response)
    }

    /// Retrieves the parameters of the application.
    fn parameters() -> Result<Self::Parameters, Self::Error> {
        let bytes = crate::contract::system_api::current_application_parameters();
        let parameters = serde_json::from_slice(&bytes)?;
        Ok(parameters)
    }

    /// Retrieves the current application ID.
    fn current_application_id() -> ApplicationId<Self::Abi> {
        crate::contract::system_api::current_application_id().with_abi()
    }
}

/// The service interface of a Linera application.
///
/// As opposed to the [`Contract`] interface of an application, service entry points
/// are triggered by JSON queries (typically GraphQL). Their execution cannot modify
/// storage and is not gas-metered.
#[async_trait]
pub trait Service: WithServiceAbi + ServiceAbi {
    /// Type used to report errors to the execution environment.
    ///
    /// Errors are not recoverable and always interrupt the current query.
    type Error: Error + From<serde_json::Error>;

    /// The desired storage backend used to store the application's state.
    ///
    /// Currently, the two supported backends are [`SimpleStateStorage`] or
    /// [`ViewStateStorage`]. Accordingly, this associated type may be defined as `type
    /// Storage = SimpleStateStorage<Self>` or `type Storage = ViewStateStorage<Self>`.
    type Storage: ServiceStateStorage;

    /// Executes a read-only query on the state of this application.
    async fn handle_query(
        self: Arc<Self>,
        runtime: &ServiceRuntime,
        query: Self::Query,
    ) -> Result<Self::QueryResponse, Self::Error>;

    /// Queries another application.
    fn query_application<A: ServiceAbi + Send>(
        application: ApplicationId<A>,
        query: &A::Query,
    ) -> Result<A::QueryResponse, Self::Error>
    where
        Self::Error: From<String>,
    {
        let query_bytes = serde_json::to_vec(&query)?;
        let response_bytes =
            crate::service::system_api::query_application(application.forget_abi(), &query_bytes);
        let response = serde_json::from_slice(&response_bytes)?;
        Ok(response)
    }

    /// Retrieves the parameters of the application.
    fn parameters() -> Result<Self::Parameters, Self::Error> {
        let bytes = crate::service::system_api::current_application_parameters();
        let parameters = serde_json::from_slice(&bytes)?;
        Ok(parameters)
    }
}

/// The context of the execution of an application's operation.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OperationContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: u32,
}

/// The context of the execution of an application's message.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MessageContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// Whether the message was rejected by the original receiver and is now bouncing back.
    pub is_bouncing: bool,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The ID of the message (based on the operation height and index in the remote
    /// chain that created the message).
    pub message_id: MessageId,
}

/// The context of the execution of an application's cross-application call or session call handler.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CalleeContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<ApplicationId>,
}

/// The context of the execution of an application's query.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryContext {
    /// The current chain ID.
    pub chain_id: ChainId,
    /// The height of the next block on this chain.
    pub next_block_height: BlockHeight,
}

/// A message together with routing information.
#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct OutgoingMessage<Message> {
    /// The destination of the message.
    pub destination: Destination,
    /// Whether the message is authenticated.
    pub authenticated: bool,
    /// Whether the message is tracked.
    pub is_tracked: bool,
    /// Resources to be forwarded with the message.
    pub resources: Resources,
    /// The message itself.
    pub message: Message,
}

impl<Message> OutgoingMessage<Message>
where
    Message: Serialize,
{
    /// Serializes the internal `Message` type into raw bytes.
    pub fn into_raw(self) -> OutgoingMessage<Vec<u8>> {
        let message = bcs::to_bytes(&self.message).expect("Failed to serialize message");

        OutgoingMessage {
            destination: self.destination,
            authenticated: self.authenticated,
            is_tracked: self.is_tracked,
            resources: self.resources,
            message,
        }
    }
}

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ExecutionOutcome<Message> {
    /// Sends messages to the given destinations, possibly forwarding the authenticated
    /// signer.
    pub messages: Vec<OutgoingMessage<Message>>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelName, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(ChannelName, ChainId)>,
}

impl<Message> Default for ExecutionOutcome<Message> {
    fn default() -> Self {
        Self {
            messages: vec![],
            subscribe: vec![],
            unsubscribe: vec![],
        }
    }
}

impl<Message: Serialize + Debug + DeserializeOwned> ExecutionOutcome<Message> {
    /// Adds a message to the execution result.
    pub fn with_message(mut self, destination: impl Into<Destination>, message: Message) -> Self {
        let destination = destination.into();
        self.messages.push(OutgoingMessage {
            destination,
            authenticated: false,
            is_tracked: false,
            resources: Resources::default(),
            message,
        });
        self
    }

    /// Adds an authenticated message to the execution result. Authenticated messages can
    /// act on behalf of the user that created them.
    pub fn with_authenticated_message(
        mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> Self {
        let destination = destination.into();
        self.messages.push(OutgoingMessage {
            destination,
            authenticated: true,
            is_tracked: false,
            resources: Resources::default(),
            message,
        });
        self
    }

    /// Adds a tracked message to the execution result. Tracked messages are bounced if
    /// rejected on the receiving end. To differentiate bounced messages from original
    /// messages, the entrypoint `handle_message` should check `context.is_bounced`.
    pub fn with_tracked_message(
        mut self,
        destination: impl Into<Destination>,
        message: Message,
    ) -> Self {
        let destination = destination.into();
        self.messages.push(OutgoingMessage {
            destination,
            authenticated: false,
            is_tracked: true,
            resources: Resources::default(),
            message,
        });
        self
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
        let destination = destination.into();
        self.messages.push(OutgoingMessage {
            destination,
            authenticated: true,
            is_tracked: true,
            resources: Resources::default(),
            message,
        });
        self
    }

    /// Converts this [`ExecutionOutcome`] into a raw [`ExecutionOutcome`], by serializing the
    /// messages.
    pub fn into_raw(self) -> ExecutionOutcome<Vec<u8>> {
        let messages = self
            .messages
            .into_iter()
            .map(OutgoingMessage::into_raw)
            .collect();

        ExecutionOutcome {
            messages,
            subscribe: self.subscribe,
            unsubscribe: self.unsubscribe,
        }
    }
}

/// The result of calling into an application.
#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ApplicationCallOutcome<Message, Value> {
    /// The return value, if any.
    pub value: Value,
    /// The externally-visible result.
    pub execution_outcome: ExecutionOutcome<Message>,
}

impl<Message, Value> Default for ApplicationCallOutcome<Message, Value>
where
    Value: Default,
{
    fn default() -> Self {
        Self {
            value: Value::default(),
            execution_outcome: ExecutionOutcome::default(),
        }
    }
}

impl<Message, Value> ApplicationCallOutcome<Message, Value>
where
    Message: Debug + DeserializeOwned + Serialize,
    Value: Serialize,
{
    /// Serializes the internal `Message`, `Value` and `SessionState` types into raw bytes.
    pub fn into_raw(self) -> ApplicationCallOutcome<Vec<u8>, Vec<u8>> {
        let value = bcs::to_bytes(&self.value)
            .expect("Failed to serialize `ApplicationCallOutcome`'s `Value`");

        ApplicationCallOutcome {
            value,
            execution_outcome: self.execution_outcome.into_raw(),
        }
    }
}

/// The result of calling into a session.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SessionCallOutcome<Message, Value, SessionState> {
    /// The result of the application call.
    pub inner: ApplicationCallOutcome<Message, Value>,
    /// The new state of the session, if any. `None` means that the session was consumed
    /// by the call.
    pub new_state: Option<SessionState>,
}

impl<Message, Value, SessionState> SessionCallOutcome<Message, Value, SessionState>
where
    Message: Debug + DeserializeOwned + Serialize,
    Value: Serialize,
    SessionState: Serialize,
{
    /// Serializes the internal `Message`, `Value` and `SessionState` types into raw bytes.
    pub fn into_raw(self) -> SessionCallOutcome<Vec<u8>, Vec<u8>, Vec<u8>> {
        let new_state = self.new_state.map(|session_state| {
            bcs::to_bytes(&session_state).expect("Failed to serialize new session state")
        });

        SessionCallOutcome {
            inner: self.inner.into_raw(),
            new_state,
        }
    }
}
