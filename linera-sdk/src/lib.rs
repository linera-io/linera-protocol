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
mod exported_future;
mod extensions;
mod log;
pub mod service;
#[cfg(feature = "test")]
#[cfg_attr(not(target_arch = "wasm32"), path = "./test/integration/mod.rs")]
#[cfg_attr(target_arch = "wasm32", path = "./test/unit/mod.rs")]
pub mod test;
pub mod views;

use async_trait::async_trait;
use custom_debug_derive::Debug;
use linera_base::{
    data_types::BlockHeight,
    identifiers::{ApplicationId, ChainId, ChannelName, Destination, EffectId, Owner, SessionId},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{error::Error, sync::Arc};

pub use self::{
    exported_future::ExportedFuture,
    extensions::{FromBcsBytes, ToBcsBytes},
    log::{ContractLogger, ServiceLogger},
};
pub use linera_base::ensure;
#[doc(hidden)]
pub use wit_bindgen_guest_rust;

/// A simple state management runtime using a single byte array.
pub struct SimpleStateStorage<A>(std::marker::PhantomData<A>);

/// A state management runtime based on `linera-views`.
pub struct ViewStateStorage<A>(std::marker::PhantomData<A>);

/// The public entry points provided by an application's contract.
///
/// Execution of these endpoints consume fuel, because they can change the application's state and
/// are therefore consensus criticial.
#[async_trait]
pub trait Contract: Sized {
    /// Message reports for application execution errors.
    type Error: Error + From<serde_json::Error> + From<bcs::Error>;
    /// The desired storage backend to use to store the application's state.
    type Storage;
    /// Initialization Arguments.
    type InitializationArguments: DeserializeOwned + Send;
    /// Execute Operation Arguments.
    type Operation: DeserializeOwned + Send;
    /// Application Call Arguments.
    type ApplicationCallArguments: DeserializeOwned + Serialize + Send;
    /// The Application Effect.
    type Effect: DeserializeOwned + Serialize + Send + std::fmt::Debug;
    /// A Session Call.
    type SessionCall: DeserializeOwned + Send;
    /// The response type of an application call.
    type Response: Serialize + Send;
    /// The type for the contract's sessions.
    type SessionState: DeserializeOwned + Serialize + Send;

    /// Initializes the application on the chain that created it.
    ///
    /// This is only called once when the application is created and only on the microchain that
    /// created the application.
    ///
    /// Deployment on other microchains will use the [`Default`] implementation of the application
    /// state if [`SimpleStateStorage`] is used, or the [`Default`] value of all sub-views in the
    /// state if the [`ViewStateStorage`] is used.
    ///
    /// Returns an [`ExecutionResult`], which can contain subscription or unsubscription requests
    /// to channels and effects to be sent to this application on another microchain.
    async fn initialize(
        &mut self,
        context: &OperationContext,
        argument: Self::InitializationArguments,
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error>;

    /// Applies an operation from the current block.
    ///
    /// Operations are created by users and added to blocks, serving as the starting point for an
    /// application's execution.
    ///
    /// Returns an [`ExecutionResult`], which can contain subscription or unsubscription requests
    /// to channels and effects to be sent to this application on another microchain.
    async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: Self::Operation,
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error>;

    /// Applies an effect originating from a cross-chain message.
    ///
    /// Effects are messages sent across microchains. These messages are created and received by
    /// the same application. Effects can be either single-sender and single-receiver, or
    /// single-sender and multiple-receivers. The former allows sending cross-chain messages to the
    /// application on some other specific microchain, while the latter uses broadcast channels to
    /// send a message to multiple other microchains where the application is subscribed to a
    /// sender channel on this microchain.
    ///
    /// For an effect to be executed, a user must mark it to be received in a block of the receiver
    /// microchain.
    ///
    /// Returns an [`ExecutionResult`], which can contain effects to be sent to this application
    /// on another microchain and subscription or unsubscription requests to channels.
    async fn execute_effect(
        &mut self,
        context: &EffectContext,
        effect: Self::Effect,
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error>;

    /// Handles a call from another application.
    ///
    /// Cross-application calls are a way for applications to interact inside a microchain. An
    /// application can call any other application available on the microchain the execution is
    /// taking place on.
    ///
    /// Use the `Self::call_application` method generated by the [`contract!`] macro to call
    /// another application.
    ///
    /// Returns an [`ApplicationCallResult`], which contains:
    ///
    /// - a response `value` sent to the caller application;
    /// - a list of newly created [`Session`]s sent to the caller application;
    /// - an [`ExecutionResult`] with effects to be sent to this application on other microchains
    ///   and channel subscription and unsubscription requests.
    ///
    /// See [`Self::handle_session_call`] for more information on
    async fn handle_application_call(
        &mut self,
        context: &CalleeContext,
        argument: Self::ApplicationCallArguments,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult<Self::Effect, Self::Response, Self::SessionState>, Self::Error>;

    /// Handles a call into a [`Session`] created by this application.
    ///
    /// [`Session`]s are another way for applications to interact inside a microchain. Sessions
    /// are very similar to cross-application calls (see [`Self::handle_application_call`]), but
    /// each one of them has a separate state, that's also separate from the application state.
    ///
    /// This allows moving parts of the application's state out into sessions, which are sent
    /// to other applications. These sessions can be freely moved between other applications, and
    /// the application that created the session does not have to keep track of which application
    /// is the current owner of the session.
    ///
    /// Other applications receive [`SessionId`]s to reference the sessions, so that they can't
    /// read or alter the session's state. The only way for an application to interact with another
    /// application's session is through calls into that session.
    ///
    /// At the end of the execution of a block, no sessions should be alive. If a block's execution
    /// ends with any leaked sessions, the block is rejected. This means that all sessions should
    /// be called at least once in a way that the call terminates the session (by returning
    /// [`None`] as the updated session data in [`SessionCallResult::data`]).
    ///
    /// Use the `Self::call_application` method generated by the [`contract!`] macro to call
    /// another application.
    ///
    /// Returns a [`SessionCallResult`] after the call into the session has finished executing,
    /// which contains:
    ///
    /// - the updated session `data`, or `None` if the session should be terminated;
    /// - an [`ApplicationCallResult`], which contains:
    ///   - a response `value` sent to the caller application;
    ///   - a list of newly created [`Session`]s sent to the caller application;
    ///   - an [`ExecutionResult`] with effects to be sent to this application on other
    ///     microchains and channel subscription and unsubscription requests.
    async fn handle_session_call(
        &mut self,
        context: &CalleeContext,
        session: Session<Self::SessionState>,
        argument: Self::SessionCall,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult<Self::Effect, Self::Response, Self::SessionState>, Self::Error>;
}

/// The public entry points provided by an application's service.
///
/// Execution of these endpoints does *not* consume fuel, because they can't change the
/// application's state and are therefore *not* consensus criticial.
#[async_trait]
pub trait Service {
    /// Message reports for service execution errors.
    type Error: Error;
    /// The desired storage backend to use to read the application's state.
    type Storage;

    /// Executes a read-only query on the state of this application.
    async fn query_application(
        self: Arc<Self>,
        context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error>;
}

/// The context of the execution of an application's operation.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OperationContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: u32,
}

/// The context of the execution of an application's effect.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EffectContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The authenticated signer of the operation, if any.
    pub authenticated_signer: Option<Owner>,
    /// The current block height.
    pub height: BlockHeight,
    /// The id of the effect (based on the operation height and index in the remote
    /// chain that created the effect).
    pub effect_id: EffectId,
}

/// The context of the execution of an application's cross-application call or session call handler.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CalleeContext {
    /// The current chain id.
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
    /// The current chain id.
    pub chain_id: ChainId,
}

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ExecutionResult<Effect> {
    /// Sends messages to the given destinations, possibly forwarding the authenticated
    /// signer.
    pub effects: Vec<(Destination, bool, Effect)>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelName, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(ChannelName, ChainId)>,
}

impl<Effect> Default for ExecutionResult<Effect> {
    fn default() -> Self {
        Self {
            effects: vec![],
            subscribe: vec![],
            unsubscribe: vec![],
        }
    }
}

impl<Effect: Serialize + std::fmt::Debug + DeserializeOwned> ExecutionResult<Effect> {
    /// Adds an effect to the execution result.
    pub fn with_effect(mut self, destination: impl Into<Destination>, effect: Effect) -> Self {
        self.effects.push((destination.into(), false, effect));
        self
    }

    /// Adds an authenticated effect to the execution result.
    pub fn with_authenticated_effect(
        mut self,
        destination: impl Into<Destination>,
        effect: Effect,
    ) -> Self {
        self.effects.push((destination.into(), true, effect));
        self
    }
}

/// The result of calling into a user application.
#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ApplicationCallResult<Effect, Value, SessionType> {
    /// The return value.
    // #[debug(with = "linera_base::hex_debug")]
    pub value: Option<Value>,
    /// The externally-visible result.
    pub execution_result: ExecutionResult<Effect>,
    /// The new sessions that were just created by the callee for us.
    pub create_sessions: Vec<Session<SessionType>>,
}

impl<Effect, Value, Session> Default for ApplicationCallResult<Effect, Value, Session> {
    fn default() -> Self {
        Self {
            value: None,
            execution_result: Default::default(),
            create_sessions: vec![],
        }
    }
}

/// Syscall to request creating a new session.
#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Session<T> {
    /// A kind provided by the creator (meant to be visible to other applications).
    pub kind: u64,
    /// The data associated to the session.
    pub data: T,
}

/// The result of calling into a session.
#[derive(Default, Deserialize, Serialize)]
pub struct SessionCallResult<Effect, Value, Session> {
    /// The application result.
    pub inner: ApplicationCallResult<Effect, Value, Session>,
    /// If `call_session` was called, this tells the system to clean up the session.
    pub new_state: Option<Session>,
}
