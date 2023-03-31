// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod base;
pub mod contract;
mod exported_future;
mod extensions;
mod log;
pub mod service;
#[cfg(feature = "test")]
pub mod test;

use async_trait::async_trait;
use custom_debug_derive::Debug;
use linera_base::{
    data_types::BlockHeight,
    identifiers::{ApplicationId, ChainId, ChannelName, Destination, EffectId, Owner, SessionId},
};
use serde::{Deserialize, Serialize};
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
    type Error: Error;
    /// The desired storage backend to use to store the application's state.
    type Storage;

    /// Initializes the application on the chain that created it.
    async fn initialize(
        &mut self,
        context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error>;

    /// Applies an operation from the current block.
    async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error>;

    /// Applies an effect originating from a cross-chain message.
    async fn execute_effect(
        &mut self,
        context: &EffectContext,
        effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error>;

    /// Handles a call from another application.
    async fn handle_application_call(
        &mut self,
        context: &CalleeContext,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error>;

    /// Handles a call into a session created by this application.
    async fn handle_session_call(
        &mut self,
        context: &CalleeContext,
        session: Session,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error>;
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
    pub index: u64,
}

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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryContext {
    /// The current chain id.
    pub chain_id: ChainId,
}

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ExecutionResult {
    /// Send messages to the given destinations, possibly forwarding the authenticated
    /// signer.
    pub effects: Vec<(Destination, bool, Vec<u8>)>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelName, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(ChannelName, ChainId)>,
}

impl ExecutionResult {
    /// Add an effect to the execution result.
    pub fn with_effect(
        mut self,
        destination: impl Into<Destination>,
        effect: &impl Serialize,
    ) -> Self {
        let effect_bytes = bcs::to_bytes(effect).expect("Effect should be serializable");
        self.effects.push((destination.into(), false, effect_bytes));
        self
    }

    /// Add an authenticated effect to the execution result.
    pub fn with_authenticated_effect(
        mut self,
        destination: impl Into<Destination>,
        effect: &impl Serialize,
    ) -> Self {
        let effect_bytes = bcs::to_bytes(effect).expect("Effect should be serializable");
        self.effects.push((destination.into(), true, effect_bytes));
        self
    }
}

/// The result of calling into a user application.
#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ApplicationCallResult {
    /// The return value.
    #[debug(with = "linera_base::hex_debug")]
    pub value: Vec<u8>,
    /// The externally-visible result.
    pub execution_result: ExecutionResult,
    /// The new sessions that were just created by the callee for us.
    pub create_sessions: Vec<Session>,
}

/// Syscall to request creating a new session.
#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Session {
    /// A kind provided by the creator (meant to be visible to other applications).
    pub kind: u64,
    /// The data associated to the session.
    #[debug(with = "linera_base::hex_debug")]
    pub data: Vec<u8>,
}

/// The result of calling into a session.
#[derive(Default, Deserialize, Serialize)]
pub struct SessionCallResult {
    /// The application result.
    pub inner: ApplicationCallResult,
    /// If `call_session` was called, this tells the system to clean up the session.
    pub data: Option<Vec<u8>>,
}
