// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime independent code for interfacing with user applications in WebAssembly modules.

use super::{
    async_boundary::{ContextForwarder, GuestFuture},
    runtime::{
        contract::{self, CallApplication, ExecuteEffect, ExecuteOperation, Initialize},
        service::{self, QueryApplication},
    },
    WasmExecutionError,
};
use crate::{
    system::Balance, CalleeContext, EffectContext, OperationContext, QueryContext,
    SessionCallResult, SessionId,
};
use std::future::Future;

/// Types that are specific to the context of an application ready to be executedy by a WebAssembly
/// runtime.
pub trait ApplicationRuntimeContext {
    /// The error emitted by the runtime when the application traps (panics).
    type Error: Into<WasmExecutionError>;

    /// How to store the application's in-memory state.
    type Store;

    /// How to clean up the system storage interface after the application has executed.
    type StorageGuard;
}

/// Common interface to calling a user contract in a WebAssembly module.
pub trait Contract: ApplicationRuntimeContext {
    /// Create a new future for the user application to initialize itself on the owner chain.
    fn initialize_new(
        &self,
        store: &mut Self::Store,
        context: contract::OperationContext,
        argument: &[u8],
    ) -> Result<contract::Initialize, Self::Error>;

    /// Poll a user contract future that's initializing the application.
    fn initialize_poll(
        &self,
        store: &mut Self::Store,
        future: &contract::Initialize,
    ) -> Result<contract::PollExecutionResult, Self::Error>;

    /// Create a new future for the user application to execute an operation.
    fn execute_operation_new(
        &self,
        store: &mut Self::Store,
        context: contract::OperationContext,
        operation: &[u8],
    ) -> Result<contract::ExecuteOperation, Self::Error>;

    /// Poll a user contract future that's executing an operation.
    fn execute_operation_poll(
        &self,
        store: &mut Self::Store,
        future: &contract::ExecuteOperation,
    ) -> Result<contract::PollExecutionResult, Self::Error>;

    /// Create a new future for the user contract to execute an effect.
    fn execute_effect_new(
        &self,
        store: &mut Self::Store,
        context: contract::EffectContext,
        effect: &[u8],
    ) -> Result<contract::ExecuteEffect, Self::Error>;

    /// Poll a user contract future that's executing an effect.
    fn execute_effect_poll(
        &self,
        store: &mut Self::Store,
        future: &contract::ExecuteEffect,
    ) -> Result<contract::PollExecutionResult, Self::Error>;

    /// Create a new future for the user contract to handle a call from another contract.
    fn call_application_new(
        &self,
        store: &mut Self::Store,
        context: contract::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallApplication, Self::Error>;

    /// Poll a user contract future that's handling a call from another contract.
    fn call_application_poll(
        &self,
        store: &mut Self::Store,
        future: &contract::CallApplication,
    ) -> Result<contract::PollCallApplication, Self::Error>;

    /// Create a new future for the user contract to handle a session call from another
    /// contract.
    fn call_session_new(
        &self,
        store: &mut Self::Store,
        context: contract::CalleeContext,
        session: contract::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallSession, Self::Error>;

    /// Poll a user contract future that's handling a session call from another contract.
    fn call_session_poll(
        &self,
        store: &mut Self::Store,
        future: &contract::CallSession,
    ) -> Result<contract::PollCallSession, Self::Error>;
}

pub trait Service: ApplicationRuntimeContext {
    /// Create a new future for the user application to handle a query.
    fn query_application_new(
        &self,
        store: &mut Self::Store,
        context: service::QueryContext,
        argument: &[u8],
    ) -> Result<service::QueryApplication, Self::Error>;

    /// Poll a user service future that's handling a query.
    fn query_application_poll(
        &self,
        store: &mut Self::Store,
        future: &service::QueryApplication,
    ) -> Result<service::PollQuery, Self::Error>;
}

/// Wrapper around all types necessary to call an asynchronous method of a WASM application.
pub struct WasmRuntimeContext<A>
where
    A: ApplicationRuntimeContext,
{
    /// Where to store the async task context to later be reused in async calls from the guest WASM
    /// module.
    pub(crate) context_forwarder: ContextForwarder,

    /// The application type.
    pub(crate) application: A,

    /// The application's memory state.
    pub(crate) store: A::Store,

    /// Guard type to clean up any host state after the call to the WASM application finishes.
    pub(crate) _storage_guard: A::StorageGuard,
}

impl<A> WasmRuntimeContext<A>
where
    A: Contract,
{
    /// Call the guest WASM module's implementation of
    /// [`UserApplication::initialize`][`linera_execution::UserApplication::initialize`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn initialize(
    ///     mut self,
    ///     context: &OperationContext,
    ///     argument: &[u8],
    /// ) -> Result<RawExecutionResult<Vec<u8>>, WasmExecutionError>
    /// ```
    pub fn initialize(
        mut self,
        context: &OperationContext,
        argument: &[u8],
    ) -> GuestFuture<Initialize, A> {
        let future = self
            .application
            .initialize_new(&mut self.store, (*context).into(), argument);

        GuestFuture::new(future, self)
    }

    /// Call the guest WASM module's implementation of
    /// [`UserApplication::execute_operation`][`linera_execution::UserApplication::execute_operation`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn execute_operation(
    ///     mut self,
    ///     context: &OperationContext,
    ///     operation: &[u8],
    /// ) -> Result<RawExecutionResult<Vec<u8>>, WasmExecutionError>
    /// ```
    pub fn execute_operation(
        mut self,
        context: &OperationContext,
        operation: &[u8],
    ) -> GuestFuture<ExecuteOperation, A> {
        let future =
            self.application
                .execute_operation_new(&mut self.store, (*context).into(), operation);

        GuestFuture::new(future, self)
    }

    /// Call the guest WASM module's implementation of
    /// [`UserApplication::execute_effect`][`linera_execution::UserApplication::execute_effect`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn execute_effect(
    ///     mut self,
    ///     context: &EffectContext,
    ///     effect: &[u8],
    /// ) -> Result<RawExecutionResult<Vec<u8>>, WasmExecutionError>
    /// ```
    pub fn execute_effect(
        mut self,
        context: &EffectContext,
        effect: &[u8],
    ) -> GuestFuture<ExecuteEffect, A> {
        let future =
            self.application
                .execute_effect_new(&mut self.store, (*context).into(), effect);

        GuestFuture::new(future, self)
    }

    /// Call the guest WASM module's implementation of
    /// [`UserApplication::call_application`][`linera_execution::UserApplication::call_application`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn call_application(
    ///     mut self,
    ///     context: &CalleeContext,
    ///     argument: &[u8],
    ///     forwarded_sessions: Vec<SessionId>,
    /// ) -> Result<ApplicationCallResult, WasmExecutionError>
    /// ```
    pub fn call_application(
        mut self,
        context: &CalleeContext,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> GuestFuture<CallApplication, A> {
        let forwarded_sessions: Vec<_> = forwarded_sessions
            .into_iter()
            .map(contract::SessionId::from)
            .collect();

        let future = self.application.call_application_new(
            &mut self.store,
            (*context).into(),
            argument,
            &forwarded_sessions,
        );

        GuestFuture::new(future, self)
    }

    /// Call the guest WASM module's implementation of
    /// [`UserApplication::call_session`][`linera_execution::UserApplication::call_session`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn call_session(
    ///     mut self,
    ///     context: &CalleeContext,
    ///     session_kind: u64,
    ///     session_data: &mut Vec<u8>,
    ///     argument: &[u8],
    ///     forwarded_sessions: Vec<SessionId>,
    /// ) -> Result<SessionCallResult, WasmExecutionError>
    /// ```
    pub fn call_session<'session_data>(
        mut self,
        context: &CalleeContext,
        session_kind: u64,
        session_data: &'session_data mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> impl Future<Output = Result<SessionCallResult, WasmExecutionError>> + 'session_data
    where
        A: Unpin + 'session_data,
        A::Store: Unpin,
        A::StorageGuard: Unpin,
        A::Error: Unpin,
        WasmExecutionError: From<A::Error>,
    {
        let forwarded_sessions: Vec<_> = forwarded_sessions
            .into_iter()
            .map(contract::SessionId::from)
            .collect();

        let session = contract::SessionParam::from((session_kind, &*session_data));

        let future = self.application.call_session_new(
            &mut self.store,
            (*context).into(),
            session,
            argument,
            &forwarded_sessions,
        );

        async move {
            let (session_call_result, updated_data) = GuestFuture::new(future, self).await?;

            *session_data = updated_data;

            Ok(session_call_result)
        }
    }
}

impl<A> WasmRuntimeContext<A>
where
    A: Service,
{
    /// Call the guest WASM module's implementation of
    /// [`UserApplication::query_application`][`linera_execution::UserApplication::query_application`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn query_application(
    ///     mut self,
    ///     context: &QueryContext,
    ///     argument: &[u8],
    /// ) -> Result<Vec<u8>, WasmExecutionError>
    /// ```
    pub fn query_application(
        mut self,
        context: &QueryContext,
        argument: &[u8],
    ) -> GuestFuture<QueryApplication, A> {
        let future =
            self.application
                .query_application_new(&mut self.store, (*context).into(), argument);

        GuestFuture::new(future, self)
    }
}

impl Balance {
    /// Helper function to obtain the 64 most significant bits of the balance.
    pub(super) fn upper_half(self) -> u64 {
        (u128::from(self) >> 64)
            .try_into()
            .expect("Insufficient shift right for u128 -> u64 conversion")
    }

    /// Helper function to obtain the 64 least significant bits of the balance.
    pub(super) fn lower_half(self) -> u64 {
        (u128::from(self) & 0xFFFF_FFFF_FFFF_FFFF)
            .try_into()
            .expect("Incorrect mask for u128 -> u64 conversion")
    }
}
