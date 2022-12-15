// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime independent code for interfacing with user applications in WebAssembly modules.

use super::{
    async_boundary::{ContextForwarder, GuestFuture, GuestFutureInterface},
    runtime::{
        contract::{
            self, CallApplication, CallSession, ExecuteEffect, ExecuteOperation, Initialize,
            PollCallApplication, PollCallSession, PollExecutionResult,
        },
        service::{self, PollQuery, QueryApplication},
    },
    WasmExecutionError,
};
use crate::{
    ApplicationCallResult, CalleeContext, EffectContext, OperationContext, QueryContext,
    RawExecutionResult, SessionCallResult, SessionId,
};
use std::{future::Future, task::Poll};

/// Types that are specific to a WebAssembly runtime.
pub trait Runtime {
    /// How to call the application interface.
    type Application;

    /// How to store the application's in-memory state.
    type Store;

    /// How to clean up the system storage interface after the application has executed.
    type StorageGuard;

    /// The error emitted by the runtime when the application traps (panics).
    type Error: Into<WasmExecutionError>;
}

/// Common interface to calling a user contract in a WebAssembly module.
pub trait Contract<R: Runtime> {
    /// Create a new future for the user application to initialize itself on the owner chain.
    fn initialize_new(
        &self,
        store: &mut R::Store,
        context: contract::OperationContext,
        argument: &[u8],
    ) -> Result<contract::Initialize, R::Error>;

    /// Poll a user contract future that's initializing the application.
    fn initialize_poll(
        &self,
        store: &mut R::Store,
        future: &contract::Initialize,
    ) -> Result<contract::PollExecutionResult, R::Error>;

    /// Create a new future for the user application to execute an operation.
    fn execute_operation_new(
        &self,
        store: &mut R::Store,
        context: contract::OperationContext,
        operation: &[u8],
    ) -> Result<contract::ExecuteOperation, R::Error>;

    /// Poll a user contract future that's executing an operation.
    fn execute_operation_poll(
        &self,
        store: &mut R::Store,
        future: &contract::ExecuteOperation,
    ) -> Result<contract::PollExecutionResult, R::Error>;

    /// Create a new future for the user contract to execute an effect.
    fn execute_effect_new(
        &self,
        store: &mut R::Store,
        context: contract::EffectContext,
        effect: &[u8],
    ) -> Result<contract::ExecuteEffect, R::Error>;

    /// Poll a user contract future that's executing an effect.
    fn execute_effect_poll(
        &self,
        store: &mut R::Store,
        future: &contract::ExecuteEffect,
    ) -> Result<contract::PollExecutionResult, R::Error>;

    /// Create a new future for the user contract to handle a call from another contract.
    fn call_application_new(
        &self,
        store: &mut R::Store,
        context: contract::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallApplication, R::Error>;

    /// Poll a user contract future that's handling a call from another contract.
    fn call_application_poll(
        &self,
        store: &mut R::Store,
        future: &contract::CallApplication,
    ) -> Result<contract::PollCallApplication, R::Error>;

    /// Create a new future for the user contract to handle a session call from another
    /// contract.
    fn call_session_new(
        &self,
        store: &mut R::Store,
        context: contract::CalleeContext,
        session: contract::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallSession, R::Error>;

    /// Poll a user contract future that's handling a session call from another contract.
    fn call_session_poll(
        &self,
        store: &mut R::Store,
        future: &contract::CallSession,
    ) -> Result<contract::PollCallSession, R::Error>;
}

pub trait Service<R: Runtime> {
    /// Create a new future for the user application to handle a query.
    fn query_application_new(
        &self,
        store: &mut R::Store,
        context: service::QueryContext,
        argument: &[u8],
    ) -> Result<service::QueryApplication, R::Error>;

    /// Poll a user service future that's handling a query.
    fn query_application_poll(
        &self,
        store: &mut R::Store,
        future: &service::QueryApplication,
    ) -> Result<service::PollQuery, R::Error>;
}

/// Wrapper around all types necessary to call an asynchronous method of a WASM application.
pub struct WasmRuntimeContext<R>
where
    R: Runtime,
{
    /// Where to store the async task context to later be reused in async calls from the guest WASM
    /// module.
    pub(crate) context_forwarder: ContextForwarder,

    /// The application type.
    pub(crate) application: R::Application,

    /// The application's memory state.
    pub(crate) store: R::Store,

    /// Guard type to clean up any host state after the call to the WASM application finishes.
    pub(crate) _storage_guard: R::StorageGuard,
}

impl<R> WasmRuntimeContext<R>
where
    R: Runtime,
    R::Application: Contract<R>,
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
    ) -> GuestFuture<Initialize, R> {
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
    ) -> GuestFuture<ExecuteOperation, R> {
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
    ) -> GuestFuture<ExecuteEffect, R> {
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
    ) -> GuestFuture<CallApplication, R> {
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
    pub fn call_session(
        mut self,
        context: &CalleeContext,
        session_kind: u64,
        session_data: &mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> impl Future<Output = Result<SessionCallResult, WasmExecutionError>>
    where
        R::Application: Unpin,
        R::Store: Unpin,
        R::StorageGuard: Unpin,
        R::Error: Unpin,
        WasmExecutionError: From<R::Error>,
    {
        let forwarded_sessions: Vec<_> = forwarded_sessions
            .into_iter()
            .map(contract::SessionId::from)
            .collect();

        let session = contract::SessionParam {
            kind: session_kind,
            data: &*session_data,
        };

        let future = self.application.call_session_new(
            &mut self.store,
            (*context).into(),
            session,
            argument,
            &forwarded_sessions,
        );

        GuestFuture::new(future, self)
    }
}

impl<R> WasmRuntimeContext<R>
where
    R: Runtime,
    R::Application: Service<R>,
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
    ) -> GuestFuture<QueryApplication, R> {
        let future =
            self.application
                .query_application_new(&mut self.store, (*context).into(), argument);

        GuestFuture::new(future, self)
    }
}

/// Implement [`GuestFutureInterface`] for a `future` type implemented by a guest WASM module.
///
/// The future is then polled by calling the guest `poll_func`. The return type of that function is
/// a `poll_type` that must be convertible into the `output` type wrapped in a
/// `Poll<Result<_, _>>`.
macro_rules! impl_guest_future_interface {
    ( $( $future:ident : $poll_func:ident -> $poll_type:ident -> $trait:ident => $output:ty ),* $(,)* ) => {
        $(
            impl<'storage, R> GuestFutureInterface<R> for $future
            where
                R: Runtime,
                R::Application: $trait<R>,
                WasmExecutionError: From<R::Error>,
            {
                type Output = $output;

                fn poll(
                    &self,
                    application: &R::Application,
                    store: &mut R::Store,
                ) -> Poll<Result<Self::Output, WasmExecutionError>> {
                    match application.$poll_func(store, self)? {
                        $poll_type::Ready(Ok(result)) => Poll::Ready(Ok(result.into())),
                        $poll_type::Ready(Err(message)) => {
                            Poll::Ready(Err(WasmExecutionError::UserApplication(message)))
                        }
                        $poll_type::Pending => Poll::Pending,
                    }
                }
            }
        )*
    }
}

impl_guest_future_interface! {
    Initialize: initialize_poll -> PollExecutionResult -> Contract => RawExecutionResult<Vec<u8>>,
    ExecuteOperation: execute_operation_poll -> PollExecutionResult -> Contract => RawExecutionResult<Vec<u8>>,
    ExecuteEffect: execute_effect_poll -> PollExecutionResult -> Contract => RawExecutionResult<Vec<u8>>,
    CallApplication: call_application_poll -> PollCallApplication -> Contract => ApplicationCallResult,
    CallSession: call_session_poll -> PollCallSession -> Contract => SessionCallResult,
    QueryApplication: query_application_poll -> PollQuery -> Service => Vec<u8>,
}
