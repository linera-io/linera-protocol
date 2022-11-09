// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime independent code for interfacing with user applications in WebAssembly modules.

use super::{
    async_boundary::{ContextForwarder, GuestFuture, GuestFutureInterface},
    runtime::application::{
        self, CallApplication, CallSession, ExecuteEffect, ExecuteOperation, PollCallApplication,
        PollCallSession, PollExecutionResult, PollQuery, QueryApplication,
    },
};
use crate::{
    system::Balance, ApplicationCallResult, ApplicationStateNotLocked, CallResult, CalleeContext,
    EffectContext, ExecutionError, OperationContext, QueryContext, QueryableStorage,
    RawExecutionResult, ReadableStorage, SessionCallResult, SessionId, WasmExecutionError,
    WritableStorage,
};
use async_trait::async_trait;
use linera_base::messages::{ApplicationId, ChainId};
use std::task::Poll;

/// Types that are specific to a WebAssembly runtime.
pub trait Runtime: Sized {
    /// How to call the application interface.
    type Application: Application<Self>;

    /// How to store the application's in-memory state.
    type Store;

    /// How to clean up the system storage interface after the application has executed.
    type StorageGuard;

    /// The error emitted by the runtime when the application traps (panics).
    type Error: Into<WasmExecutionError>;
}

/// Common interface to calling a user application in a WebAssembly module.
pub trait Application<R: Runtime> {
    /// Create a new future for the user application to execute an operation.
    fn execute_operation_new(
        &self,
        store: &mut R::Store,
        context: application::OperationContext,
        operation: &[u8],
    ) -> Result<application::ExecuteOperation, R::Error>;

    /// Poll a user application future that's executing an operation.
    fn execute_operation_poll(
        &self,
        store: &mut R::Store,
        future: &application::ExecuteOperation,
    ) -> Result<application::PollExecutionResult, R::Error>;

    /// Create a new future for the user application to execute an effect.
    fn execute_effect_new(
        &self,
        store: &mut R::Store,
        context: application::EffectContext,
        effect: &[u8],
    ) -> Result<application::ExecuteEffect, R::Error>;

    /// Poll a user application future that's executing an effect.
    fn execute_effect_poll(
        &self,
        store: &mut R::Store,
        future: &application::ExecuteEffect,
    ) -> Result<application::PollExecutionResult, R::Error>;

    /// Create a new future for the user application to handle a call from another application.
    fn call_application_new(
        &self,
        store: &mut R::Store,
        context: application::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[application::SessionId],
    ) -> Result<application::CallApplication, R::Error>;

    /// Poll a user application future that's handling a call from another application.
    fn call_application_poll(
        &self,
        store: &mut R::Store,
        future: &application::CallApplication,
    ) -> Result<application::PollCallApplication, R::Error>;

    /// Create a new future for the user application to handle a session call from another
    /// application.
    fn call_session_new(
        &self,
        store: &mut R::Store,
        context: application::CalleeContext,
        session: application::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[application::SessionId],
    ) -> Result<application::CallSession, R::Error>;

    /// Poll a user application future that's handling a session call from another application.
    fn call_session_poll(
        &self,
        store: &mut R::Store,
        future: &application::CallSession,
    ) -> Result<application::PollCallSession, R::Error>;

    /// Create a new future for the user application to handle a query.
    fn query_application_new(
        &self,
        store: &mut R::Store,
        context: application::QueryContext,
        argument: &[u8],
    ) -> Result<application::QueryApplication, R::Error>;

    /// Poll a user application future that's handling a query.
    fn query_application_poll(
        &self,
        store: &mut R::Store,
        future: &application::QueryApplication,
    ) -> Result<application::PollQuery, R::Error>;
}

/// Wrapper around all types necessary to call an asynchronous method of a WASM application.
pub struct WritableRuntimeContext<R>
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

impl<R> WritableRuntimeContext<R>
where
    R: Runtime,
{
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
            .map(application::SessionId::from)
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
    ) -> GuestFuture<CallSession, R> {
        let forwarded_sessions: Vec<_> = forwarded_sessions
            .into_iter()
            .map(application::SessionId::from)
            .collect();

        let session = application::SessionParam {
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

/// Wrap a [`QueryableStorage`] trait object so that it implements [`WritableStorage`] with stub
/// methods.
///
/// All implemented methods will either do nothing or return an error.
pub struct WrappedQueryableStorage<'storage>(&'storage dyn QueryableStorage);

impl<'storage> WrappedQueryableStorage<'storage> {
    /// Wrap a [`QueryableStorage`] trait object in a [`WrappedQueryableStorage`].
    pub fn new(storage: &'storage dyn QueryableStorage) -> Self {
        WrappedQueryableStorage(storage)
    }
}

#[async_trait]
impl ReadableStorage for WrappedQueryableStorage<'_> {
    fn chain_id(&self) -> ChainId {
        self.0.chain_id()
    }

    fn application_id(&self) -> ApplicationId {
        self.0.application_id()
    }

    fn read_system_balance(&self) -> Balance {
        self.0.read_system_balance()
    }

    async fn try_read_my_state(&self) -> Result<Vec<u8>, ExecutionError> {
        self.0.try_read_my_state().await
    }
}

#[async_trait]
impl WritableStorage for WrappedQueryableStorage<'_> {
    async fn try_read_and_lock_my_state(&self) -> Result<Vec<u8>, ExecutionError> {
        Err(ExecutionError::LockStateFromQuery)
    }

    fn save_and_unlock_my_state(&self, _state: Vec<u8>) -> Result<(), ApplicationStateNotLocked> {
        Err(ApplicationStateNotLocked)
    }

    fn unlock_my_state(&self) {}

    async fn try_call_application(
        &self,
        _authenticated: bool,
        _callee_id: ApplicationId,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        Err(ExecutionError::CallApplicationFromQuery)
    }

    async fn try_call_session(
        &self,
        _authenticated: bool,
        _session_id: SessionId,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        Err(ExecutionError::InvalidSession)
    }
}

/// Implement [`GuestFutureInterface`] for a `future` type implemented by a guest WASM module.
///
/// The future is then polled by calling the guest `poll_func`. The return type of that function is
/// a `poll_type` that must be convertible into the `output` type wrapped in a
/// `Poll<Result<_, _>>`.
macro_rules! impl_guest_future_interface {
    ( $( $future:ident : $poll_func:ident -> $poll_type:ident => $output:ty ),* $(,)* ) => {
        $(
            impl<'storage, R> GuestFutureInterface<R> for $future
            where
                R: Runtime,
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
    ExecuteOperation: execute_operation_poll -> PollExecutionResult => RawExecutionResult<Vec<u8>>,
    ExecuteEffect: execute_effect_poll -> PollExecutionResult => RawExecutionResult<Vec<u8>>,
    CallApplication: call_application_poll -> PollCallApplication => ApplicationCallResult,
    CallSession: call_session_poll -> PollCallSession => SessionCallResult,
    QueryApplication: query_application_poll -> PollQuery => Vec<u8>,
}
