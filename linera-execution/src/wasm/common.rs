// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime independent code for interfacing with user applications in WebAssembly modules.

use super::{
    async_boundary::{ContextForwarder, GuestFutureInterface},
    runtime::application::{
        self, CallApplication, CallSession, ExecuteEffect, ExecuteOperation, PollCallApplication,
        PollCallSession, PollExecutionResult, PollQuery, QueryApplication,
    },
};
use crate::{ApplicationCallResult, RawExecutionResult, SessionCallResult, WasmExecutionError};
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
