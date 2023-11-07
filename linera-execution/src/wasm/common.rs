// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime independent code for interfacing with user applications in WebAssembly modules.

use super::{
    async_boundary::{GuestFutureActor, GuestFutureInterface, PollSender},
    async_determinism::HostFutureQueue,
    ExecutionError, WasmExecutionError,
};
use crate::{
    ApplicationCallResult, CalleeContext, MessageContext, OperationContext, QueryContext,
    RawExecutionResult, SessionCallResult, SessionId,
};
use futures::{
    future::{FutureExt, Map, MapErr, TryFutureExt},
    stream::StreamExt,
};
use std::{convert, thread};

/// Types that are specific to the context of an application ready to be executedy by a WebAssembly
/// runtime.
pub trait ApplicationRuntimeContext: Sized {
    /// The error emitted by the runtime when the application traps (panics).
    type Error: Into<ExecutionError> + Send + Unpin;

    /// How to store the application's in-memory state.
    type Store: Send + Unpin;

    /// Extra runtime-specific data.
    type Extra: Send + Unpin;

    /// Configures the fuel available for execution.
    fn configure_initial_fuel(context: &mut WasmRuntimeContext<Self>);

    /// Persists the remaining fuel after execution.
    fn persist_remaining_fuel(context: &mut WasmRuntimeContext<Self>) -> Result<(), ()>;
}

/// Common interface to calling a user contract in a WebAssembly module.
pub trait Contract: ApplicationRuntimeContext {
    /// The WIT type for the resource representing the guest future
    /// [`execute_operation`][crate::Contract::execute_operation] method.
    type ExecuteOperation: GuestFutureInterface<
            Self,
            Output = RawExecutionResult<Vec<u8>>,
            Parameters = (OperationContext, Vec<u8>),
        > + Send
        + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`execute_message`][crate::Contract::execute_message] method.
    type ExecuteMessage: GuestFutureInterface<
            Self,
            Output = RawExecutionResult<Vec<u8>>,
            Parameters = (MessageContext, Vec<u8>),
        > + Send
        + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`handle_application_call`][crate::Contract::handle_application_call] method.
    type HandleApplicationCall: GuestFutureInterface<
            Self,
            Output = ApplicationCallResult,
            Parameters = (CalleeContext, Vec<u8>, Vec<SessionId>),
        > + Send
        + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`handle_session_call`][crate::Contract::handle_session_call] method.
    type HandleSessionCall: GuestFutureInterface<
            Self,
            Output = (SessionCallResult, Vec<u8>),
            Parameters = (CalleeContext, Vec<u8>, Vec<u8>, Vec<SessionId>),
        > + Send
        + Unpin;

    /// The WIT type eqivalent for [`Poll<Result<RawExecutionResult<Vec<u8>>, String>>`].
    type PollExecutionResult;

    /// The WIT type eqivalent for [`Poll<Result<ApplicationCallResult, String>>`].
    type PollApplicationCallResult;

    /// The WIT type eqivalent for [`Poll<Result<SessionCallResult, String>>`].
    type PollSessionCallResult;

    /// Initializes the user application on the owner chain.
    fn initialize(
        &self,
        store: &mut Self::Store,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, Self::Error>;

    /// Creates a new future for the user application to execute an operation.
    fn execute_operation_new(
        &self,
        store: &mut Self::Store,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Self::ExecuteOperation, Self::Error>;

    /// Polls a user contract future that's executing an operation.
    fn execute_operation_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::ExecuteOperation,
    ) -> Result<Self::PollExecutionResult, Self::Error>;

    /// Creates a new future for the user contract to execute a message.
    fn execute_message_new(
        &self,
        store: &mut Self::Store,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<Self::ExecuteMessage, Self::Error>;

    /// Polls a user contract future that's executing a message.
    fn execute_message_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::ExecuteMessage,
    ) -> Result<Self::PollExecutionResult, Self::Error>;

    /// Creates a new future for the user contract to handle a call from another contract.
    fn handle_application_call_new(
        &self,
        store: &mut Self::Store,
        context: CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<Self::HandleApplicationCall, Self::Error>;

    /// Polls a user contract future that's handling a call from another contract.
    fn handle_application_call_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::HandleApplicationCall,
    ) -> Result<Self::PollApplicationCallResult, Self::Error>;

    /// Creates a new future for the user contract to handle a session call from another
    /// contract.
    fn handle_session_call_new(
        &self,
        store: &mut Self::Store,
        context: CalleeContext,
        session_state: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<Self::HandleSessionCall, Self::Error>;

    /// Polls a user contract future that's handling a session call from another contract.
    fn handle_session_call_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::HandleSessionCall,
    ) -> Result<Self::PollSessionCallResult, Self::Error>;
}

/// Common interface to calling a user service in a WebAssembly module.
pub trait Service: ApplicationRuntimeContext {
    /// The WIT type for the resource representing the guest future
    /// [`handle_query`][crate::Service::handle_query] method.
    type HandleQuery: GuestFutureInterface<Self, Output = Vec<u8>, Parameters = (QueryContext, Vec<u8>)>
        + Send
        + Unpin;

    /// The WIT type eqivalent for [`Poll<Result<Vec<u8>, String>>`].
    type PollApplicationQueryResult;

    /// Creates a new future for the user application to handle a query.
    fn handle_query_new(
        &self,
        store: &mut Self::Store,
        context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Self::HandleQuery, Self::Error>;

    /// Polls a user service future that's handling a query.
    fn handle_query_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::HandleQuery,
    ) -> Result<Self::PollApplicationQueryResult, Self::Error>;
}

/// Wrapper around all types necessary to call an asynchronous method of a Wasm application.
pub struct WasmRuntimeContext<A>
where
    A: ApplicationRuntimeContext,
{
    /// The application type.
    pub(crate) application: A,

    /// A queue of host futures called by the guest that must complete deterministically.
    pub(crate) future_queue: Option<HostFutureQueue>,

    /// The application's memory state.
    pub(crate) store: A::Store,

    /// Guard type to clean up any host state after the call to the Wasm application finishes.
    #[cfg_attr(all(not(feature = "wasmer"), feature = "wasmtime"), allow(dead_code))]
    pub(crate) extra: A::Extra,
}

type WasmResultFuture<T> = Map<
    MapErr<oneshot::Receiver<Result<T, ExecutionError>>, fn(oneshot::RecvError) -> ExecutionError>,
    fn(Result<Result<T, ExecutionError>, ExecutionError>) -> Result<T, ExecutionError>,
>;

impl<A> WasmRuntimeContext<A>
where
    A: Contract + Send + Unpin + 'static,
{
    /// Calls the guest Wasm module's implementation of
    /// [`UserApplication::initialize`][`linera_execution::UserApplication::initialize`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn initialize(
    ///     mut self,
    ///     context: &OperationContext,
    ///     argument: &[u8],
    /// ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>
    /// ```
    pub fn initialize(
        self,
        context: &OperationContext,
        argument: &[u8],
    ) -> WasmResultFuture<RawExecutionResult<Vec<u8>>> {
        let context = *context;
        let argument = argument.to_owned();

        self.run_wasm_guest(move |application, store| {
            application.initialize(store, context, argument)
        })
    }

    /// Calls the guest Wasm module's implementation of
    /// [`UserApplication::execute_operation`][`linera_execution::UserApplication::execute_operation`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn execute_operation(
    ///     mut self,
    ///     context: &OperationContext,
    ///     operation: &[u8],
    /// ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>
    /// ```
    pub fn execute_operation(
        self,
        context: &OperationContext,
        operation: &[u8],
    ) -> PollSender<RawExecutionResult<Vec<u8>>> {
        GuestFutureActor::<A::ExecuteOperation, A>::spawn((*context, operation.to_owned()), self)
    }

    /// Calls the guest Wasm module's implementation of
    /// [`UserApplication::execute_message`][`linera_execution::UserApplication::execute_message`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn execute_message(
    ///     mut self,
    ///     context: &MessageContext,
    ///     message: &[u8],
    /// ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError>
    /// ```
    pub fn execute_message(
        self,
        context: &MessageContext,
        message: &[u8],
    ) -> PollSender<RawExecutionResult<Vec<u8>>> {
        GuestFutureActor::<A::ExecuteMessage, A>::spawn((*context, message.to_owned()), self)
    }

    /// Calls the guest Wasm module's implementation of
    /// [`UserApplication::handle_application_call`][`linera_execution::UserApplication::handle_application_call`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn handle_application_call(
    ///     mut self,
    ///     context: &CalleeContext,
    ///     argument: &[u8],
    ///     forwarded_sessions: Vec<SessionId>,
    /// ) -> Result<ApplicationCallResult, ExecutionError>
    /// ```
    pub fn handle_application_call(
        self,
        context: &CalleeContext,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> PollSender<ApplicationCallResult> {
        GuestFutureActor::<A::HandleApplicationCall, A>::spawn(
            (*context, argument.to_owned(), forwarded_sessions),
            self,
        )
    }

    /// Calls the guest Wasm module's implementation of
    /// [`UserApplication::handle_session_call`][`linera_execution::UserApplication::handle_session_call`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn handle_session_call(
    ///     mut self,
    ///     context: &CalleeContext,
    ///     session_state: &[u8],
    ///     argument: &[u8],
    ///     forwarded_sessions: Vec<SessionId>,
    /// ) -> Result<(SessionCallResult, Vec<u8>), ExecutionError>
    /// ```
    pub fn handle_session_call(
        self,
        context: &CalleeContext,
        session_state: &[u8],
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> PollSender<(SessionCallResult, Vec<u8>)> {
        GuestFutureActor::<A::HandleSessionCall, A>::spawn(
            (
                *context,
                session_state.to_owned(),
                argument.to_owned(),
                forwarded_sessions,
            ),
            self,
        )
    }
}

impl<A> WasmRuntimeContext<A>
where
    A: Service + Send + Unpin + 'static,
{
    /// Calls the guest Wasm module's implementation of
    /// [`UserApplication::handle_query`][`linera_execution::UserApplication::handle_query`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn handle_query(
    ///     mut self,
    ///     context: &QueryContext,
    ///     argument: &[u8],
    /// ) -> Result<Vec<u8>, ExecutionError>
    /// ```
    pub fn handle_query(self, context: &QueryContext, argument: &[u8]) -> PollSender<Vec<u8>> {
        GuestFutureActor::<A::HandleQuery, A>::spawn((*context, argument.to_owned()), self)
    }
}

impl<A> WasmRuntimeContext<A>
where
    A: ApplicationRuntimeContext + Send + Unpin + 'static,
{
    /// Spawns a thread to execute a Wasm guest.
    fn run_wasm_guest<T>(
        mut self,
        guest_operation: impl FnOnce(&A, &mut A::Store) -> Result<Result<T, String>, A::Error>
            + Send
            + 'static,
    ) -> WasmResultFuture<T>
    where
        T: Send + 'static,
    {
        let (result_sender, result_receiver) = oneshot::channel();

        if let Some(future_queue) = self.future_queue.take() {
            tokio::spawn(future_queue.collect::<()>());
        }

        thread::spawn(move || {
            A::configure_initial_fuel(&mut self);

            let result = guest_operation(&self.application, &mut self.store)
                .map_err(|error| error.into())
                .and_then(|result| result.map_err(ExecutionError::UserError));

            if result_sender.send(result).is_err() {
                tracing::debug!("Wasm guest operation canceled");
            }
        });

        result_receiver
            .map_err(
                (|oneshot::RecvError| WasmExecutionError::Aborted.into())
                    as fn(oneshot::RecvError) -> ExecutionError,
            )
            .map(|result| result.and_then(convert::identity))
    }
}

impl<A> Drop for WasmRuntimeContext<A>
where
    A: ApplicationRuntimeContext,
{
    fn drop(&mut self) {
        if A::persist_remaining_fuel(self).is_err() {
            tracing::warn!(
                "Failed to persist remaining fuel. This is okay if the transaction was canceled"
            );
        }
    }
}
