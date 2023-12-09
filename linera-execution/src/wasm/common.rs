// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime independent code for interfacing with user applications in WebAssembly modules.

use super::ExecutionError;
use crate::{
    ApplicationCallResult, CalleeContext, MessageContext, OperationContext, QueryContext,
    RawExecutionResult, SessionCallResult, SessionId,
};

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
    fn configure_initial_fuel(context: &mut WasmRuntimeContext<Self>) -> Result<(), Self::Error>;

    /// Persists the remaining fuel after execution.
    fn persist_remaining_fuel(context: &mut WasmRuntimeContext<Self>) -> Result<(), Self::Error>;
}

/// Common interface to calling a user contract in a WebAssembly module.
pub trait Contract: ApplicationRuntimeContext {
    /// Initializes the user application on the owner chain.
    fn initialize(
        &self,
        store: &mut Self::Store,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, Self::Error>;

    /// Executes an application's operation.
    fn execute_operation(
        &self,
        store: &mut Self::Store,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, Self::Error>;

    /// Executes a cross-chain message.
    fn execute_message(
        &self,
        store: &mut Self::Store,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, Self::Error>;

    /// Handles a call from another contract.
    fn handle_application_call(
        &self,
        store: &mut Self::Store,
        context: CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<Result<ApplicationCallResult, String>, Self::Error>;

    /// Handles a session call from another contract.
    #[allow(clippy::type_complexity)]
    fn handle_session_call(
        &self,
        store: &mut Self::Store,
        context: CalleeContext,
        session_state: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<Result<(SessionCallResult, Vec<u8>), String>, Self::Error>;
}

/// Common interface to calling a user service in a WebAssembly module.
pub trait Service: ApplicationRuntimeContext {
    /// Handles a query to the user application.
    fn handle_query(
        &self,
        store: &mut Self::Store,
        context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Result<Vec<u8>, String>, Self::Error>;
}

/// Wrapper around all types necessary to call an asynchronous method of a Wasm application.
pub struct WasmRuntimeContext<A>
where
    A: ApplicationRuntimeContext,
{
    /// The application type.
    pub(crate) application: A,

    /// The application's memory state.
    pub(crate) store: A::Store,

    /// Guard type to clean up any host state after the call to the Wasm application finishes.
    #[cfg_attr(all(not(feature = "wasmer"), feature = "wasmtime"), allow(dead_code))]
    pub(crate) extra: A::Extra,
}

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
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
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
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.run_wasm_guest(move |application, store| {
            application.execute_operation(store, context, operation)
        })
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
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.run_wasm_guest(move |application, store| {
            application.execute_message(store, context, message)
        })
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
        context: CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        self.run_wasm_guest(move |application, store| {
            application.handle_application_call(store, context, argument, forwarded_sessions)
        })
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
        context: CalleeContext,
        session_state: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<(SessionCallResult, Vec<u8>), ExecutionError> {
        self.run_wasm_guest(move |application, store| {
            application.handle_session_call(
                store,
                context,
                session_state,
                argument,
                forwarded_sessions,
            )
        })
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
    pub fn handle_query(
        self,
        context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        self.run_wasm_guest(move |application, store| {
            application.handle_query(store, context, argument)
        })
    }
}

impl<A> WasmRuntimeContext<A>
where
    A: ApplicationRuntimeContext + Send + Unpin + 'static,
{
    fn run_wasm_guest<T>(
        mut self,
        guest_operation: impl FnOnce(&A, &mut A::Store) -> Result<Result<T, String>, A::Error>
            + Send
            + 'static,
    ) -> Result<T, ExecutionError>
    where
        T: Send + 'static,
    {
        A::configure_initial_fuel(&mut self).map_err(|error| error.into())?;

        let result = guest_operation(&self.application, &mut self.store)
            .map_err(|error| error.into())
            .and_then(|result| result.map_err(ExecutionError::UserError));

        // TODO(#989): Eventually, we should exit early again in case of UserError.
        A::persist_remaining_fuel(&mut self).map_err(|error| error.into())?;

        result
    }
}
