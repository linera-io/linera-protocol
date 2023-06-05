// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime independent code for interfacing with user applications in WebAssembly modules.

use super::{
    async_boundary::{GuestFuture, GuestFutureInterface, WakerForwarder},
    async_determinism::HostFutureQueue,
    ExecutionError,
};
use crate::{
    ApplicationCallResult, CalleeContext, MessageContext, OperationContext, QueryContext,
    RawExecutionResult, SessionCallResult, SessionId,
};
use futures::future::{self, TryFutureExt};

/// Types that are specific to the context of an application ready to be executedy by a WebAssembly
/// runtime.
pub trait ApplicationRuntimeContext: Sized {
    /// The error emitted by the runtime when the application traps (panics).
    type Error: Into<ExecutionError> + Send + Unpin;

    /// How to store the application's in-memory state.
    type Store: Send + Unpin;

    /// Extra runtime-specific data.
    type Extra: Send + Unpin;

    /// Finalizes the runtime context, running any extra clean-up operations.
    fn finalize(_context: &mut WasmRuntimeContext<Self>) {}
}

/// Common interface to calling a user contract in a WebAssembly module.
pub trait Contract: ApplicationRuntimeContext {
    /// The WIT type for the resource representing the guest future
    /// [`initialize`][crate::Contract::initialize] method.
    type Initialize: GuestFutureInterface<Self, Output = RawExecutionResult<Vec<u8>>> + Send + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`execute_operation`][crate::Contract::execute_operation] method.
    type ExecuteOperation: GuestFutureInterface<Self, Output = RawExecutionResult<Vec<u8>>>
        + Send
        + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`execute_message`][crate::Contract::execute_message] method.
    type ExecuteMessage: GuestFutureInterface<Self, Output = RawExecutionResult<Vec<u8>>>
        + Send
        + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`handle_application_call`][crate::Contract::handle_application_call] method.
    type HandleApplicationCall: GuestFutureInterface<Self, Output = ApplicationCallResult>
        + Send
        + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`handle_session_call`][crate::Contract::handle_session_call] method.
    type HandleSessionCall: GuestFutureInterface<Self, Output = (SessionCallResult, Vec<u8>)>
        + Send
        + Unpin;

    /// The WIT type equivalent for the [`OperationContext`].
    type OperationContext: From<OperationContext>;

    /// The WIT type equivalent for the [`MessageContext`].
    type MessageContext: From<MessageContext>;

    /// The WIT type equivalent for the [`CalleeContext`].
    type CalleeContext: From<CalleeContext>;

    /// The WIT type equivalent for the [`SessionId`].
    type SessionId: From<SessionId>;

    /// The WIT type eqivalent for [`Poll<Result<RawExecutionResult<Vec<u8>>, String>>`].
    type PollExecutionResult;

    /// The WIT type eqivalent for [`Poll<Result<ApplicationCallResult, String>>`].
    type PollCallApplication;

    /// The WIT type eqivalent for [`Poll<Result<SessionCallResult, String>>`].
    type PollCallSession;

    /// Creates a new future for the user application to initialize itself on the owner chain.
    fn initialize_new(
        &self,
        store: &mut Self::Store,
        context: Self::OperationContext,
        argument: &[u8],
    ) -> Result<Self::Initialize, Self::Error>;

    /// Polls a user contract future that's initializing the application.
    fn initialize_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::Initialize,
    ) -> Result<Self::PollExecutionResult, Self::Error>;

    /// Creates a new future for the user application to execute an operation.
    fn execute_operation_new(
        &self,
        store: &mut Self::Store,
        context: Self::OperationContext,
        operation: &[u8],
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
        context: Self::MessageContext,
        message: &[u8],
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
        context: Self::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[Self::SessionId],
    ) -> Result<Self::HandleApplicationCall, Self::Error>;

    /// Polls a user contract future that's handling a call from another contract.
    fn handle_application_call_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::HandleApplicationCall,
    ) -> Result<Self::PollCallApplication, Self::Error>;

    /// Creates a new future for the user contract to handle a session call from another
    /// contract.
    fn handle_session_call_new(
        &self,
        store: &mut Self::Store,
        context: Self::CalleeContext,
        session_state: &[u8],
        argument: &[u8],
        forwarded_sessions: &[Self::SessionId],
    ) -> Result<Self::HandleSessionCall, Self::Error>;

    /// Polls a user contract future that's handling a session call from another contract.
    fn handle_session_call_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::HandleSessionCall,
    ) -> Result<Self::PollCallSession, Self::Error>;
}

/// Common interface to calling a user service in a WebAssembly module.
pub trait Service: ApplicationRuntimeContext {
    /// The WIT type for the resource representing the guest future
    /// [`query_application`][crate::Service::query_application] method.
    type QueryApplication: GuestFutureInterface<Self, Output = Vec<u8>> + Send + Unpin;

    /// The WIT type equivalent for the [`QueryContext`].
    type QueryContext: From<QueryContext>;

    /// The WIT type eqivalent for [`Poll<Result<Vec<u8>, String>>`].
    type PollQuery;

    /// Creates a new future for the user application to handle a query.
    fn query_application_new(
        &self,
        store: &mut Self::Store,
        context: Self::QueryContext,
        argument: &[u8],
    ) -> Result<Self::QueryApplication, Self::Error>;

    /// Polls a user service future that's handling a query.
    fn query_application_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::QueryApplication,
    ) -> Result<Self::PollQuery, Self::Error>;
}

/// Wrapper around all types necessary to call an asynchronous method of a WASM application.
pub struct WasmRuntimeContext<'context, A>
where
    A: ApplicationRuntimeContext,
{
    /// Where to store the async task context to later be reused in async calls from the guest WASM
    /// module.
    pub(crate) waker_forwarder: WakerForwarder,

    /// The application type.
    pub(crate) application: A,

    /// A queue of host futures called by the guest that must complete deterministically.
    pub(crate) future_queue: HostFutureQueue<'context>,

    /// The application's memory state.
    pub(crate) store: A::Store,

    /// Guard type to clean up any host state after the call to the WASM application finishes.
    #[allow(dead_code)]
    pub(crate) extra: A::Extra,
}

impl<'context, A> WasmRuntimeContext<'context, A>
where
    A: Contract,
{
    /// Calls the guest WASM module's implementation of
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
        mut self,
        context: &OperationContext,
        argument: &[u8],
    ) -> GuestFuture<'context, A::Initialize, A> {
        let future = self
            .application
            .initialize_new(&mut self.store, (*context).into(), argument);

        GuestFuture::new(future, self)
    }

    /// Calls the guest WASM module's implementation of
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
        mut self,
        context: &OperationContext,
        operation: &[u8],
    ) -> GuestFuture<'context, A::ExecuteOperation, A> {
        let future =
            self.application
                .execute_operation_new(&mut self.store, (*context).into(), operation);

        GuestFuture::new(future, self)
    }

    /// Calls the guest WASM module's implementation of
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
        mut self,
        context: &MessageContext,
        message: &[u8],
    ) -> GuestFuture<'context, A::ExecuteMessage, A> {
        let future =
            self.application
                .execute_message_new(&mut self.store, (*context).into(), message);

        GuestFuture::new(future, self)
    }

    /// Calls the guest WASM module's implementation of
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
        mut self,
        context: &CalleeContext,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> GuestFuture<'context, A::HandleApplicationCall, A> {
        let forwarded_sessions: Vec<_> = forwarded_sessions
            .into_iter()
            .map(A::SessionId::from)
            .collect();

        let future = self.application.handle_application_call_new(
            &mut self.store,
            (*context).into(),
            argument,
            &forwarded_sessions,
        );

        GuestFuture::new(future, self)
    }

    /// Calls the guest WASM module's implementation of
    /// [`UserApplication::handle_session_call`][`linera_execution::UserApplication::handle_session_call`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn handle_session_call(
    ///     mut self,
    ///     context: &CalleeContext,
    ///     session_state: &mut Vec<u8>,
    ///     argument: &[u8],
    ///     forwarded_sessions: Vec<SessionId>,
    /// ) -> Result<SessionCallResult, ExecutionError>
    /// ```
    pub fn handle_session_call<'session_state>(
        mut self,
        context: &CalleeContext,
        session_state: &'session_state mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> future::MapOk<
        GuestFuture<'context, A::HandleSessionCall, A>,
        impl FnOnce((SessionCallResult, Vec<u8>)) -> SessionCallResult + 'session_state,
    >
    where
        A: Unpin + 'session_state,
        A::Store: Unpin,
        A::Error: Unpin,
        A::Extra: Unpin,
    {
        let forwarded_sessions: Vec<_> = forwarded_sessions
            .into_iter()
            .map(A::SessionId::from)
            .collect();

        let future = self.application.handle_session_call_new(
            &mut self.store,
            (*context).into(),
            &*session_state,
            argument,
            &forwarded_sessions,
        );

        GuestFuture::new(future, self).map_ok(|(session_call_result, updated_data)| {
            *session_state = updated_data;
            session_call_result
        })
    }
}

impl<'context, A> WasmRuntimeContext<'context, A>
where
    A: Service,
{
    /// Calls the guest WASM module's implementation of
    /// [`UserApplication::query_application`][`linera_execution::UserApplication::query_application`].
    ///
    /// This method returns a [`Future`][`std::future::Future`], and is equivalent to
    ///
    /// ```ignore
    /// pub async fn query_application(
    ///     mut self,
    ///     context: &QueryContext,
    ///     argument: &[u8],
    /// ) -> Result<Vec<u8>, ExecutionError>
    /// ```
    pub fn query_application(
        mut self,
        context: &QueryContext,
        argument: &[u8],
    ) -> GuestFuture<'context, A::QueryApplication, A> {
        let future =
            self.application
                .query_application_new(&mut self.store, (*context).into(), argument);

        GuestFuture::new(future, self)
    }
}

impl<A> Drop for WasmRuntimeContext<'_, A>
where
    A: ApplicationRuntimeContext,
{
    fn drop(&mut self) {
        A::finalize(self);
    }
}
