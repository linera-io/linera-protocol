// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime independent code for interfacing with user applications in WebAssembly modules.

use super::{
    async_boundary::{ContextForwarder, GuestFuture, GuestFutureInterface},
    WasmExecutionError,
};
use crate::{
    system::Balance, ApplicationCallResult, CalleeContext, EffectContext, OperationContext,
    QueryContext, RawExecutionResult, SessionCallResult, SessionId,
};
use futures::future::{self, TryFutureExt};

/// Types that are specific to the context of an application ready to be executedy by a WebAssembly
/// runtime.
pub trait ApplicationRuntimeContext: Sized {
    /// The error emitted by the runtime when the application traps (panics).
    type Error: Into<WasmExecutionError> + Send + Unpin;

    /// How to store the application's in-memory state.
    type Store: Send + Unpin;

    /// How to clean up the system storage interface after the application has executed.
    type StorageGuard: Send + Unpin;
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
    /// [`execute_effect`][crate::Contract::execute_effect] method.
    type ExecuteEffect: GuestFutureInterface<Self, Output = RawExecutionResult<Vec<u8>>>
        + Send
        + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`call_application`][crate::Contract::call_application] method.
    type CallApplication: GuestFutureInterface<Self, Output = ApplicationCallResult> + Send + Unpin;

    /// The WIT type for the resource representing the guest future
    /// [`call_session`][crate::Contract::call_session] method.
    type CallSession: GuestFutureInterface<Self, Output = (SessionCallResult, Vec<u8>)>
        + Send
        + Unpin;

    /// The WIT type equivalent for the [`OperationContext`].
    type OperationContext: From<OperationContext>;

    /// The WIT type equivalent for the [`EffectContext`].
    type EffectContext: From<EffectContext>;

    /// The WIT type equivalent for the [`CalleeContext`].
    type CalleeContext: From<CalleeContext>;

    /// The WIT type equivalent for the [`Session`], used as a parameter for calling guest methods.
    ///
    /// This type is created from a tuple rather than a [`Session`] instance so that allocation of a
    /// [`Vec`] is avoided when passing the session as a parameter to the WASM guest module.
    type SessionParam<'param>: From<(u64, &'param [u8])>;

    /// The WIT type equivalent for the [`SessionId`].
    type SessionId: From<SessionId>;

    /// The WIT type eqivalent for [`Poll<Result<RawExecutionResult<Vec<u8>>, String>>`].
    type PollExecutionResult;

    /// The WIT type eqivalent for [`Poll<Result<ApplicationCallResult, String>>`].
    type PollCallApplication;

    /// The WIT type eqivalent for [`Poll<Result<SessionCallResult, String>>`].
    type PollCallSession;

    /// Create a new future for the user application to initialize itself on the owner chain.
    fn initialize_new(
        &self,
        store: &mut Self::Store,
        context: Self::OperationContext,
        argument: &[u8],
    ) -> Result<Self::Initialize, Self::Error>;

    /// Poll a user contract future that's initializing the application.
    fn initialize_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::Initialize,
    ) -> Result<Self::PollExecutionResult, Self::Error>;

    /// Create a new future for the user application to execute an operation.
    fn execute_operation_new(
        &self,
        store: &mut Self::Store,
        context: Self::OperationContext,
        operation: &[u8],
    ) -> Result<Self::ExecuteOperation, Self::Error>;

    /// Poll a user contract future that's executing an operation.
    fn execute_operation_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::ExecuteOperation,
    ) -> Result<Self::PollExecutionResult, Self::Error>;

    /// Create a new future for the user contract to execute an effect.
    fn execute_effect_new(
        &self,
        store: &mut Self::Store,
        context: Self::EffectContext,
        effect: &[u8],
    ) -> Result<Self::ExecuteEffect, Self::Error>;

    /// Poll a user contract future that's executing an effect.
    fn execute_effect_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::ExecuteEffect,
    ) -> Result<Self::PollExecutionResult, Self::Error>;

    /// Create a new future for the user contract to handle a call from another contract.
    fn call_application_new(
        &self,
        store: &mut Self::Store,
        context: Self::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[Self::SessionId],
    ) -> Result<Self::CallApplication, Self::Error>;

    /// Poll a user contract future that's handling a call from another contract.
    fn call_application_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::CallApplication,
    ) -> Result<Self::PollCallApplication, Self::Error>;

    /// Create a new future for the user contract to handle a session call from another
    /// contract.
    fn call_session_new(
        &self,
        store: &mut Self::Store,
        context: Self::CalleeContext,
        session: Self::SessionParam<'_>,
        argument: &[u8],
        forwarded_sessions: &[Self::SessionId],
    ) -> Result<Self::CallSession, Self::Error>;

    /// Poll a user contract future that's handling a session call from another contract.
    fn call_session_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::CallSession,
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

    /// Create a new future for the user application to handle a query.
    fn query_application_new(
        &self,
        store: &mut Self::Store,
        context: Self::QueryContext,
        argument: &[u8],
    ) -> Result<Self::QueryApplication, Self::Error>;

    /// Poll a user service future that's handling a query.
    fn query_application_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::QueryApplication,
    ) -> Result<Self::PollQuery, Self::Error>;
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
    ) -> GuestFuture<A::Initialize, A> {
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
    ) -> GuestFuture<A::ExecuteOperation, A> {
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
    ) -> GuestFuture<A::ExecuteEffect, A> {
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
    ) -> GuestFuture<A::CallApplication, A> {
        let forwarded_sessions: Vec<_> = forwarded_sessions
            .into_iter()
            .map(A::SessionId::from)
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
    ) -> future::MapOk<
        GuestFuture<A::CallSession, A>,
        impl FnOnce((SessionCallResult, Vec<u8>)) -> SessionCallResult + 'session_data,
    >
    where
        A: Unpin + 'session_data,
        A::Store: Unpin,
        A::StorageGuard: Unpin,
        A::Error: Unpin,
    {
        let forwarded_sessions: Vec<_> = forwarded_sessions
            .into_iter()
            .map(A::SessionId::from)
            .collect();

        let session = A::SessionParam::from((session_kind, &*session_data));

        let future = self.application.call_session_new(
            &mut self.store,
            (*context).into(),
            session,
            argument,
            &forwarded_sessions,
        );

        GuestFuture::new(future, self).map_ok(|(session_call_result, updated_data)| {
            *session_data = updated_data;
            session_call_result
        })
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
    ) -> GuestFuture<A::QueryApplication, A> {
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
