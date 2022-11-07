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
    RawExecutionResult, ReadableStorage, SessionCallResult, SessionId, WritableStorage,
};
use async_trait::async_trait;
use linera_base::messages::{ApplicationId, ChainId};
use std::task::Poll;

pub trait Runtime: Sized {
    type Application: Application<Self>;
    type Store;
    type StorageGuard;
    type Error: Into<ExecutionError>;
}

pub trait Application<R: Runtime> {
    fn execute_operation_new(
        &self,
        store: &mut R::Store,
        context: application::OperationContext,
        operation: &[u8],
    ) -> Result<application::ExecuteOperation, R::Error>;

    fn execute_operation_poll(
        &self,
        store: &mut R::Store,
        future: &application::ExecuteOperation,
    ) -> Result<application::PollExecutionResult, R::Error>;

    fn execute_effect_new(
        &self,
        store: &mut R::Store,
        context: application::EffectContext,
        effect: &[u8],
    ) -> Result<application::ExecuteEffect, R::Error>;

    fn execute_effect_poll(
        &self,
        store: &mut R::Store,
        future: &application::ExecuteEffect,
    ) -> Result<application::PollExecutionResult, R::Error>;

    fn call_application_new(
        &self,
        store: &mut R::Store,
        context: application::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[application::SessionId],
    ) -> Result<application::CallApplication, R::Error>;

    fn call_application_poll(
        &self,
        store: &mut R::Store,
        future: &application::CallApplication,
    ) -> Result<application::PollCallApplication, R::Error>;

    fn call_session_new(
        &self,
        store: &mut R::Store,
        context: application::CalleeContext,
        session: application::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[application::SessionId],
    ) -> Result<application::CallSession, R::Error>;

    fn call_session_poll(
        &self,
        store: &mut R::Store,
        future: &application::CallSession,
    ) -> Result<application::PollCallSession, R::Error>;

    fn query_application_new(
        &self,
        store: &mut R::Store,
        context: application::QueryContext,
        argument: &[u8],
    ) -> Result<application::QueryApplication, R::Error>;

    fn query_application_poll(
        &self,
        store: &mut R::Store,
        future: &application::QueryApplication,
    ) -> Result<application::PollQuery, R::Error>;
}

pub struct WritableRuntimeContext<R>
where
    R: Runtime,
{
    pub(crate) context_forwarder: ContextForwarder,
    pub(crate) application: R::Application,
    pub(crate) store: R::Store,
    pub(crate) _storage_guard: R::StorageGuard,
}

impl<R> WritableRuntimeContext<R>
where
    R: Runtime,
{
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

pub struct WrappedQueryableStorage<'storage>(&'storage dyn QueryableStorage);

impl<'storage> WrappedQueryableStorage<'storage> {
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

macro_rules! impl_guest_future_interface {
    ( $( $future:ident : $poll_func:ident -> $poll_type:ident => $output:ty ),* $(,)* ) => {
        $(
            impl<'storage, R> GuestFutureInterface<R> for $future
            where
                R: Runtime,
                ExecutionError: From<R::Error>,
            {
                type Output = $output;

                fn poll(
                    &self,
                    application: &R::Application,
                    store: &mut R::Store,
                ) -> Poll<Result<Self::Output, ExecutionError>> {
                    match application.$poll_func(store, self)? {
                        $poll_type::Ready(Ok(result)) => Poll::Ready(Ok(result.into())),
                        $poll_type::Ready(Err(message)) => {
                            Poll::Ready(Err(ExecutionError::UserApplication(message)))
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
