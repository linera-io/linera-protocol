#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

mod async_boundary;
#[cfg(feature = "wasmer")]
#[path = "wasmer.rs"]
mod runtime;
#[cfg(feature = "wasmtime")]
#[path = "wasmtime.rs"]
mod runtime;

use self::{
    async_boundary::{ContextForwarder, GuestFuture, GuestFutureInterface},
    runtime::application::{
        self, CallApplication, CallSession, ExecuteEffect, ExecuteOperation, PollCallApplication,
        PollCallSession, PollExecutionResult, PollQuery, QueryApplication,
    },
};
use crate::{
    system::Balance, ApplicationCallResult, ApplicationStateNotLocked, CallResult, CalleeContext,
    EffectContext, EffectId, ExecutionError, NewSession, OperationContext, QueryContext,
    QueryableStorage, RawExecutionResult, ReadableStorage, SessionCallResult, SessionId,
    UserApplication, WritableStorage,
};
use async_trait::async_trait;
use linera_base::{
    crypto::HashValue,
    messages::{ApplicationId, ChainId, Destination},
};
use std::{path::PathBuf, task::Poll};

pub struct WasmApplication {
    bytecode_file: PathBuf,
}

impl WasmApplication {
    pub fn new(bytecode_file: impl Into<PathBuf>) -> Self {
        WasmApplication {
            bytecode_file: bytecode_file.into(),
        }
    }
}

#[async_trait]
impl UserApplication for WasmApplication {
    async fn execute_operation(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        operation: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.prepare_runtime(storage)?
            .execute_operation(context, operation)
            .await
    }

    async fn execute_effect(
        &self,
        context: &EffectContext,
        storage: &dyn WritableStorage,
        effect: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.prepare_runtime(storage)?
            .execute_effect(context, effect)
            .await
    }

    async fn call_application(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        self.prepare_runtime(storage)?
            .call_application(context, argument, forwarded_sessions)
            .await
    }

    async fn call_session(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        session_kind: u64,
        session_data: &mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, ExecutionError> {
        self.prepare_runtime(storage)?
            .call_session(
                context,
                session_kind,
                session_data,
                argument,
                forwarded_sessions,
            )
            .await
    }

    async fn query_application(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorage,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError> {
        let wrapped_storage = WrappedQueryableStorage(storage);
        let storage_reference = &wrapped_storage;
        let result = self
            .prepare_runtime(storage_reference)?
            .query_application(context, argument)
            .await;
        result
    }
}

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
    context_forwarder: ContextForwarder,
    application: R::Application,
    store: R::Store,
    _storage_guard: R::StorageGuard,
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

impl From<OperationContext> for application::OperationContext {
    fn from(host: OperationContext) -> Self {
        application::OperationContext {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            index: host
                .index
                .try_into()
                .expect("Operation index should fit in an `u64`"),
        }
    }
}

impl From<EffectContext> for application::EffectContext {
    fn from(host: EffectContext) -> Self {
        application::EffectContext {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            effect_id: host.effect_id.into(),
        }
    }
}

impl From<EffectId> for application::EffectId {
    fn from(host: EffectId) -> Self {
        application::EffectId {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            index: host
                .index
                .try_into()
                .expect("Effect index should fit in an `u64`"),
        }
    }
}

impl From<CalleeContext> for application::CalleeContext {
    fn from(host: CalleeContext) -> Self {
        application::CalleeContext {
            chain_id: host.chain_id.into(),
            authenticated_caller_id: host.authenticated_caller_id.map(|app_id| app_id.0),
        }
    }
}

impl From<QueryContext> for application::QueryContext {
    fn from(host: QueryContext) -> Self {
        application::QueryContext {
            chain_id: host.chain_id.into(),
        }
    }
}

impl From<SessionId> for application::SessionId {
    fn from(host: SessionId) -> Self {
        application::SessionId {
            application_id: host.application_id.0,
            kind: host.kind,
            index: host.index,
        }
    }
}

impl From<ChainId> for application::ChainId {
    fn from(chain_id: ChainId) -> Self {
        chain_id.0.into()
    }
}

impl From<HashValue> for application::HashValue {
    fn from(hash_value: HashValue) -> Self {
        let bytes = hash_value.as_bytes();

        application::HashValue {
            part1: u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices")),
            part2: u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices")),
            part3: u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices")),
            part4: u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices")),
            part5: u64::from_le_bytes(bytes[32..40].try_into().expect("incorrect indices")),
            part6: u64::from_le_bytes(bytes[40..48].try_into().expect("incorrect indices")),
            part7: u64::from_le_bytes(bytes[48..56].try_into().expect("incorrect indices")),
            part8: u64::from_le_bytes(bytes[56..64].try_into().expect("incorrect indices")),
        }
    }
}

impl<'storage, R> GuestFutureInterface<R> for ExecuteOperation
where
    R: Runtime,
    ExecutionError: From<R::Error>,
{
    type Output = RawExecutionResult<Vec<u8>>;

    fn poll(
        &self,
        application: &R::Application,
        store: &mut R::Store,
    ) -> Poll<Result<Self::Output, ExecutionError>> {
        match application.execute_operation_poll(store, self)? {
            PollExecutionResult::Ready(Ok(result)) => Poll::Ready(Ok(result.into())),
            PollExecutionResult::Ready(Err(message)) => {
                Poll::Ready(Err(ExecutionError::UserApplication(message)))
            }
            PollExecutionResult::Pending => Poll::Pending,
        }
    }
}

impl<'storage, R> GuestFutureInterface<R> for ExecuteEffect
where
    R: Runtime,
    ExecutionError: From<R::Error>,
{
    type Output = RawExecutionResult<Vec<u8>>;

    fn poll(
        &self,
        application: &R::Application,
        store: &mut R::Store,
    ) -> Poll<Result<Self::Output, ExecutionError>> {
        match application.execute_effect_poll(store, self)? {
            PollExecutionResult::Ready(Ok(result)) => Poll::Ready(Ok(result.into())),
            PollExecutionResult::Ready(Err(message)) => {
                Poll::Ready(Err(ExecutionError::UserApplication(message)))
            }
            PollExecutionResult::Pending => Poll::Pending,
        }
    }
}

impl<'storage, R> GuestFutureInterface<R> for CallApplication
where
    R: Runtime,
    ExecutionError: From<R::Error>,
{
    type Output = ApplicationCallResult;

    fn poll(
        &self,
        application: &R::Application,
        store: &mut R::Store,
    ) -> Poll<Result<Self::Output, ExecutionError>> {
        match application.call_application_poll(store, self)? {
            PollCallApplication::Ready(Ok(result)) => Poll::Ready(Ok(result.into())),
            PollCallApplication::Ready(Err(message)) => {
                Poll::Ready(Err(ExecutionError::UserApplication(message)))
            }
            PollCallApplication::Pending => Poll::Pending,
        }
    }
}

impl<'storage, R> GuestFutureInterface<R> for CallSession
where
    R: Runtime,
    ExecutionError: From<R::Error>,
{
    type Output = SessionCallResult;

    fn poll(
        &self,
        application: &R::Application,
        store: &mut R::Store,
    ) -> Poll<Result<Self::Output, ExecutionError>> {
        match application.call_session_poll(store, self)? {
            PollCallSession::Ready(Ok(result)) => Poll::Ready(Ok(result.into())),
            PollCallSession::Ready(Err(message)) => {
                Poll::Ready(Err(ExecutionError::UserApplication(message)))
            }
            PollCallSession::Pending => Poll::Pending,
        }
    }
}

impl<'storage, R> GuestFutureInterface<R> for QueryApplication
where
    R: Runtime,
    ExecutionError: From<R::Error>,
{
    type Output = Vec<u8>;

    fn poll(
        &self,
        application: &R::Application,
        store: &mut R::Store,
    ) -> Poll<Result<Self::Output, ExecutionError>> {
        match application.query_application_poll(store, self)? {
            PollQuery::Ready(Ok(result)) => Poll::Ready(Ok(result.into())),
            PollQuery::Ready(Err(message)) => {
                Poll::Ready(Err(ExecutionError::UserApplication(message)))
            }
            PollQuery::Pending => Poll::Pending,
        }
    }
}

impl From<application::SessionCallResult> for SessionCallResult {
    fn from(result: application::SessionCallResult) -> Self {
        SessionCallResult {
            inner: result.inner.into(),
            close_session: result.data.is_some(),
        }
    }
}

impl From<application::ApplicationCallResult> for ApplicationCallResult {
    fn from(result: application::ApplicationCallResult) -> Self {
        let create_sessions = result
            .create_sessions
            .into_iter()
            .map(NewSession::from)
            .collect();

        ApplicationCallResult {
            create_sessions,
            execution_result: result.execution_result.into(),
            value: result.value,
        }
    }
}

impl From<application::ExecutionResult> for RawExecutionResult<Vec<u8>> {
    fn from(result: application::ExecutionResult) -> Self {
        let effects = result
            .effects
            .into_iter()
            .map(|(destination, effect)| (destination.into(), effect))
            .collect();

        let subscribe = result
            .subscribe
            .into_iter()
            .map(|(channel_id, chain_id)| (channel_id, chain_id.into()))
            .collect();

        let unsubscribe = result
            .unsubscribe
            .into_iter()
            .map(|(channel_id, chain_id)| (channel_id, chain_id.into()))
            .collect();

        RawExecutionResult {
            effects,
            subscribe,
            unsubscribe,
        }
    }
}

impl From<application::Destination> for Destination {
    fn from(guest: application::Destination) -> Self {
        match guest {
            application::Destination::Recipient(chain_id) => {
                Destination::Recipient(chain_id.into())
            }
            application::Destination::Subscribers(channel_id) => {
                Destination::Subscribers(channel_id)
            }
        }
    }
}

impl From<application::SessionResult> for NewSession {
    fn from(guest: application::SessionResult) -> Self {
        NewSession {
            kind: guest.kind,
            data: guest.data,
        }
    }
}

impl From<application::HashValue> for HashValue {
    fn from(guest: application::HashValue) -> Self {
        let mut bytes = [0u8; 64];

        bytes[0..8].copy_from_slice(&guest.part1.to_le_bytes());
        bytes[8..16].copy_from_slice(&guest.part2.to_le_bytes());
        bytes[16..24].copy_from_slice(&guest.part3.to_le_bytes());
        bytes[24..32].copy_from_slice(&guest.part4.to_le_bytes());
        bytes[32..40].copy_from_slice(&guest.part5.to_le_bytes());
        bytes[40..48].copy_from_slice(&guest.part6.to_le_bytes());
        bytes[48..56].copy_from_slice(&guest.part7.to_le_bytes());
        bytes[56..64].copy_from_slice(&guest.part8.to_le_bytes());

        HashValue::try_from(&bytes[..]).expect("Incorrect byte count for `HashValue`")
    }
}

impl From<application::ChainId> for ChainId {
    fn from(guest: application::ChainId) -> Self {
        ChainId(guest.into())
    }
}

struct WrappedQueryableStorage<'storage>(&'storage dyn QueryableStorage);

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
