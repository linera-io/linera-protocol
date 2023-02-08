#[cfg(feature = "wasmer")]
use super::wasmer;
#[cfg(feature = "wasmtime")]
use super::wasmtime;
use super::{
    async_boundary::GuestFutureInterface,
    common::{self, ApplicationRuntimeContext},
    WasmExecutionError,
};
use crate::{
    ApplicationCallResult, CalleeContext, EffectContext, OperationContext, QueryContext,
    QueryableStorage, RawExecutionResult, SessionCallResult, SessionId, WritableStorage,
};
use derive_more::From;
use std::{marker::PhantomData, task::Poll};

/// The contract application attached to a runtime to run it.
#[derive(From)]
#[allow(clippy::large_enum_variant)]
pub enum Contract<'storage> {
    #[cfg(feature = "wasmer")]
    Wasmer(wasmer::Contract<'storage>),
    #[cfg(feature = "wasmtime")]
    Wasmtime(wasmtime::Contract<'storage>),
}

/// The service application attached to a runtime to run it.
#[derive(From)]
pub enum Service<'storage> {
    #[cfg(feature = "wasmer")]
    Wasmer(wasmer::Service<'storage>),
    #[cfg(feature = "wasmtime")]
    Wasmtime(wasmtime::Service<'storage>),
}

impl<'storage> ApplicationRuntimeContext for Contract<'storage> {
    type Error = Error;
    type Store = ContractStore<'storage>;
    type StorageGuard = StorageGuard<'storage, &'static dyn WritableStorage>;
}

impl<'storage> ApplicationRuntimeContext for Service<'storage> {
    type Error = Error;
    type Store = ServiceStore<'storage>;
    type StorageGuard = StorageGuard<'storage, &'static dyn QueryableStorage>;
}

/// The runtime specific error type.
#[derive(From)]
pub enum Error {
    #[cfg(feature = "wasmer")]
    Wasmer(::wasmer::RuntimeError),
    #[cfg(feature = "wasmtime")]
    Wasmtime(::wasmtime::Trap),
}

impl From<Error> for WasmExecutionError {
    fn from(error: Error) -> Self {
        match error {
            #[cfg(feature = "wasmer")]
            Error::Wasmer(error) => WasmExecutionError::ExecuteModuleInWasmer(error),
            #[cfg(feature = "wasmtime")]
            Error::Wasmtime(error) => WasmExecutionError::ExecuteModuleInWasmtime(error),
        }
    }
}

/// The runtime specific WebAssembly storage type for contract applications.
#[derive(From)]
pub enum ContractStore<'storage> {
    #[cfg(feature = "wasmer")]
    #[from(ignore)]
    Wasmer(::wasmer::Store, PhantomData<&'storage ()>),
    #[cfg(feature = "wasmtime")]
    Wasmtime(::wasmtime::Store<wasmtime::ContractState<'storage>>),
}

#[cfg(feature = "wasmer")]
impl From<::wasmer::Store> for ContractStore<'_> {
    fn from(store: ::wasmer::Store) -> Self {
        ContractStore::Wasmer(store, PhantomData)
    }
}

/// The runtime specific WebAssembly storage type for service applications.
#[derive(From)]
pub enum ServiceStore<'storage> {
    #[cfg(feature = "wasmer")]
    Wasmer(::wasmer::Store, PhantomData<&'storage ()>),
    #[cfg(feature = "wasmtime")]
    Wasmtime(::wasmtime::Store<wasmtime::ServiceState<'storage>>),
}

#[cfg(feature = "wasmer")]
impl From<::wasmer::Store> for ServiceStore<'_> {
    fn from(store: ::wasmer::Store) -> Self {
        ServiceStore::Wasmer(store, PhantomData)
    }
}

/// The runtime specific storage guard type.
#[derive(From)]
pub enum StorageGuard<'storage, S> {
    #[cfg(feature = "wasmer")]
    Wasmer(wasmer::StorageGuard<'storage, S>),
    #[cfg(feature = "wasmtime")]
    #[from(ignore)]
    Wasmtime(PhantomData<&'storage S>),
}

#[cfg(feature = "wasmtime")]
impl<S> From<()> for StorageGuard<'_, S> {
    fn from(_: ()) -> Self {
        StorageGuard::Wasmtime(PhantomData)
    }
}

impl<'storage> common::Contract for Contract<'storage> {
    type Initialize = Initialize;
    type ExecuteOperation = ExecuteOperation;
    type ExecuteEffect = ExecuteEffect;
    type CallApplication = CallApplication;
    type CallSession = CallSession;
    type OperationContext = OperationContext;
    type EffectContext = EffectContext;
    type CalleeContext = CalleeContext;
    type SessionParam<'param> = (u64, &'param [u8]);
    type SessionId = SessionId;
    type PollExecutionResult = PollExecutionResult;
    type PollCallApplication = PollCallApplication;
    type PollCallSession = PollCallSession;

    fn initialize_new(
        &self,
        store: &mut Self::Store,
        context: Self::OperationContext,
        argument: &[u8],
    ) -> Result<Self::Initialize, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store) {
            #[cfg(feature = "wasmer")]
            (Contract::Wasmer(contract), ContractStore::Wasmer(store, _)) => contract
                .initialize_new(store, context.into(), argument)
                .map(Initialize::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (Contract::Wasmtime(contract), ContractStore::Wasmtime(store)) => contract
                .initialize_new(store, context.into(), argument)
                .map(Initialize::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application and runtime storage types"),
        }
    }

    fn initialize_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::Initialize,
    ) -> Result<Self::PollExecutionResult, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store, future) {
            #[cfg(feature = "wasmer")]
            (
                Contract::Wasmer(contract),
                ContractStore::Wasmer(store, _),
                Initialize::Wasmer(future),
            ) => contract
                .initialize_poll(store, future)
                .map(PollExecutionResult::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (
                Contract::Wasmtime(contract),
                ContractStore::Wasmtime(store),
                Initialize::Wasmtime(future),
            ) => contract
                .initialize_poll(store, future)
                .map(PollExecutionResult::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application, runtime storage and future types"),
        }
    }

    fn execute_operation_new(
        &self,
        store: &mut Self::Store,
        context: Self::OperationContext,
        operation: &[u8],
    ) -> Result<Self::ExecuteOperation, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store) {
            #[cfg(feature = "wasmer")]
            (Contract::Wasmer(contract), ContractStore::Wasmer(store, _)) => contract
                .execute_operation_new(store, context.into(), operation)
                .map(ExecuteOperation::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (Contract::Wasmtime(contract), ContractStore::Wasmtime(store)) => contract
                .execute_operation_new(store, context.into(), operation)
                .map(ExecuteOperation::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application and runtime storage types"),
        }
    }

    fn execute_operation_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::ExecuteOperation,
    ) -> Result<Self::PollExecutionResult, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store, future) {
            #[cfg(feature = "wasmer")]
            (
                Contract::Wasmer(contract),
                ContractStore::Wasmer(store, _),
                ExecuteOperation::Wasmer(future),
            ) => contract
                .execute_operation_poll(store, future)
                .map(PollExecutionResult::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (
                Contract::Wasmtime(contract),
                ContractStore::Wasmtime(store),
                ExecuteOperation::Wasmtime(future),
            ) => contract
                .execute_operation_poll(store, future)
                .map(PollExecutionResult::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application, runtime storage and future types"),
        }
    }

    fn execute_effect_new(
        &self,
        store: &mut Self::Store,
        context: Self::EffectContext,
        effect: &[u8],
    ) -> Result<Self::ExecuteEffect, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store) {
            #[cfg(feature = "wasmer")]
            (Contract::Wasmer(contract), ContractStore::Wasmer(store, _)) => contract
                .execute_effect_new(store, context.into(), effect)
                .map(ExecuteEffect::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (Contract::Wasmtime(contract), ContractStore::Wasmtime(store)) => contract
                .execute_effect_new(store, context.into(), effect)
                .map(ExecuteEffect::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application and runtime storage types"),
        }
    }

    fn execute_effect_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::ExecuteEffect,
    ) -> Result<Self::PollExecutionResult, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store, future) {
            #[cfg(feature = "wasmer")]
            (
                Contract::Wasmer(contract),
                ContractStore::Wasmer(store, _),
                ExecuteEffect::Wasmer(future),
            ) => contract
                .execute_effect_poll(store, future)
                .map(PollExecutionResult::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (
                Contract::Wasmtime(contract),
                ContractStore::Wasmtime(store),
                ExecuteEffect::Wasmtime(future),
            ) => contract
                .execute_effect_poll(store, future)
                .map(PollExecutionResult::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application, runtime storage and future types"),
        }
    }

    fn call_application_new(
        &self,
        store: &mut Self::Store,
        context: Self::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[Self::SessionId],
    ) -> Result<Self::CallApplication, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store) {
            #[cfg(feature = "wasmer")]
            (Contract::Wasmer(contract), ContractStore::Wasmer(store, _)) => {
                let forwarded_sessions: Vec<_> = forwarded_sessions
                    .iter()
                    .copied()
                    .map(|session_id| session_id.into())
                    .collect();

                contract
                    .call_application_new(store, context.into(), argument, &forwarded_sessions)
                    .map(CallApplication::from)
                    .map_err(Error::from)
            }
            #[cfg(feature = "wasmtime")]
            (Contract::Wasmtime(contract), ContractStore::Wasmtime(store)) => {
                let forwarded_sessions: Vec<_> = forwarded_sessions
                    .iter()
                    .copied()
                    .map(|session_id| session_id.into())
                    .collect();

                contract
                    .call_application_new(store, context.into(), argument, &forwarded_sessions)
                    .map(CallApplication::from)
                    .map_err(Error::from)
            }
            _ => unreachable!("Unmatched application and runtime storage types"),
        }
    }

    fn call_application_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::CallApplication,
    ) -> Result<Self::PollCallApplication, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store, future) {
            #[cfg(feature = "wasmer")]
            (
                Contract::Wasmer(contract),
                ContractStore::Wasmer(store, _),
                CallApplication::Wasmer(future),
            ) => contract
                .call_application_poll(store, future)
                .map(PollCallApplication::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (
                Contract::Wasmtime(contract),
                ContractStore::Wasmtime(store),
                CallApplication::Wasmtime(future),
            ) => contract
                .call_application_poll(store, future)
                .map(PollCallApplication::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application, runtime storage and future types"),
        }
    }

    fn call_session_new(
        &self,
        store: &mut Self::Store,
        context: Self::CalleeContext,
        session: Self::SessionParam<'_>,
        argument: &[u8],
        forwarded_sessions: &[Self::SessionId],
    ) -> Result<Self::CallSession, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store) {
            #[cfg(feature = "wasmer")]
            (Contract::Wasmer(contract), ContractStore::Wasmer(store, _)) => {
                let forwarded_sessions: Vec<_> = forwarded_sessions
                    .iter()
                    .copied()
                    .map(|session_id| session_id.into())
                    .collect();

                contract
                    .call_session_new(
                        store,
                        context.into(),
                        session.into(),
                        argument,
                        &forwarded_sessions,
                    )
                    .map(CallSession::from)
                    .map_err(Error::from)
            }
            #[cfg(feature = "wasmtime")]
            (Contract::Wasmtime(contract), ContractStore::Wasmtime(store)) => {
                let forwarded_sessions: Vec<_> = forwarded_sessions
                    .iter()
                    .copied()
                    .map(|session_id| session_id.into())
                    .collect();

                contract
                    .call_session_new(
                        store,
                        context.into(),
                        session.into(),
                        argument,
                        &forwarded_sessions,
                    )
                    .map(CallSession::from)
                    .map_err(Error::from)
            }
            _ => unreachable!("Unmatched application and runtime storage types"),
        }
    }

    fn call_session_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::CallSession,
    ) -> Result<Self::PollCallSession, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store, future) {
            #[cfg(feature = "wasmer")]
            (
                Contract::Wasmer(contract),
                ContractStore::Wasmer(store, _),
                CallSession::Wasmer(future),
            ) => contract
                .call_session_poll(store, future)
                .map(PollCallSession::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (
                Contract::Wasmtime(contract),
                ContractStore::Wasmtime(store),
                CallSession::Wasmtime(future),
            ) => contract
                .call_session_poll(store, future)
                .map(PollCallSession::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application, runtime storage and future types"),
        }
    }
}

impl<'storage> common::Service for Service<'storage> {
    type QueryApplication = QueryApplication;
    type QueryContext = QueryContext;
    type PollQuery = PollQuery;

    fn query_application_new(
        &self,
        store: &mut Self::Store,
        context: Self::QueryContext,
        argument: &[u8],
    ) -> Result<Self::QueryApplication, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store) {
            #[cfg(feature = "wasmer")]
            (Service::Wasmer(contract), ServiceStore::Wasmer(store, _)) => contract
                .query_application_new(store, context.into(), argument)
                .map(QueryApplication::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (Service::Wasmtime(contract), ServiceStore::Wasmtime(store)) => contract
                .query_application_new(store, context.into(), argument)
                .map(QueryApplication::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application and runtime storage types"),
        }
    }

    fn query_application_poll(
        &self,
        store: &mut Self::Store,
        future: &Self::QueryApplication,
    ) -> Result<Self::PollQuery, Self::Error> {
        #[allow(unreachable_patterns)]
        match (self, store, future) {
            #[cfg(feature = "wasmer")]
            (
                Service::Wasmer(contract),
                ServiceStore::Wasmer(store, _),
                QueryApplication::Wasmer(future),
            ) => contract
                .query_application_poll(store, future)
                .map(PollQuery::from)
                .map_err(Error::from),
            #[cfg(feature = "wasmtime")]
            (
                Service::Wasmtime(contract),
                ServiceStore::Wasmtime(store),
                QueryApplication::Wasmtime(future),
            ) => contract
                .query_application_poll(store, future)
                .map(PollQuery::from)
                .map_err(Error::from),
            _ => unreachable!("Unmatched application, runtime storage and future types"),
        }
    }
}

/// Generates future wrapper types and their respective [`GuestFutureInterface`] implementations.
macro_rules! guest_future_dispatchers {
    (
        $( ( $module:ident, $trait:ident, $store:ident ) :: $future:ident -> $output:ty ),* $(,)*
    ) => {
        $(
            #[derive(From)]
            pub enum $future {
                #[cfg(feature = "wasmer")]
                Wasmer(wasmer::$module::$future),
                #[cfg(feature = "wasmtime")]
                Wasmtime(wasmtime::$module::$future),
            }

            impl<'storage> GuestFutureInterface<$trait<'storage>> for $future {
                type Output = $output;

                fn poll(
                    &self,
                    application: &$trait<'storage>,
                    store: &mut $store<'storage>,
                ) -> Poll<Result<Self::Output, WasmExecutionError>> {
                    #[allow(unreachable_patterns)]
                    match (self, application, store) {
                        #[cfg(feature = "wasmer")]
                        (
                            $future::Wasmer(future),
                            $trait::Wasmer(contract),
                            $store::Wasmer(store, _),
                        ) => future.poll(contract, store),
                        #[cfg(feature = "wasmtime")]
                        (
                            $future::Wasmtime(future),
                            $trait::Wasmtime(contract),
                            $store::Wasmtime(store),
                        ) => future.poll(contract, store),
                        _ => unreachable!("Unmatched application and runtime storage types"),
                    }
                }
            }
        )*
    };
}

guest_future_dispatchers! {
    (contract, Contract, ContractStore)::Initialize -> RawExecutionResult<Vec<u8>>,
    (contract, Contract, ContractStore)::ExecuteOperation -> RawExecutionResult<Vec<u8>>,
    (contract, Contract, ContractStore)::ExecuteEffect -> RawExecutionResult<Vec<u8>>,
    (contract, Contract, ContractStore)::CallApplication -> ApplicationCallResult,
    (contract, Contract, ContractStore)::CallSession -> (SessionCallResult, Vec<u8>),
    (service, Service, ServiceStore)::QueryApplication -> Vec<u8>,
}

/// Wrapper type around the `PollExecutionResult` WIT type generated for each runtime.
#[derive(From)]
pub enum PollExecutionResult {
    #[cfg(feature = "wasmer")]
    Wasmer(wasmer::contract::PollExecutionResult),
    #[cfg(feature = "wasmtime")]
    Wasmtime(wasmtime::contract::PollExecutionResult),
}

/// Wrapper type around the `PollCallApplication` WIT type generated for each runtime.
#[derive(From)]
pub enum PollCallApplication {
    #[cfg(feature = "wasmer")]
    Wasmer(wasmer::contract::PollCallApplication),
    #[cfg(feature = "wasmtime")]
    Wasmtime(wasmtime::contract::PollCallApplication),
}

/// Wrapper type around the `PollCallSession` WIT type generated for each runtime.
#[derive(From)]
pub enum PollCallSession {
    #[cfg(feature = "wasmer")]
    Wasmer(wasmer::contract::PollCallSession),
    #[cfg(feature = "wasmtime")]
    Wasmtime(wasmtime::contract::PollCallSession),
}

/// Wrapper type around the `PollQuery` WIT type generated for each runtime.
#[derive(From)]
pub enum PollQuery {
    #[cfg(feature = "wasmer")]
    Wasmer(wasmer::service::PollQuery),
    #[cfg(feature = "wasmtime")]
    Wasmtime(wasmtime::service::PollQuery),
}
