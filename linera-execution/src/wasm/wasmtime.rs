// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmtime](https://wasmtime.dev/) runtime.

// Export the system interface used by a user contract.
wit_bindgen_host_wasmtime_rust::export!("../linera-sdk/writable_system.wit");

// Export the system interface used by a user service.
wit_bindgen_host_wasmtime_rust::export!("../linera-sdk/queryable_system.wit");

// Import the interface implemented by a user contract.
wit_bindgen_host_wasmtime_rust::import!("../linera-sdk/contract.wit");

// Import the interface implemented by a user service.
wit_bindgen_host_wasmtime_rust::import!("../linera-sdk/service.wit");

#[path = "conversions_from_wit.rs"]
mod conversions_from_wit;
#[path = "conversions_to_wit.rs"]
mod conversions_to_wit;
#[path = "guest_futures.rs"]
mod guest_futures;

use self::{
    contract::ContractData,
    queryable_system::{QueryableSystem, QueryableSystemTables},
    service::ServiceData,
    writable_system::{WritableSystem, WritableSystemTables},
};
use super::{
    async_boundary::{ContextForwarder, HostFuture, HostFutureQueue},
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};
use crate::{CallResult, ExecutionError, QueryableStorage, SessionId, WritableStorage};
use linera_views::common::Batch;
use std::task::Poll;
use wasmtime::{Engine, Linker, Module, Store, Trap};
use wit_bindgen_host_wasmtime_rust::Le;

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for contracts.
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub struct Contract<'storage> {
    contract: contract::Contract<ContractState<'storage>>,
}

impl<'storage> ApplicationRuntimeContext for Contract<'storage> {
    type Store = Store<ContractState<'storage>>;
    type StorageGuard = ();
    type Error = Trap;
}

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for services.
pub struct Service<'storage> {
    service: service::Service<ServiceState<'storage>>,
}

impl<'storage> ApplicationRuntimeContext for Service<'storage> {
    type Store = Store<ServiceState<'storage>>;
    type StorageGuard = ();
    type Error = Trap;
}

impl WasmApplication {
    /// Prepare a runtime instance to call into the WASM contract.
    pub fn prepare_contract_runtime_with_wasmtime<'storage>(
        &self,
        storage: &'storage dyn WritableStorage,
    ) -> Result<WasmRuntimeContext<Contract<'storage>>, WasmExecutionError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        writable_system::add_to_linker(&mut linker, ContractState::system_api)?;

        let module = Module::new(&engine, &self.contract_bytecode)?;
        let context_forwarder = ContextForwarder::default();
        let state = ContractState::new(storage, context_forwarder.clone());
        let mut store = Store::new(&engine, state);
        let (contract, _instance) =
            contract::Contract::instantiate(&mut store, &module, &mut linker, ContractState::data)?;
        let application = Contract { contract };

        Ok(WasmRuntimeContext {
            context_forwarder,
            application,
            store,
            _storage_guard: (),
        })
    }

    /// Prepare a runtime instance to call into the WASM service.
    pub fn prepare_service_runtime_with_wasmtime<'storage>(
        &self,
        storage: &'storage dyn QueryableStorage,
    ) -> Result<WasmRuntimeContext<Service<'storage>>, WasmExecutionError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        queryable_system::add_to_linker(&mut linker, ServiceState::system_api)?;

        let module = Module::new(&engine, &self.service_bytecode)?;
        let context_forwarder = ContextForwarder::default();
        let state = ServiceState::new(storage, context_forwarder.clone());
        let mut store = Store::new(&engine, state);
        let (service, _instance) =
            service::Service::instantiate(&mut store, &module, &mut linker, ServiceState::data)?;
        let application = Service { service };

        Ok(WasmRuntimeContext {
            context_forwarder,
            application,
            store,
            _storage_guard: (),
        })
    }
}

/// Data stored by the runtime that's necessary for handling calls to and from the WASM module.
pub struct ContractState<'storage> {
    data: ContractData,
    system_api: SystemApi<&'storage dyn WritableStorage>,
    system_tables: WritableSystemTables<SystemApi<&'storage dyn WritableStorage>>,
}

/// Data stored by the runtime that's necessary for handling queries to and from the WASM module.
pub struct ServiceState<'storage> {
    data: ServiceData,
    system_api: SystemApi<&'storage dyn QueryableStorage>,
    system_tables: QueryableSystemTables<SystemApi<&'storage dyn QueryableStorage>>,
}

impl<'storage> ContractState<'storage> {
    /// Create a new instance of [`ContractState`].
    ///
    /// Uses `storage` to export the system API, and the `context` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(storage: &'storage dyn WritableStorage, context: ContextForwarder) -> Self {
        Self {
            data: ContractData::default(),
            system_api: SystemApi::new(context, storage),
            system_tables: WritableSystemTables::default(),
        }
    }

    /// Obtain the runtime instance specific [`ContractData`].
    pub fn data(&mut self) -> &mut ContractData {
        &mut self.data
    }

    /// Obtain the data required by the runtime to export the system API.
    pub fn system_api(
        &mut self,
    ) -> (
        &mut SystemApi<&'storage dyn WritableStorage>,
        &mut WritableSystemTables<SystemApi<&'storage dyn WritableStorage>>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }
}

impl<'storage> ServiceState<'storage> {
    /// Create a new instance of [`ServiceState`].
    ///
    /// Uses `storage` to export the system API, and the `context` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(storage: &'storage dyn QueryableStorage, context: ContextForwarder) -> Self {
        Self {
            data: ServiceData::default(),
            system_api: SystemApi::new(context, storage),
            system_tables: QueryableSystemTables::default(),
        }
    }

    /// Obtain the runtime instance specific [`ServiceData`].
    pub fn data(&mut self) -> &mut ServiceData {
        &mut self.data
    }

    /// Obtain the data required by the runtime to export the system API.
    pub fn system_api(
        &mut self,
    ) -> (
        &mut SystemApi<&'storage dyn QueryableStorage>,
        &mut QueryableSystemTables<SystemApi<&'storage dyn QueryableStorage>>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }
}

impl<'storage> common::Contract for Contract<'storage> {
    type Initialize = contract::Initialize;
    type ExecuteOperation = contract::ExecuteOperation;
    type ExecuteEffect = contract::ExecuteEffect;
    type CallApplication = contract::CallApplication;
    type CallSession = contract::CallSession;
    type OperationContext = contract::OperationContext;
    type EffectContext = contract::EffectContext;
    type CalleeContext = contract::CalleeContext;
    type SessionParam<'param> = contract::SessionParam<'param>;
    type SessionId = contract::SessionId;
    type PollExecutionResult = contract::PollExecutionResult;
    type PollCallApplication = contract::PollCallApplication;
    type PollCallSession = contract::PollCallSession;

    fn initialize_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::OperationContext,
        argument: &[u8],
    ) -> Result<contract::Initialize, Trap> {
        contract::Contract::initialize_new(&self.contract, store, context, argument)
    }

    fn initialize_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::Initialize,
    ) -> Result<contract::PollExecutionResult, Trap> {
        contract::Contract::initialize_poll(&self.contract, store, future)
    }

    fn execute_operation_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::OperationContext,
        operation: &[u8],
    ) -> Result<contract::ExecuteOperation, Trap> {
        contract::Contract::execute_operation_new(&self.contract, store, context, operation)
    }

    fn execute_operation_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::ExecuteOperation,
    ) -> Result<contract::PollExecutionResult, Trap> {
        contract::Contract::execute_operation_poll(&self.contract, store, future)
    }

    fn execute_effect_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::EffectContext,
        effect: &[u8],
    ) -> Result<contract::ExecuteEffect, Trap> {
        contract::Contract::execute_effect_new(&self.contract, store, context, effect)
    }

    fn execute_effect_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::ExecuteEffect,
    ) -> Result<contract::PollExecutionResult, Trap> {
        contract::Contract::execute_effect_poll(&self.contract, store, future)
    }

    fn call_application_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallApplication, Trap> {
        contract::Contract::call_application_new(
            &self.contract,
            store,
            context,
            argument,
            forwarded_sessions,
        )
    }

    fn call_application_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::CallApplication,
    ) -> Result<contract::PollCallApplication, Trap> {
        contract::Contract::call_application_poll(&self.contract, store, future)
    }

    fn call_session_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::CalleeContext,
        session: contract::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallSession, Trap> {
        contract::Contract::call_session_new(
            &self.contract,
            store,
            context,
            session,
            argument,
            forwarded_sessions,
        )
    }

    fn call_session_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::CallSession,
    ) -> Result<contract::PollCallSession, Trap> {
        contract::Contract::call_session_poll(&self.contract, store, future)
    }
}

impl<'storage> common::Service for Service<'storage> {
    type QueryApplication = service::QueryApplication;
    type QueryContext = service::QueryContext;
    type PollQuery = service::PollQuery;

    fn query_application_new(
        &self,
        store: &mut Store<ServiceState<'storage>>,
        context: service::QueryContext,
        argument: &[u8],
    ) -> Result<service::QueryApplication, Trap> {
        service::Service::query_application_new(&self.service, store, context, argument)
    }

    fn query_application_poll(
        &self,
        store: &mut Store<ServiceState<'storage>>,
        future: &service::QueryApplication,
    ) -> Result<service::PollQuery, Trap> {
        service::Service::query_application_poll(&self.service, store, future)
    }
}

/// Implementation to forward system calls from the guest WASM module to the host implementation.
pub struct SystemApi<S> {
    context: ContextForwarder,
    storage: S,
    future_queue: HostFutureQueue,
}

impl<S> SystemApi<S> {
    /// Create a new [`SystemApi`] instance using the provided asynchronous `context` and exporting
    /// the API from `storage`.
    pub fn new(context: ContextForwarder, storage: S) -> Self {
        SystemApi {
            context,
            storage,
            future_queue: HostFutureQueue::new(),
        }
    }
}

impl<'storage> WritableSystem for SystemApi<&'storage dyn WritableStorage> {
    type SimpleLoad = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;
    type SimpleLoadAndLock = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;
    type ViewLock = HostFuture<'storage, Result<(), ExecutionError>>;
    type ViewReadKeyBytes = HostFuture<'storage, Result<Option<Vec<u8>>, ExecutionError>>;
    type ViewFindKeys = HostFuture<'storage, Result<Vec<Vec<u8>>, ExecutionError>>;
    type ViewFindKeyValues = HostFuture<'storage, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;
    type ViewWriteBatch = HostFuture<'storage, Result<(), ExecutionError>>;
    type TryCallApplication = HostFuture<'storage, Result<CallResult, ExecutionError>>;
    type TryCallSession = HostFuture<'storage, Result<CallResult, ExecutionError>>;

    fn chain_id(&mut self) -> writable_system::ChainId {
        self.storage.chain_id().into()
    }

    fn application_id(&mut self) -> writable_system::ApplicationId {
        self.storage.application_id().into()
    }

    fn read_system_balance(&mut self) -> writable_system::SystemBalance {
        self.storage.read_system_balance().into()
    }

    fn read_system_timestamp(&mut self) -> writable_system::Timestamp {
        self.storage.read_system_timestamp().micros()
    }

    fn simple_load_new(&mut self) -> Self::SimpleLoad {
        self.future_queue.add(self.storage.try_read_my_state())
    }

    fn simple_load_poll(&mut self, future: &Self::SimpleLoad) -> writable_system::PollSimpleLoad {
        use writable_system::PollSimpleLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollSimpleLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollSimpleLoad::Ready(Err(error.to_string())),
        }
    }

    fn simple_load_and_lock_new(&mut self) -> Self::SimpleLoadAndLock {
        self.future_queue
            .add(self.storage.try_read_and_lock_my_state())
    }

    fn simple_load_and_lock_poll(&mut self, future: &Self::SimpleLoadAndLock) -> writable_system::PollSimpleLoad {
        use writable_system::PollLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollSimpleLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollSimpleLoad::Ready(Err(error.to_string())),
        }
    }

    fn view_lock_new(&mut self) -> Self::ViewLock {
        HostFuture::new(self.storage.view_lock_user_state())
    }

    fn view_lock_poll(&mut self, future: &Self::ViewLock) -> writable_system::PollViewLock {
        use writable_system::PollViewLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewLock::Pending,
            Poll::Ready(Ok(())) => PollViewLock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollViewLock::Ready(Err(error.to_string())),
        }
    }

    fn view_read_key_bytes_new(&mut self, key: &[u8]) -> Self::ViewReadKeyBytes {
        HostFuture::new(self.storage.view_read_key_bytes(key.to_owned()))
    }

    fn view_read_key_bytes_poll(
        &mut self,
        future: &Self::ViewReadKeyBytes,
    ) -> writable_system::PollViewReadKeyBytes {
        use writable_system::PollViewReadKeyBytes;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewReadKeyBytes::Pending,
            Poll::Ready(Ok(opt_list)) => PollViewReadKeyBytes::Ready(Ok(opt_list)),
            Poll::Ready(Err(error)) => PollViewReadKeyBytes::Ready(Err(error.to_string())),
        }
    }

    fn view_find_keys_new(&mut self, key_prefix: &[u8]) -> Self::ViewFindKeys {
        HostFuture::new(
            self.storage
                .view_find_keys_by_prefix(key_prefix.to_owned()),
        )
    }

    fn view_find_keys_poll(&mut self, future: &Self::ViewFindKeys) -> writable_system::PollViewFindKeys {
        use writable_system::PollViewFindKeys;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewFindKeys::Pending,
            Poll::Ready(Ok(keys)) => PollViewFindKeys::Ready(Ok(keys)),
            Poll::Ready(Err(error)) => PollViewFindKeys::Ready(Err(error.to_string())),
        }
    }

    fn view_find_key_values_new(&mut self, key_prefix: &[u8]) -> Self::ViewFindKeyValues {
        HostFuture::new(
            self.storage
                .view_find_key_values_by_prefix(key_prefix.to_owned()),
        )
    }

    fn view_find_key_values_poll(
        &mut self,
        future: &Self::ViewFindKeyValues,
    ) -> writable_system::PollViewFindKeyValues {
        use writable_system::PollViewFindKeyValues;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewFindKeyValues::Pending,
            Poll::Ready(Ok(key_values)) => PollViewFindKeyValues::Ready(Ok(key_values)),
            Poll::Ready(Err(error)) => PollViewFindKeyValues::Ready(Err(error.to_string())),
        }
    }

    fn view_write_batch_new(
        &mut self,
        list_oper: Vec<writable_system::ViewWriteOperation>,
    ) -> Self::ViewWriteBatch {
        let mut batch = Batch::default();
        for x in list_oper {
            match x {
                writable_system::ViewWriteOperation::Delete(key) => batch.delete_key(key.to_vec()),
                writable_system::ViewWriteOperation::Deleteprefix(key_prefix) => {
                    batch.delete_key_prefix(key_prefix.to_vec())
                }
                writable_system::ViewWriteOperation::Put(key_value) => {
                    batch.put_key_value_bytes(key_value.0.to_vec(), key_value.1.to_vec())
                }
            }
        }
        HostFuture::new(self.storage.view_write_batch_and_unlock(batch))
    }

    fn view_write_batch_poll(&mut self, future: &Self::ViewWriteBatch) -> writable_system::PollViewWriteBatch {
        use writable_system::PollViewWriteBatch;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewWriteBatch::Pending,
            Poll::Ready(Ok(())) => PollViewWriteBatch::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollViewWriteBatch::Ready(Err(error.to_string())),
        }
    }

    fn simple_lock_new(&mut self) -> Self::SimpleLock {
        HostFuture::new(self.storage.lock_userkv_state())
    }

    fn simple_lock_poll(&mut self, future: &Self::SimpleLock) -> writable_system::PollSimpleLock {
        use writable_system::PollSimpleLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLock::Pending,
            Poll::Ready(Ok(())) => PollSimpleLock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollSimpleLock::Ready(Err(error.to_string())),
        }
    }

    fn try_call_application_new(
        &mut self,
        authenticated: bool,
        application: writable_system::ApplicationId,
        argument: &[u8],
        forwarded_sessions: &[Le<writable_system::SessionId>],
    ) -> Self::TryCallApplication {
        let storage = self.storage;
        let forwarded_sessions = forwarded_sessions
            .iter()
            .map(Le::get)
            .map(SessionId::from)
            .collect();
        let argument = Vec::from(argument);

        self.future_queue.add(async move {
            storage
                .try_call_application(
                    authenticated,
                    application.into(),
                    &argument,
                    forwarded_sessions,
                )
                .await
        })
    }

    fn try_call_application_poll(
        &mut self,
        future: &Self::TryCallApplication,
    ) -> writable_system::PollCallResult {
        use writable_system::PollCallResult;
        match future.poll(&mut self.context) {
            Poll::Pending => PollCallResult::Pending,
            Poll::Ready(Ok(result)) => PollCallResult::Ready(Ok(result.into())),
            Poll::Ready(Err(error)) => PollCallResult::Ready(Err(error.to_string())),
        }
    }

    fn try_call_session_new(
        &mut self,
        authenticated: bool,
        session: writable_system::SessionId,
        argument: &[u8],
        forwarded_sessions: &[Le<writable_system::SessionId>],
    ) -> Self::TryCallApplication {
        let storage = self.storage;
        let forwarded_sessions = forwarded_sessions
            .iter()
            .map(Le::get)
            .map(SessionId::from)
            .collect();
        let argument = Vec::from(argument);

        self.future_queue.add(async move {
            storage
                .try_call_session(authenticated, session.into(), &argument, forwarded_sessions)
                .await
        })
    }

    fn try_call_session_poll(
        &mut self,
        future: &Self::TryCallApplication,
    ) -> writable_system::PollCallResult {
        use writable_system::PollCallResult;
        match future.poll(&mut self.context) {
            Poll::Pending => PollCallResult::Pending,
            Poll::Ready(Ok(result)) => PollCallResult::Ready(Ok(result.into())),
            Poll::Ready(Err(error)) => PollCallResult::Ready(Err(error.to_string())),
        }
    }

    fn log(&mut self, message: &str, level: writable_system::LogLevel) {
        log::log!(level.into(), "{message}");
    }
}

impl<'storage> QueryableSystem for SystemApi<&'storage dyn QueryableStorage> {
    type SimpleLoad = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;
    type TryQueryApplication = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;
    type ViewLock = HostFuture<'storage, Result<(), ExecutionError>>;
    type ViewUnlock = HostFuture<'storage, Result<(), ExecutionError>>;
    type ViewReadKeyBytes = HostFuture<'storage, Result<Option<Vec<u8>>, ExecutionError>>;
    type ViewFindKeys = HostFuture<'storage, Result<Vec<Vec<u8>>, ExecutionError>>;
    type ViewFindKeyValues = HostFuture<'storage, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;

    fn chain_id(&mut self) -> queryable_system::ChainId {
        self.storage.chain_id().into()
    }

    fn application_id(&mut self) -> queryable_system::ApplicationId {
        self.storage.application_id().into()
    }

    fn read_system_balance(&mut self) -> queryable_system::SystemBalance {
        self.storage.read_system_balance().into()
    }

    fn read_system_timestamp(&mut self) -> queryable_system::Timestamp {
        self.storage.read_system_timestamp().micros()
    }

    fn simple_load_new(&mut self) -> Self::SimpleLoad {
        self.future_queue.add(self.storage.try_read_my_state())
    }

    fn simple_load_poll(&mut self, future: &Self::SimpleLoad) -> queryable_system::PollSimpleLoad {
        use queryable_system::PollSimpleLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollSimpleLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollSimpleLoad::Ready(Err(error.to_string())),
        }
    }

    fn view_lock_new(&mut self) -> Self::ViewLock {
        HostFuture::new(self.storage.view_lock_user_state())
    }

    fn view_lock_poll(&mut self, future: &Self::ViewLock) -> queryable_system::PollViewLock {
        use queryable_system::PollViewLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewLock::Pending,
            Poll::Ready(Ok(())) => PollViewLock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollViewLock::Ready(Err(error.to_string())),
        }
    }

    fn view_unlock_new(&mut self) -> Self::ViewUnlock {
        HostFuture::new(self.storage.view_unlock_user_state())
    }

    fn view_unlock_poll(&mut self, future: &Self::ViewUnlock) -> queryable_system::PollViewUnlock {
        use queryable_system::PollViewUnlock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewUnlock::Pending,
            Poll::Ready(Ok(())) => PollViewUnlock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollViewUnlock::Ready(Err(error.to_string())),
        }
    }

    fn view_read_key_bytes_new(&mut self, key: &[u8]) -> Self::ViewReadKeyBytes {
        HostFuture::new(self.storage.view_read_key_bytes(key.to_owned()))
    }

    fn view_read_key_bytes_poll(
        &mut self,
        future: &Self::ViewReadKeyBytes,
    ) -> queryable_system::PollViewReadKeyBytes {
        use queryable_system::PollViewReadKeyBytes;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewReadKeyBytes::Pending,
            Poll::Ready(Ok(opt_list)) => PollViewReadKeyBytes::Ready(Ok(opt_list)),
            Poll::Ready(Err(error)) => PollViewReadKeyBytes::Ready(Err(error.to_string())),
        }
    }

    fn view_find_keys_new(&mut self, key_prefix: &[u8]) -> Self::ViewFindKeys {
        HostFuture::new(
            self.storage
                .view_find_keys_by_prefix(key_prefix.to_owned()),
        )
    }

    fn view_find_keys_poll(&mut self, future: &Self::ViewFindKeys) -> queryable_system::PollViewFindKeys {
        use queryable_system::PollViewFindKeys;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewFindKeys::Pending,
            Poll::Ready(Ok(keys)) => PollViewFindKeys::Ready(Ok(keys)),
            Poll::Ready(Err(error)) => PollViewFindKeys::Ready(Err(error.to_string())),
        }
    }

    fn view_find_key_values_new(&mut self, key_prefix: &[u8]) -> Self::ViewFindKeyValues {
        HostFuture::new(
            self.storage
                .view_find_key_values_by_prefix(key_prefix.to_owned()),
        )
    }

    fn view_find_key_values_poll(
        &mut self,
        future: &Self::ViewFindKeyValues,
    ) -> queryable_system::PollViewFindKeyValues {
        use queryable_system::PollViewFindKeyValues;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewFindKeyValues::Pending,
            Poll::Ready(Ok(key_values)) => PollViewFindKeyValues::Ready(Ok(key_values)),
            Poll::Ready(Err(error)) => PollViewFindKeyValues::Ready(Err(error.to_string())),
        }
    }

    fn simple_lock_new(&mut self) -> Self::SimpleLock {
        HostFuture::new(self.storage.lock_userkv_state())
    }

    fn simple_lock_poll(&mut self, future: &Self::SimpleLock) -> queryable_system::PollSimpleLock {
        use queryable_system::PollSimpleLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLock::Pending,
            Poll::Ready(Ok(())) => PollSimpleLock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollSimpleLock::Ready(Err(error.to_string())),
        }
    }

    fn simple_unlock_new(&mut self) -> Self::SimpleUnlock {
        HostFuture::new(self.storage.unlock_userkv_state())
    }

    fn simple_unlock_poll(&mut self, future: &Self::SimpleLock) -> queryable_system::PollSimpleUnlock {
        use queryable_system::PollSimpleUnlock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleUnlock::Pending,
            Poll::Ready(Ok(())) => PollSimpleUnlock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollSimpleUnlock::Ready(Err(error.to_string())),
        }
    }

    fn try_query_application_new(
        &mut self,
        application: queryable_system::ApplicationId,
        argument: &[u8],
    ) -> Self::TryQueryApplication {
        let storage = self.storage;
        let argument = Vec::from(argument);

        self.future_queue.add(async move {
            storage
                .try_query_application(application.into(), &argument)
                .await
        })
    }

    fn try_query_application_poll(
        &mut self,
        future: &Self::TryQueryApplication,
    ) -> queryable_system::PollLoad {
        use queryable_system::PollLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(result)) => PollLoad::Ready(Ok(result)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn log(&mut self, message: &str, level: queryable_system::LogLevel) {
        log::log!(level.into(), "{message}");
    }
}
