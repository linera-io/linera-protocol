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
    async_boundary::{ContextForwarder, HostFuture},
    async_determinism::{HostFutureQueue, QueuedHostFutureFactory},
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};
use crate::{CallResult, ExecutionError, QueryableStorage, SessionId, WritableStorage};
use linera_views::{common::Batch, views::ViewError};
use std::task::Poll;
use tokio::sync::oneshot;
use wasmtime::{Config, Engine, Linker, Module, Store, Trap};
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
    type Error = Trap;
    type Extra = ();

    fn finalize(context: &mut WasmRuntimeContext<Self>) {
        let storage = context.store.data().system_api.storage();
        let initial_fuel = storage.remaining_fuel();
        let remaining_fuel = initial_fuel - context.store.fuel_consumed().unwrap_or(0);

        storage.set_remaining_fuel(remaining_fuel);
    }
}

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for services.
pub struct Service<'storage> {
    service: service::Service<ServiceState<'storage>>,
}

impl<'storage> ApplicationRuntimeContext for Service<'storage> {
    type Store = Store<ServiceState<'storage>>;
    type Error = Trap;
    type Extra = ();
}

impl WasmApplication {
    /// Prepare a runtime instance to call into the WASM contract.
    pub fn prepare_contract_runtime_with_wasmtime<'storage>(
        &self,
        storage: &'storage dyn WritableStorage,
    ) -> Result<WasmRuntimeContext<'storage, Contract<'storage>>, WasmExecutionError> {
        let mut config = Config::default();
        config.consume_fuel(true);

        let engine = Engine::new(&config).map_err(WasmExecutionError::CreateWasmtimeEngine)?;
        let mut linker = Linker::new(&engine);

        writable_system::add_to_linker(&mut linker, ContractState::system_api)?;

        let module = Module::new(&engine, &self.contract_bytecode)?;
        let context_forwarder = ContextForwarder::default();
        let (future_queue, queued_future_factory) = HostFutureQueue::new();
        let (internal_error_sender, internal_error_receiver) = oneshot::channel();
        let state = ContractState::new(
            storage,
            context_forwarder.clone(),
            queued_future_factory,
            internal_error_sender,
        );
        let mut store = Store::new(&engine, state);
        let (contract, _instance) =
            contract::Contract::instantiate(&mut store, &module, &mut linker, ContractState::data)?;
        let application = Contract { contract };

        store
            .add_fuel(storage.remaining_fuel())
            .expect("Fuel consumption wasn't properly enabled");

        Ok(WasmRuntimeContext {
            context_forwarder,
            application,
            future_queue,
            internal_error_receiver,
            store,
            extra: (),
        })
    }

    /// Prepare a runtime instance to call into the WASM service.
    pub fn prepare_service_runtime_with_wasmtime<'storage>(
        &self,
        storage: &'storage dyn QueryableStorage,
    ) -> Result<WasmRuntimeContext<'storage, Service<'storage>>, WasmExecutionError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        queryable_system::add_to_linker(&mut linker, ServiceState::system_api)?;

        let module = Module::new(&engine, &self.service_bytecode)?;
        let context_forwarder = ContextForwarder::default();
        let (future_queue, _queued_future_factory) = HostFutureQueue::new();
        let (_internal_error_sender, internal_error_receiver) = oneshot::channel();
        let state = ServiceState::new(storage, context_forwarder.clone());
        let mut store = Store::new(&engine, state);
        let (service, _instance) =
            service::Service::instantiate(&mut store, &module, &mut linker, ServiceState::data)?;
        let application = Service { service };

        Ok(WasmRuntimeContext {
            context_forwarder,
            application,
            future_queue,
            internal_error_receiver,
            store,
            extra: (),
        })
    }
}

/// Data stored by the runtime that's necessary for handling calls to and from the WASM module.
pub struct ContractState<'storage> {
    data: ContractData,
    system_api: ContractSystemApi<'storage>,
    system_tables: WritableSystemTables<ContractSystemApi<'storage>>,
}

/// Data stored by the runtime that's necessary for handling queries to and from the WASM module.
pub struct ServiceState<'storage> {
    data: ServiceData,
    system_api: ServiceSystemApi<'storage>,
    system_tables: QueryableSystemTables<ServiceSystemApi<'storage>>,
}

impl<'storage> ContractState<'storage> {
    /// Create a new instance of [`ContractState`].
    ///
    /// Uses `storage` to export the system API, and the `context` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(
        storage: &'storage dyn WritableStorage,
        context: ContextForwarder,
        queued_future_factory: QueuedHostFutureFactory<'storage>,
        internal_error_sender: oneshot::Sender<ExecutionError>,
    ) -> Self {
        Self {
            data: ContractData::default(),
            system_api: ContractSystemApi::new(
                context,
                storage,
                queued_future_factory,
                internal_error_sender,
            ),
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
        &mut ContractSystemApi<'storage>,
        &mut WritableSystemTables<ContractSystemApi<'storage>>,
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
            system_api: ServiceSystemApi::new(context, storage),
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
        &mut ServiceSystemApi<'storage>,
        &mut QueryableSystemTables<ServiceSystemApi<'storage>>,
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

/// Helper type with common functionality across the contract and service system API
/// implementations.
struct SystemApi<S> {
    context: ContextForwarder,
    storage: S,
}

/// Implementation to forward contract system calls from the guest WASM module to the host
/// implementation.
pub struct ContractSystemApi<'storage> {
    shared: SystemApi<&'storage dyn WritableStorage>,
    queued_future_factory: QueuedHostFutureFactory<'storage>,
    internal_error_sender: Option<oneshot::Sender<ExecutionError>>,
}

impl<'storage> ContractSystemApi<'storage> {
    /// Creates a new [`ContractSystemApi`] instance using the provided asynchronous `context` and
    /// exporting the API from `storage`.
    pub fn new(
        context: ContextForwarder,
        storage: &'storage dyn WritableStorage,
        queued_future_factory: QueuedHostFutureFactory<'storage>,
        internal_error_sender: oneshot::Sender<ExecutionError>,
    ) -> Self {
        ContractSystemApi {
            shared: SystemApi { context, storage },
            queued_future_factory,
            internal_error_sender: Some(internal_error_sender),
        }
    }

    /// Returns the [`WritableStorage`] trait object instance to handle a system call.
    fn storage(&self) -> &'storage dyn WritableStorage {
        self.shared.storage
    }

    /// Returns the [`ContextForwarder`] to be used for asynchronous system calls.
    fn context(&mut self) -> &mut ContextForwarder {
        &mut self.shared.context
    }

    /// Reports an error to the [`GuestFuture`] responsible for executing the application.
    ///
    /// This causes the [`GuestFuture`] to return that error the next time it is polled.
    fn report_internal_error(&mut self, error: ExecutionError) {
        if let Some(sender) = self.internal_error_sender.take() {
            sender
                .send(error)
                .expect("Internal error receiver has unexpectedly been dropped");
        }
    }
}

impl_writable_system!(ContractSystemApi<'storage>);

/// Implementation to forward service system calls from the guest WASM module to the host
/// implementation.
pub struct ServiceSystemApi<'storage> {
    shared: SystemApi<&'storage dyn QueryableStorage>,
}

impl<'storage> ServiceSystemApi<'storage> {
    /// Creates a new [`ServiceSystemApi`] instance using the provided asynchronous `context` and
    /// exporting the API from `storage`.
    pub fn new(context: ContextForwarder, storage: &'storage dyn QueryableStorage) -> Self {
        ServiceSystemApi {
            shared: SystemApi { context, storage },
        }
    }

    /// Returns the [`QueryableStorage`] trait object instance to handle a system call.
    fn storage(&self) -> &'storage dyn QueryableStorage {
        self.shared.storage
    }

    /// Returns the [`ContextForwarder`] to be used for asynchronous system calls.
    fn context(&mut self) -> &mut ContextForwarder {
        &mut self.shared.context
    }
}

impl<'storage> QueryableSystem for ServiceSystemApi<'storage> {
    type Load = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;
    type Lock = HostFuture<'storage, Result<(), ExecutionError>>;
    type Unlock = HostFuture<'storage, Result<(), ExecutionError>>;
    type ReadKeyBytes = HostFuture<'storage, Result<Option<Vec<u8>>, ExecutionError>>;
    type FindKeys = HostFuture<'storage, Result<Vec<Vec<u8>>, ExecutionError>>;
    type FindKeyValues = HostFuture<'storage, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;
    type TryQueryApplication = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;

    fn chain_id(&mut self) -> queryable_system::ChainId {
        self.storage().chain_id().into()
    }

    fn application_id(&mut self) -> queryable_system::ApplicationId {
        self.storage().application_id().into()
    }

    fn application_parameters(&mut self) -> Vec<u8> {
        self.storage().application_parameters()
    }

    fn read_system_balance(&mut self) -> queryable_system::SystemBalance {
        self.storage().read_system_balance().into()
    }

    fn read_system_timestamp(&mut self) -> queryable_system::Timestamp {
        self.storage().read_system_timestamp().micros()
    }

    fn load_new(&mut self) -> Self::Load {
        HostFuture::new(self.storage().try_read_my_state())
    }

    fn load_poll(&mut self, future: &Self::Load) -> queryable_system::PollLoad {
        use queryable_system::PollLoad;
        match future.poll(self.context()) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn lock_new(&mut self) -> Self::Lock {
        HostFuture::new(self.storage().lock_view_user_state())
    }

    fn lock_poll(&mut self, future: &Self::Lock) -> queryable_system::PollLock {
        use queryable_system::PollLock;
        match future.poll(self.context()) {
            Poll::Pending => PollLock::Pending,
            Poll::Ready(Ok(())) => PollLock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollLock::Ready(Err(error.to_string())),
        }
    }

    fn unlock_new(&mut self) -> Self::Unlock {
        HostFuture::new(self.storage().unlock_view_user_state())
    }

    fn unlock_poll(&mut self, future: &Self::Lock) -> queryable_system::PollUnlock {
        use queryable_system::PollUnlock;
        match future.poll(self.context()) {
            Poll::Pending => PollUnlock::Pending,
            Poll::Ready(Ok(())) => PollUnlock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollUnlock::Ready(Err(error.to_string())),
        }
    }

    fn read_key_bytes_new(&mut self, key: &[u8]) -> Self::ReadKeyBytes {
        HostFuture::new(self.storage().read_key_bytes(key.to_owned()))
    }

    fn read_key_bytes_poll(
        &mut self,
        future: &Self::ReadKeyBytes,
    ) -> queryable_system::PollReadKeyBytes {
        use queryable_system::PollReadKeyBytes;
        match future.poll(self.context()) {
            Poll::Pending => PollReadKeyBytes::Pending,
            Poll::Ready(Ok(opt_list)) => PollReadKeyBytes::Ready(Ok(opt_list)),
            Poll::Ready(Err(error)) => PollReadKeyBytes::Ready(Err(error.to_string())),
        }
    }

    fn find_keys_new(&mut self, key_prefix: &[u8]) -> Self::FindKeys {
        HostFuture::new(self.storage().find_keys_by_prefix(key_prefix.to_owned()))
    }

    fn find_keys_poll(&mut self, future: &Self::FindKeys) -> queryable_system::PollFindKeys {
        use queryable_system::PollFindKeys;
        match future.poll(self.context()) {
            Poll::Pending => PollFindKeys::Pending,
            Poll::Ready(Ok(keys)) => PollFindKeys::Ready(Ok(keys)),
            Poll::Ready(Err(error)) => PollFindKeys::Ready(Err(error.to_string())),
        }
    }

    fn find_key_values_new(&mut self, key_prefix: &[u8]) -> Self::FindKeyValues {
        HostFuture::new(
            self.storage()
                .find_key_values_by_prefix(key_prefix.to_owned()),
        )
    }

    fn find_key_values_poll(
        &mut self,
        future: &Self::FindKeyValues,
    ) -> queryable_system::PollFindKeyValues {
        use queryable_system::PollFindKeyValues;
        match future.poll(self.context()) {
            Poll::Pending => PollFindKeyValues::Pending,
            Poll::Ready(Ok(key_values)) => PollFindKeyValues::Ready(Ok(key_values)),
            Poll::Ready(Err(error)) => PollFindKeyValues::Ready(Err(error.to_string())),
        }
    }

    fn try_query_application_new(
        &mut self,
        application: queryable_system::ApplicationId,
        argument: &[u8],
    ) -> Self::TryQueryApplication {
        let storage = self.storage();
        let argument = Vec::from(argument);

        HostFuture::new(async move {
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
        match future.poll(self.context()) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(result)) => PollLoad::Ready(Ok(result)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn log(&mut self, message: &str, level: queryable_system::LogLevel) {
        log::log!(level.into(), "{message}");
    }
}
