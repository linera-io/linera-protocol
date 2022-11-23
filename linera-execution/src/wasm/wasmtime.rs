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

use self::{
    contract::{Contract, ContractData},
    queryable_system::{QueryableSystem, QueryableSystemTables},
    service::{Service, ServiceData},
    writable_system::{WritableSystem, WritableSystemTables},
};
use super::{
    async_boundary::{ContextForwarder, HostFuture},
    common::{self, Runtime, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};
use crate::{ExecutionError, QueryableStorage, WritableStorage};
use std::{marker::PhantomData, task::Poll};
use wasmtime::{Engine, Linker, Module, Store, Trap};

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for contracts.
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub struct ContractWasmtime<'storage> {
    _lifetime: PhantomData<&'storage ()>,
}

impl<'storage> Runtime for ContractWasmtime<'storage> {
    type Application = Contract<ContractState<'storage>>;
    type Store = Store<ContractState<'storage>>;
    type StorageGuard = ();
    type Error = Trap;
}

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for services.
pub struct ServiceWasmtime<'storage> {
    _lifetime: PhantomData<&'storage ()>,
}

impl<'storage> Runtime for ServiceWasmtime<'storage> {
    type Application = Service<ServiceState<'storage>>;
    type Store = Store<ServiceState<'storage>>;
    type StorageGuard = ();
    type Error = Trap;
}

impl WasmApplication {
    /// Prepare a runtime instance to call into the WASM contract.
    pub fn prepare_contract_runtime<'storage>(
        &self,
        storage: &'storage dyn WritableStorage,
    ) -> Result<WasmRuntimeContext<ContractWasmtime<'storage>>, WasmExecutionError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        writable_system::add_to_linker(&mut linker, ContractState::system_api)?;

        let module = Module::new(&engine, &self.contract_bytecode)?;
        let context_forwarder = ContextForwarder::default();
        let state = ContractState::new(storage, context_forwarder.clone());
        let mut store = Store::new(&engine, state);
        let (application, _instance) =
            Contract::instantiate(&mut store, &module, &mut linker, ContractState::data)?;

        Ok(WasmRuntimeContext {
            context_forwarder,
            application,
            store,
            _storage_guard: (),
        })
    }

    /// Prepare a runtime instance to call into the WASM service.
    pub fn prepare_service_runtime<'storage>(
        &self,
        storage: &'storage dyn QueryableStorage,
    ) -> Result<WasmRuntimeContext<ServiceWasmtime<'storage>>, WasmExecutionError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        queryable_system::add_to_linker(&mut linker, ServiceState::system_api)?;

        let module = Module::new(&engine, &self.service_bytecode)?;
        let context_forwarder = ContextForwarder::default();
        let state = ServiceState::new(storage, context_forwarder.clone());
        let mut store = Store::new(&engine, state);
        let (application, _instance) =
            Service::instantiate(&mut store, &module, &mut linker, ServiceState::data)?;

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
            system_api: SystemApi { storage, context },
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
            system_api: SystemApi { storage, context },
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

impl<'storage> common::Contract<ContractWasmtime<'storage>> for Contract<ContractState<'storage>> {
    fn initialize_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::OperationContext,
        argument: &[u8],
    ) -> Result<contract::Initialize, Trap> {
        Contract::initialize_new(self, store, context, argument)
    }

    fn initialize_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::Initialize,
    ) -> Result<contract::PollExecutionResult, Trap> {
        Contract::initialize_poll(self, store, future)
    }

    fn execute_operation_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::OperationContext,
        operation: &[u8],
    ) -> Result<contract::ExecuteOperation, Trap> {
        Contract::execute_operation_new(self, store, context, operation)
    }

    fn execute_operation_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::ExecuteOperation,
    ) -> Result<contract::PollExecutionResult, Trap> {
        Contract::execute_operation_poll(self, store, future)
    }

    fn execute_effect_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::EffectContext,
        effect: &[u8],
    ) -> Result<contract::ExecuteEffect, Trap> {
        Contract::execute_effect_new(self, store, context, effect)
    }

    fn execute_effect_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::ExecuteEffect,
    ) -> Result<contract::PollExecutionResult, Trap> {
        Contract::execute_effect_poll(self, store, future)
    }

    fn call_application_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallApplication, Trap> {
        Contract::call_application_new(self, store, context, argument, forwarded_sessions)
    }

    fn call_application_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::CallApplication,
    ) -> Result<contract::PollCallApplication, Trap> {
        Contract::call_application_poll(self, store, future)
    }

    fn call_session_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::CalleeContext,
        session: contract::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallSession, Trap> {
        Contract::call_session_new(self, store, context, session, argument, forwarded_sessions)
    }

    fn call_session_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::CallSession,
    ) -> Result<contract::PollCallSession, Trap> {
        Contract::call_session_poll(self, store, future)
    }
}

impl<'storage> common::Service<ServiceWasmtime<'storage>> for Service<ServiceState<'storage>> {
    fn query_application_new(
        &self,
        store: &mut Store<ServiceState<'storage>>,
        context: service::QueryContext,
        argument: &[u8],
    ) -> Result<service::QueryApplication, Trap> {
        Service::query_application_new(self, store, context, argument)
    }

    fn query_application_poll(
        &self,
        store: &mut Store<ServiceState<'storage>>,
        future: &service::QueryApplication,
    ) -> Result<service::PollQuery, Trap> {
        Service::query_application_poll(self, store, future)
    }
}

/// Implementation to forward system calls from the guest WASM module to the host implementation.
pub struct SystemApi<S> {
    context: ContextForwarder,
    storage: S,
}

impl<'storage> WritableSystem for SystemApi<&'storage dyn WritableStorage> {
    type Load = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;
    type LoadAndLock = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;

    fn load_new(&mut self) -> Self::Load {
        HostFuture::new(self.storage.try_read_my_state())
    }

    fn load_poll(&mut self, future: &Self::Load) -> writable_system::PollLoad {
        use writable_system::PollLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn load_and_lock_new(&mut self) -> Self::LoadAndLock {
        HostFuture::new(self.storage.try_read_and_lock_my_state())
    }

    fn load_and_lock_poll(&mut self, future: &Self::LoadAndLock) -> writable_system::PollLoad {
        use writable_system::PollLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn store_and_unlock(&mut self, state: &[u8]) -> bool {
        self.storage
            .save_and_unlock_my_state(state.to_owned())
            .is_ok()
    }
}

impl<'storage> QueryableSystem for SystemApi<&'storage dyn QueryableStorage> {
    type Load = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;

    fn load_new(&mut self) -> Self::Load {
        HostFuture::new(self.storage.try_read_my_state())
    }

    fn load_poll(&mut self, future: &Self::Load) -> queryable_system::PollLoad {
        use queryable_system::PollLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }
}
