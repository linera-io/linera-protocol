// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmtime](https://wasmtime.dev/) runtime.

// Export the system interface used by a user contract.
wit_bindgen_host_wasmtime_rust::export!({
    custom_error: true,
    paths: ["../linera-sdk/writable_system.wit"],
});

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
    async_boundary::{HostFuture, WakerForwarder},
    async_determinism::{HostFutureQueue, QueuedHostFutureFactory},
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};
use crate::{ContractRuntime, ExecutionError, ServiceRuntime, SessionId};
use linera_views::{batch::Batch, views::ViewError};
use std::{error::Error, task::Poll};
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
        storage: &'storage dyn ContractRuntime,
    ) -> Result<WasmRuntimeContext<'storage, Contract<'storage>>, WasmExecutionError> {
        let mut config = Config::default();
        config
            .consume_fuel(true)
            .cranelift_nan_canonicalization(true);

        let engine = Engine::new(&config).map_err(WasmExecutionError::CreateWasmtimeEngine)?;
        let mut linker = Linker::new(&engine);

        writable_system::add_to_linker(&mut linker, ContractState::system_api)?;

        let module = Module::new(&engine, &self.contract_bytecode)?;
        let waker_forwarder = WakerForwarder::default();
        let (future_queue, queued_future_factory) = HostFutureQueue::new();
        let state = ContractState::new(storage, waker_forwarder.clone(), queued_future_factory);
        let mut store = Store::new(&engine, state);
        let (contract, _instance) =
            contract::Contract::instantiate(&mut store, &module, &mut linker, ContractState::data)?;
        let application = Contract { contract };

        store
            .add_fuel(storage.remaining_fuel())
            .expect("Fuel consumption wasn't properly enabled");

        Ok(WasmRuntimeContext {
            waker_forwarder,
            application,
            future_queue,
            store,
            extra: (),
        })
    }

    /// Prepare a runtime instance to call into the WASM service.
    pub fn prepare_service_runtime_with_wasmtime<'storage>(
        &self,
        storage: &'storage dyn ServiceRuntime,
    ) -> Result<WasmRuntimeContext<'storage, Service<'storage>>, WasmExecutionError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        queryable_system::add_to_linker(&mut linker, ServiceState::system_api)?;

        let module = Module::new(&engine, &self.service_bytecode)?;
        let waker_forwarder = WakerForwarder::default();
        let (future_queue, _queued_future_factory) = HostFutureQueue::new();
        let state = ServiceState::new(storage, waker_forwarder.clone());
        let mut store = Store::new(&engine, state);
        let (service, _instance) =
            service::Service::instantiate(&mut store, &module, &mut linker, ServiceState::data)?;
        let application = Service { service };

        Ok(WasmRuntimeContext {
            waker_forwarder,
            application,
            future_queue,
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
    /// Uses `storage` to export the system API, and the `waker` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(
        storage: &'storage dyn ContractRuntime,
        waker: WakerForwarder,
        queued_future_factory: QueuedHostFutureFactory<'storage>,
    ) -> Self {
        Self {
            data: ContractData::default(),
            system_api: ContractSystemApi::new(waker, storage, queued_future_factory),
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
    /// Uses `storage` to export the system API, and the `waker` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(storage: &'storage dyn ServiceRuntime, waker: WakerForwarder) -> Self {
        Self {
            data: ServiceData::default(),
            system_api: ServiceSystemApi::new(waker, storage),
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
    type HandleApplicationCall = contract::HandleApplicationCall;
    type HandleSessionCall = contract::HandleSessionCall;
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

    fn handle_application_call_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::HandleApplicationCall, Trap> {
        contract::Contract::handle_application_call_new(
            &self.contract,
            store,
            context,
            argument,
            forwarded_sessions,
        )
    }

    fn handle_application_call_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::HandleApplicationCall,
    ) -> Result<contract::PollCallApplication, Trap> {
        contract::Contract::handle_application_call_poll(&self.contract, store, future)
    }

    fn handle_session_call_new(
        &self,
        store: &mut Store<ContractState<'storage>>,
        context: contract::CalleeContext,
        session: contract::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::HandleSessionCall, Trap> {
        contract::Contract::handle_session_call_new(
            &self.contract,
            store,
            context,
            session,
            argument,
            forwarded_sessions,
        )
    }

    fn handle_session_call_poll(
        &self,
        store: &mut Store<ContractState<'storage>>,
        future: &contract::HandleSessionCall,
    ) -> Result<contract::PollCallSession, Trap> {
        contract::Contract::handle_session_call_poll(&self.contract, store, future)
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
    waker: WakerForwarder,
    storage: S,
}

/// Implementation to forward contract system calls from the guest WASM module to the host
/// implementation.
pub struct ContractSystemApi<'storage> {
    shared: SystemApi<&'storage dyn ContractRuntime>,
    queued_future_factory: QueuedHostFutureFactory<'storage>,
}

impl<'storage> ContractSystemApi<'storage> {
    /// Creates a new [`ContractSystemApi`] instance using the provided asynchronous `waker` and
    /// exporting the API from `storage`.
    pub fn new(
        waker: WakerForwarder,
        storage: &'storage dyn ContractRuntime,
        queued_future_factory: QueuedHostFutureFactory<'storage>,
    ) -> Self {
        ContractSystemApi {
            shared: SystemApi { waker, storage },
            queued_future_factory,
        }
    }

    /// Returns the [`ContractRuntime`] trait object instance to handle a system call.
    fn storage(&self) -> &'storage dyn ContractRuntime {
        self.shared.storage
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn waker(&mut self) -> &mut WakerForwarder {
        &mut self.shared.waker
    }
}

impl_writable_system!(ContractSystemApi<'storage>);

/// Implementation to forward service system calls from the guest WASM module to the host
/// implementation.
pub struct ServiceSystemApi<'storage> {
    shared: SystemApi<&'storage dyn ServiceRuntime>,
}

impl<'storage> ServiceSystemApi<'storage> {
    /// Creates a new [`ServiceSystemApi`] instance using the provided asynchronous `waker` and
    /// exporting the API from `storage`.
    pub fn new(waker: WakerForwarder, storage: &'storage dyn ServiceRuntime) -> Self {
        ServiceSystemApi {
            shared: SystemApi { waker, storage },
        }
    }

    /// Returns the [`ServiceRuntime`] trait object instance to handle a system call.
    fn storage(&self) -> &'storage dyn ServiceRuntime {
        self.shared.storage
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn waker(&mut self) -> &mut WakerForwarder {
        &mut self.shared.waker
    }
}

impl_queryable_system!(ServiceSystemApi<'storage>);

impl From<ExecutionError> for wasmtime::Trap {
    fn from(error: ExecutionError) -> Self {
        match error {
            ExecutionError::UserError(message) => wasmtime::Trap::new(message),
            _ => {
                let boxed_error: Box<dyn Error + Send + Sync + 'static> = Box::new(error);
                wasmtime::Trap::from(boxed_error)
            }
        }
    }
}

impl From<wasmtime::Trap> for ExecutionError {
    fn from(trap: wasmtime::Trap) -> Self {
        if trap.trap_code().is_none() {
            ExecutionError::UserError(trap.display_reason().to_string())
        } else {
            ExecutionError::WasmError(WasmExecutionError::ExecuteModuleInWasmtime(trap))
        }
    }
}
