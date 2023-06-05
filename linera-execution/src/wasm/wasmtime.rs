// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmtime](https://wasmtime.dev/) runtime.

// Export the system interface used by a user contract.
wit_bindgen_host_wasmtime_rust::export!({
    custom_error: true,
    paths: ["contract_system_api.wit"],
});

// Export the system interface used by a user service.
wit_bindgen_host_wasmtime_rust::export!("service_system_api.wit");

// Export the system interface used by views.
wit_bindgen_host_wasmtime_rust::export!({
    custom_error: true,
    paths: ["view_system_api.wit"],
});

// Import the interface implemented by a user contract.
wit_bindgen_host_wasmtime_rust::import!("contract.wit");

// Import the interface implemented by a user service.
wit_bindgen_host_wasmtime_rust::import!("service.wit");

#[path = "conversions_from_wit.rs"]
mod conversions_from_wit;
#[path = "conversions_to_wit.rs"]
mod conversions_to_wit;
#[path = "guest_futures.rs"]
mod guest_futures;

use self::{
    contract::ContractData, contract_system_api::ContractSystemApiTables, service::ServiceData,
    service_system_api::ServiceSystemApiTables, view_system_api::ViewSystemApiTables,
};
use super::{
    async_boundary::{HostFuture, WakerForwarder},
    async_determinism::{HostFutureQueue, QueuedHostFutureFactory},
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};
use crate::{Bytecode, ContractRuntime, ExecutionError, ServiceRuntime, SessionId};
use linera_views::{batch::Batch, views::ViewError};
use std::{error::Error, future::Future, task::Poll};
use wasmtime::{Config, Engine, Linker, Module, Store, Trap};
use wit_bindgen_host_wasmtime_rust::Le;

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for contracts.
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub struct Contract<'runtime> {
    contract: contract::Contract<ContractState<'runtime>>,
}

impl<'runtime> ApplicationRuntimeContext for Contract<'runtime> {
    type Store = Store<ContractState<'runtime>>;
    type Error = Trap;
    type Extra = ();

    fn finalize(context: &mut WasmRuntimeContext<Self>) {
        let runtime = context.store.data().system_api.runtime();
        let initial_fuel = runtime.remaining_fuel();
        let remaining_fuel = initial_fuel - context.store.fuel_consumed().unwrap_or(0);

        runtime.set_remaining_fuel(remaining_fuel);
    }
}

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for services.
pub struct Service<'runtime> {
    service: service::Service<ServiceState<'runtime>>,
}

impl<'runtime> ApplicationRuntimeContext for Service<'runtime> {
    type Store = Store<ServiceState<'runtime>>;
    type Error = Trap;
    type Extra = ();
}

impl WasmApplication {
    /// Creates a new [`WasmApplication`] using Wasmtime with the provided bytecodes.
    pub fn new_with_wasmtime(
        contract_bytecode: Bytecode,
        service_bytecode: Bytecode,
    ) -> Result<Self, WasmExecutionError> {
        let contract_engine = Self::create_wasmtime_engine_for_contracts()?;
        let contract_module = Module::new(&contract_engine, contract_bytecode)?;

        let service_engine = Self::create_wasmtime_engine_for_services()?;
        let service_module = Module::new(&service_engine, service_bytecode)?;

        Ok(WasmApplication::Wasmtime {
            contract_engine,
            contract_module,
            service_module,
            service_engine,
        })
    }

    /// Creates an [`Engine`] instance configured to run application contracts.
    fn create_wasmtime_engine_for_contracts() -> Result<Engine, WasmExecutionError> {
        let mut config = Config::default();
        config
            .consume_fuel(true)
            .cranelift_nan_canonicalization(true);

        Engine::new(&config).map_err(WasmExecutionError::CreateWasmtimeEngine)
    }

    /// Creates an [`Engine`] instance configured to run application services.
    fn create_wasmtime_engine_for_services() -> Result<Engine, WasmExecutionError> {
        Ok(Engine::default())
    }

    /// Prepares a runtime instance to call into the WASM contract.
    pub fn prepare_contract_runtime_with_wasmtime<'runtime>(
        contract_engine: &Engine,
        contract_module: &Module,
        runtime: &'runtime dyn ContractRuntime,
    ) -> Result<WasmRuntimeContext<'runtime, Contract<'runtime>>, WasmExecutionError> {
        let mut linker = Linker::new(contract_engine);

        contract_system_api::add_to_linker(&mut linker, ContractState::system_api)?;
        view_system_api::add_to_linker(&mut linker, ContractState::views_api)?;

        let waker_forwarder = WakerForwarder::default();
        let (future_queue, queued_future_factory) = HostFutureQueue::new();
        let state = ContractState::new(runtime, waker_forwarder.clone(), queued_future_factory);
        let mut store = Store::new(contract_engine, state);
        let (contract, _instance) = contract::Contract::instantiate(
            &mut store,
            contract_module,
            &mut linker,
            ContractState::data,
        )?;
        let application = Contract { contract };

        store
            .add_fuel(runtime.remaining_fuel())
            .expect("Fuel consumption wasn't properly enabled");

        Ok(WasmRuntimeContext {
            waker_forwarder,
            application,
            future_queue,
            store,
            extra: (),
        })
    }

    /// Prepares a runtime instance to call into the WASM service.
    pub fn prepare_service_runtime_with_wasmtime<'runtime>(
        service_engine: &Engine,
        service_module: &Module,
        runtime: &'runtime dyn ServiceRuntime,
    ) -> Result<WasmRuntimeContext<'runtime, Service<'runtime>>, WasmExecutionError> {
        let mut linker = Linker::new(service_engine);

        service_system_api::add_to_linker(&mut linker, ServiceState::system_api)?;
        view_system_api::add_to_linker(&mut linker, ServiceState::views_api)?;

        let waker_forwarder = WakerForwarder::default();
        let (future_queue, _queued_future_factory) = HostFutureQueue::new();
        let state = ServiceState::new(runtime, waker_forwarder.clone());
        let mut store = Store::new(service_engine, state);
        let (service, _instance) = service::Service::instantiate(
            &mut store,
            service_module,
            &mut linker,
            ServiceState::data,
        )?;
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
pub struct ContractState<'runtime> {
    data: ContractData,
    system_api: ContractSystemApi<'runtime>,
    system_tables: ContractSystemApiTables<ContractSystemApi<'runtime>>,
    views_tables: ViewSystemApiTables<ContractSystemApi<'runtime>>,
}

/// Data stored by the runtime that's necessary for handling queries to and from the WASM module.
pub struct ServiceState<'runtime> {
    data: ServiceData,
    system_api: ServiceSystemApi<'runtime>,
    system_tables: ServiceSystemApiTables<ServiceSystemApi<'runtime>>,
    views_tables: ViewSystemApiTables<ServiceSystemApi<'runtime>>,
}

impl<'runtime> ContractState<'runtime> {
    /// Creates a new instance of [`ContractState`].
    ///
    /// Uses `runtime` to export the system API, and the `waker` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(
        runtime: &'runtime dyn ContractRuntime,
        waker: WakerForwarder,
        queued_future_factory: QueuedHostFutureFactory<'runtime>,
    ) -> Self {
        Self {
            data: ContractData::default(),
            system_api: ContractSystemApi::new(waker, runtime, queued_future_factory),
            system_tables: ContractSystemApiTables::default(),
            views_tables: ViewSystemApiTables::default(),
        }
    }

    /// Obtains the runtime instance specific [`ContractData`].
    pub fn data(&mut self) -> &mut ContractData {
        &mut self.data
    }

    /// Obtains the data required by the runtime to export the system API.
    pub fn system_api(
        &mut self,
    ) -> (
        &mut ContractSystemApi<'runtime>,
        &mut ContractSystemApiTables<ContractSystemApi<'runtime>>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }

    /// Obtains the data required by the runtime to export the views API.
    pub fn views_api(
        &mut self,
    ) -> (
        &mut ContractSystemApi<'runtime>,
        &mut ViewSystemApiTables<ContractSystemApi<'runtime>>,
    ) {
        (&mut self.system_api, &mut self.views_tables)
    }
}

impl<'runtime> ServiceState<'runtime> {
    /// Creates a new instance of [`ServiceState`].
    ///
    /// Uses `runtime` to export the system API, and the `waker` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(runtime: &'runtime dyn ServiceRuntime, waker: WakerForwarder) -> Self {
        Self {
            data: ServiceData::default(),
            system_api: ServiceSystemApi::new(waker, runtime),
            system_tables: ServiceSystemApiTables::default(),
            views_tables: ViewSystemApiTables::default(),
        }
    }

    /// Obtains the runtime instance specific [`ServiceData`].
    pub fn data(&mut self) -> &mut ServiceData {
        &mut self.data
    }

    /// Obtains the data required by the runtime to export the system API.
    pub fn system_api(
        &mut self,
    ) -> (
        &mut ServiceSystemApi<'runtime>,
        &mut ServiceSystemApiTables<ServiceSystemApi<'runtime>>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }

    /// Obtains the data required by the runtime to export the views API.
    pub fn views_api(
        &mut self,
    ) -> (
        &mut ServiceSystemApi<'runtime>,
        &mut ViewSystemApiTables<ServiceSystemApi<'runtime>>,
    ) {
        (&mut self.system_api, &mut self.views_tables)
    }
}

impl<'runtime> common::Contract for Contract<'runtime> {
    type Initialize = contract::Initialize;
    type ExecuteOperation = contract::ExecuteOperation;
    type ExecuteMessage = contract::ExecuteMessage;
    type HandleApplicationCall = contract::HandleApplicationCall;
    type HandleSessionCall = contract::HandleSessionCall;
    type OperationContext = contract::OperationContext;
    type MessageContext = contract::MessageContext;
    type CalleeContext = contract::CalleeContext;
    type SessionId = contract::SessionId;
    type PollExecutionResult = contract::PollExecutionResult;
    type PollCallApplication = contract::PollCallApplication;
    type PollCallSession = contract::PollCallSession;

    fn initialize_new(
        &self,
        store: &mut Store<ContractState<'runtime>>,
        context: contract::OperationContext,
        argument: &[u8],
    ) -> Result<contract::Initialize, Trap> {
        contract::Contract::initialize_new(&self.contract, store, context, argument)
    }

    fn initialize_poll(
        &self,
        store: &mut Store<ContractState<'runtime>>,
        future: &contract::Initialize,
    ) -> Result<contract::PollExecutionResult, Trap> {
        contract::Contract::initialize_poll(&self.contract, store, future)
    }

    fn execute_operation_new(
        &self,
        store: &mut Store<ContractState<'runtime>>,
        context: contract::OperationContext,
        operation: &[u8],
    ) -> Result<contract::ExecuteOperation, Trap> {
        contract::Contract::execute_operation_new(&self.contract, store, context, operation)
    }

    fn execute_operation_poll(
        &self,
        store: &mut Store<ContractState<'runtime>>,
        future: &contract::ExecuteOperation,
    ) -> Result<contract::PollExecutionResult, Trap> {
        contract::Contract::execute_operation_poll(&self.contract, store, future)
    }

    fn execute_message_new(
        &self,
        store: &mut Store<ContractState<'runtime>>,
        context: contract::MessageContext,
        message: &[u8],
    ) -> Result<contract::ExecuteMessage, Trap> {
        contract::Contract::execute_message_new(&self.contract, store, context, message)
    }

    fn execute_message_poll(
        &self,
        store: &mut Store<ContractState<'runtime>>,
        future: &contract::ExecuteMessage,
    ) -> Result<contract::PollExecutionResult, Trap> {
        contract::Contract::execute_message_poll(&self.contract, store, future)
    }

    fn handle_application_call_new(
        &self,
        store: &mut Store<ContractState<'runtime>>,
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
        store: &mut Store<ContractState<'runtime>>,
        future: &contract::HandleApplicationCall,
    ) -> Result<contract::PollCallApplication, Trap> {
        contract::Contract::handle_application_call_poll(&self.contract, store, future)
    }

    fn handle_session_call_new(
        &self,
        store: &mut Store<ContractState<'runtime>>,
        context: contract::CalleeContext,
        session: &[u8],
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
        store: &mut Store<ContractState<'runtime>>,
        future: &contract::HandleSessionCall,
    ) -> Result<contract::PollCallSession, Trap> {
        contract::Contract::handle_session_call_poll(&self.contract, store, future)
    }
}

impl<'runtime> common::Service for Service<'runtime> {
    type QueryApplication = service::QueryApplication;
    type QueryContext = service::QueryContext;
    type PollQuery = service::PollQuery;

    fn query_application_new(
        &self,
        store: &mut Store<ServiceState<'runtime>>,
        context: service::QueryContext,
        argument: &[u8],
    ) -> Result<service::QueryApplication, Trap> {
        service::Service::query_application_new(&self.service, store, context, argument)
    }

    fn query_application_poll(
        &self,
        store: &mut Store<ServiceState<'runtime>>,
        future: &service::QueryApplication,
    ) -> Result<service::PollQuery, Trap> {
        service::Service::query_application_poll(&self.service, store, future)
    }
}

/// Helper type with common functionality across the contract and service system API
/// implementations.
struct SystemApi<S> {
    waker: WakerForwarder,
    runtime: S,
}

/// Implementation to forward contract system calls from the guest WASM module to the host
/// implementation.
pub struct ContractSystemApi<'runtime> {
    shared: SystemApi<&'runtime dyn ContractRuntime>,
    queued_future_factory: QueuedHostFutureFactory<'runtime>,
}

impl<'runtime> ContractSystemApi<'runtime> {
    /// Creates a new [`ContractSystemApi`] instance using the provided asynchronous `waker` and
    /// exporting the API from `runtime`.
    pub fn new(
        waker: WakerForwarder,
        runtime: &'runtime dyn ContractRuntime,
        queued_future_factory: QueuedHostFutureFactory<'runtime>,
    ) -> Self {
        ContractSystemApi {
            shared: SystemApi { waker, runtime },
            queued_future_factory,
        }
    }

    /// Returns the [`ContractRuntime`] trait object instance to handle a system call.
    fn runtime(&self) -> &'runtime dyn ContractRuntime {
        self.shared.runtime
    }

    /// Same as [`Self::runtime`].
    fn runtime_with_writable_storage(
        &self,
    ) -> Result<&'runtime dyn ContractRuntime, ExecutionError> {
        Ok(self.runtime())
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn waker(&mut self) -> &mut WakerForwarder {
        &mut self.shared.waker
    }

    /// Enqueues a `future` to be executed deterministically.
    fn new_host_future<Output>(
        &mut self,
        future: impl Future<Output = Output> + Send + 'runtime,
    ) -> HostFuture<'runtime, Output>
    where
        Output: Send + 'static,
    {
        self.queued_future_factory.enqueue(future)
    }
}

impl_contract_system_api!(ContractSystemApi<'runtime>);
impl_view_system_api!(ContractSystemApi<'runtime>);

/// Implementation to forward service system calls from the guest WASM module to the host
/// implementation.
pub struct ServiceSystemApi<'runtime> {
    shared: SystemApi<&'runtime dyn ServiceRuntime>,
}

impl<'runtime> ServiceSystemApi<'runtime> {
    /// Creates a new [`ServiceSystemApi`] instance using the provided asynchronous `waker` and
    /// exporting the API from `runtime`.
    pub fn new(waker: WakerForwarder, runtime: &'runtime dyn ServiceRuntime) -> Self {
        ServiceSystemApi {
            shared: SystemApi { waker, runtime },
        }
    }

    /// Returns the [`ServiceRuntime`] trait object instance to handle a system call.
    fn runtime(&self) -> &'runtime dyn ServiceRuntime {
        self.shared.runtime
    }

    /// Returns an error due to an attempt to use a contract system API from a service.
    fn runtime_with_writable_storage(
        &self,
    ) -> Result<&'runtime dyn ContractRuntime, ExecutionError> {
        Err(WasmExecutionError::WriteAttemptToReadOnlyStorage.into())
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn waker(&mut self) -> &mut WakerForwarder {
        &mut self.shared.waker
    }

    /// Enqueues a `future` to be executed deterministically.
    fn new_host_future<Output>(
        &mut self,
        future: impl Future<Output = Output> + Send + 'runtime,
    ) -> HostFuture<'runtime, Output> {
        HostFuture::new(future)
    }
}

impl_service_system_api!(ServiceSystemApi<'runtime>);
impl_view_system_api!(ServiceSystemApi<'runtime>);

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
