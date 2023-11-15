// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmtime](https://wasmtime.dev/) runtime.

// Export the system interface used by a user contract.
wit_bindgen_host_wasmtime_rust::export!({
    custom_error: true,
    paths: ["contract_system_api.wit"],
});

// Export the system interface used by a user service.
wit_bindgen_host_wasmtime_rust::export!({
    custom_error: true,
    paths: ["service_system_api.wit"],
});

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

use self::{
    contract::ContractData, contract_system_api::ContractSystemApiTables, service::ServiceData,
    service_system_api::ServiceSystemApiTables, view_system_api::ViewSystemApiTables,
};
use super::{
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    module_cache::ModuleCache,
    runtime_actor::{BaseRequest, ContractRequest, SendRequestExt, ServiceRequest},
    WasmApplication, WasmExecutionError,
};
use crate::{
    ApplicationCallResult, Bytecode, CalleeContext, ExecutionError, MessageContext,
    OperationContext, QueryContext, RawExecutionResult, SessionCallResult, SessionId,
};
use futures::channel::mpsc;
use linera_views::batch::Batch;
use once_cell::sync::Lazy;
use std::error::Error;
use tokio::sync::Mutex;
use wasmtime::{Config, Engine, Linker, Module, Store, Trap};
use wit_bindgen_host_wasmtime_rust::Le;

/// An [`Engine`] instance configured to run application contracts.
static CONTRACT_ENGINE: Lazy<Engine> = Lazy::new(|| {
    let mut config = Config::default();
    config
        .consume_fuel(true)
        .cranelift_nan_canonicalization(true);

    Engine::new(&config).expect("Failed to create Wasmtime `Engine` for contracts")
});

/// An [`Engine`] instance configured to run application services.
static SERVICE_ENGINE: Lazy<Engine> = Lazy::new(Engine::default);

/// A cache of compiled contract modules.
static CONTRACT_CACHE: Lazy<Mutex<ModuleCache<Module>>> = Lazy::new(Mutex::default);

/// A cache of compiled service modules.
static SERVICE_CACHE: Lazy<Mutex<ModuleCache<Module>>> = Lazy::new(Mutex::default);

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for contracts.
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub struct Contract {
    contract: contract::Contract<ContractState>,
}

impl ApplicationRuntimeContext for Contract {
    type Store = Store<ContractState>;
    type Error = Trap;
    type Extra = ();

    fn configure_initial_fuel(context: &mut WasmRuntimeContext<Self>) {
        let runtime = &context.store.data().system_api.runtime;
        let fuel = runtime
            .send_request(|response_sender| ContractRequest::RemainingFuel { response_sender })
            .and_then(|response_receiver| {
                response_receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse)
            })
            .unwrap_or_else(|_| {
                tracing::debug!("Failed to read initial fuel for transaction");
                0
            });

        context
            .store
            .add_fuel(fuel)
            .expect("Fuel consumption wasn't properly enabled");
    }

    fn persist_remaining_fuel(context: &mut WasmRuntimeContext<Self>) -> Result<(), ()> {
        let runtime = &context.store.data().system_api.runtime;
        let initial_fuel = runtime
            .send_request(|response_sender| ContractRequest::RemainingFuel { response_sender })
            .and_then(|response_receiver| {
                response_receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse)
            })
            .map_err(|_| ())?;
        let consumed_fuel = context.store.fuel_consumed().ok_or(())?;
        let remaining_fuel = initial_fuel.saturating_sub(consumed_fuel);

        runtime
            .send_request(|response_sender| ContractRequest::SetRemainingFuel {
                remaining_fuel,
                response_sender,
            })
            .map_err(|_| ())?
            .recv()
            .map_err(|_| ())
    }
}

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for services.
pub struct Service {
    service: service::Service<ServiceState>,
}

impl ApplicationRuntimeContext for Service {
    type Store = Store<ServiceState>;
    type Error = Trap;
    type Extra = ();

    fn configure_initial_fuel(_context: &mut WasmRuntimeContext<Self>) {}

    fn persist_remaining_fuel(_context: &mut WasmRuntimeContext<Self>) -> Result<(), ()> {
        Ok(())
    }
}

impl WasmApplication {
    /// Creates a new [`WasmApplication`] using Wasmtime with the provided bytecodes.
    pub async fn new_with_wasmtime(
        contract_bytecode: Bytecode,
        service_bytecode: Bytecode,
    ) -> Result<Self, WasmExecutionError> {
        let mut contract_cache = CONTRACT_CACHE.lock().await;
        let contract = contract_cache
            .get_or_insert_with(contract_bytecode, |bytecode| {
                Module::new(&CONTRACT_ENGINE, bytecode)
            })
            .map_err(WasmExecutionError::LoadContractModule)?;

        let mut service_cache = SERVICE_CACHE.lock().await;
        let service = service_cache
            .get_or_insert_with(service_bytecode, |bytecode| {
                Module::new(&SERVICE_ENGINE, bytecode)
            })
            .map_err(WasmExecutionError::LoadServiceModule)?;

        Ok(WasmApplication::Wasmtime { contract, service })
    }

    /// Prepares a runtime instance to call into the Wasm contract.
    pub fn prepare_contract_runtime_with_wasmtime(
        contract_module: &Module,
        runtime: mpsc::UnboundedSender<ContractRequest>,
    ) -> Result<WasmRuntimeContext<Contract>, WasmExecutionError> {
        let mut linker = Linker::new(&CONTRACT_ENGINE);

        contract_system_api::add_to_linker(&mut linker, ContractState::system_api)
            .map_err(WasmExecutionError::LoadContractModule)?;
        view_system_api::add_to_linker(&mut linker, ContractState::views_api)
            .map_err(WasmExecutionError::LoadContractModule)?;

        let state = ContractState::new(runtime);
        let mut store = Store::new(&CONTRACT_ENGINE, state);
        let (contract, _instance) = contract::Contract::instantiate(
            &mut store,
            contract_module,
            &mut linker,
            ContractState::data,
        )
        .map_err(WasmExecutionError::LoadContractModule)?;
        let application = Contract { contract };

        Ok(WasmRuntimeContext {
            application,
            store,
            extra: (),
        })
    }

    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare_service_runtime_with_wasmtime(
        service_module: &Module,
        runtime: mpsc::UnboundedSender<ServiceRequest>,
    ) -> Result<WasmRuntimeContext<Service>, WasmExecutionError> {
        let mut linker = Linker::new(&SERVICE_ENGINE);

        service_system_api::add_to_linker(&mut linker, ServiceState::system_api)
            .map_err(WasmExecutionError::LoadServiceModule)?;
        view_system_api::add_to_linker(&mut linker, ServiceState::views_api)
            .map_err(WasmExecutionError::LoadServiceModule)?;

        let state = ServiceState::new(runtime);
        let mut store = Store::new(&SERVICE_ENGINE, state);
        let (service, _instance) = service::Service::instantiate(
            &mut store,
            service_module,
            &mut linker,
            ServiceState::data,
        )
        .map_err(WasmExecutionError::LoadServiceModule)?;
        let application = Service { service };

        Ok(WasmRuntimeContext {
            application,
            store,
            extra: (),
        })
    }
}

/// Data stored by the runtime that's necessary for handling calls to and from the Wasm module.
pub struct ContractState {
    data: ContractData,
    system_api: ContractSystemApi,
    system_tables: ContractSystemApiTables<ContractSystemApi>,
    views_tables: ViewSystemApiTables<ContractSystemApi>,
}

/// Data stored by the runtime that's necessary for handling queries to and from the Wasm module.
pub struct ServiceState {
    data: ServiceData,
    system_api: ServiceSystemApi,
    system_tables: ServiceSystemApiTables<ServiceSystemApi>,
    views_tables: ViewSystemApiTables<ServiceSystemApi>,
}

impl ContractState {
    /// Creates a new instance of [`ContractState`].
    ///
    /// Uses `runtime` to export the system API.
    pub fn new(runtime: mpsc::UnboundedSender<ContractRequest>) -> Self {
        Self {
            data: ContractData::default(),
            system_api: ContractSystemApi::new(runtime),
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
        &mut ContractSystemApi,
        &mut ContractSystemApiTables<ContractSystemApi>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }

    /// Obtains the data required by the runtime to export the views API.
    pub fn views_api(
        &mut self,
    ) -> (
        &mut ContractSystemApi,
        &mut ViewSystemApiTables<ContractSystemApi>,
    ) {
        (&mut self.system_api, &mut self.views_tables)
    }
}

impl ServiceState {
    /// Creates a new instance of [`ServiceState`].
    ///
    /// Uses `runtime` to export the system API.
    pub fn new(runtime: mpsc::UnboundedSender<ServiceRequest>) -> Self {
        Self {
            data: ServiceData::default(),
            system_api: ServiceSystemApi::new(runtime),
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
        &mut ServiceSystemApi,
        &mut ServiceSystemApiTables<ServiceSystemApi>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }

    /// Obtains the data required by the runtime to export the views API.
    pub fn views_api(
        &mut self,
    ) -> (
        &mut ServiceSystemApi,
        &mut ViewSystemApiTables<ServiceSystemApi>,
    ) {
        (&mut self.system_api, &mut self.views_tables)
    }
}

impl common::Contract for Contract {
    fn initialize(
        &self,
        store: &mut Store<ContractState>,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, Trap> {
        contract::Contract::initialize(&self.contract, store, context.into(), &argument)
            .map(|inner| inner.map(RawExecutionResult::from))
    }

    fn execute_operation(
        &self,
        store: &mut Store<ContractState>,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, Trap> {
        contract::Contract::execute_operation(&self.contract, store, context.into(), &operation)
            .map(|inner| inner.map(RawExecutionResult::from))
    }

    fn execute_message(
        &self,
        store: &mut Store<ContractState>,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, Trap> {
        contract::Contract::execute_message(&self.contract, store, context.into(), &message)
            .map(|inner| inner.map(RawExecutionResult::from))
    }

    fn handle_application_call(
        &self,
        store: &mut Store<ContractState>,
        context: CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<Result<ApplicationCallResult, String>, Trap> {
        let forwarded_sessions = forwarded_sessions
            .into_iter()
            .map(contract::SessionId::from)
            .collect::<Vec<_>>();

        contract::Contract::handle_application_call(
            &self.contract,
            store,
            context.into(),
            &argument,
            &forwarded_sessions,
        )
        .map(|inner| inner.map(ApplicationCallResult::from))
    }

    fn handle_session_call(
        &self,
        store: &mut Store<ContractState>,
        context: CalleeContext,
        session: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<Result<(SessionCallResult, Vec<u8>), String>, Trap> {
        let forwarded_sessions = forwarded_sessions
            .into_iter()
            .map(contract::SessionId::from)
            .collect::<Vec<_>>();

        contract::Contract::handle_session_call(
            &self.contract,
            store,
            context.into(),
            &session,
            &argument,
            &forwarded_sessions,
        )
        .map(|inner| inner.map(<(SessionCallResult, Vec<u8>)>::from))
    }
}

impl common::Service for Service {
    fn handle_query(
        &self,
        store: &mut Store<ServiceState>,
        context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Result<Vec<u8>, String>, Trap> {
        service::Service::handle_query(&self.service, store, context.into(), &argument)
    }
}

/// Implementation to forward contract system calls from the guest Wasm module to the host
/// implementation.
pub struct ContractSystemApi {
    runtime: mpsc::UnboundedSender<ContractRequest>,
}

impl ContractSystemApi {
    /// Creates a new [`ContractSystemApi`] instance exporting the API from `runtime`.
    pub fn new(runtime: mpsc::UnboundedSender<ContractRequest>) -> Self {
        ContractSystemApi { runtime }
    }
}

impl_contract_system_api!(ContractSystemApi, wasmtime::Trap);
impl_view_system_api_for_contract!(ContractSystemApi, wasmtime::Trap);

/// Implementation to forward service system calls from the guest Wasm module to the host
/// implementation.
pub struct ServiceSystemApi {
    runtime: mpsc::UnboundedSender<ServiceRequest>,
}

impl ServiceSystemApi {
    /// Creates a new [`ServiceSystemApi`] instance exporting the API from `runtime`.
    pub fn new(runtime: mpsc::UnboundedSender<ServiceRequest>) -> Self {
        ServiceSystemApi { runtime }
    }
}

impl_service_system_api!(ServiceSystemApi, wasmtime::Trap);
impl_view_system_api_for_service!(ServiceSystemApi, wasmtime::Trap);

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
