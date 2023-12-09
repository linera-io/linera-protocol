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
    WasmContract, WasmExecutionError, WasmService,
};
use crate::{
    ApplicationCallResult, Bytecode, CalleeContext, ContractRuntimeSender, ExecutionError,
    MessageContext, OperationContext, QueryContext, RawExecutionResult, ServiceRuntimeSender,
    SessionCallResult, SessionId,
};
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

    fn configure_initial_fuel(context: &mut WasmRuntimeContext<Self>) -> Result<(), Self::Error> {
        let runtime_sender = &context.store.data().system_api;
        let fuel = runtime_sender.remaining_fuel()?;

        context
            .store
            .add_fuel(fuel)
            .expect("Fuel consumption wasn't properly enabled");

        Ok(())
    }

    fn persist_remaining_fuel(context: &mut WasmRuntimeContext<Self>) -> Result<(), Self::Error> {
        let runtime_sender = &context.store.data().system_api;
        let initial_fuel = runtime_sender.remaining_fuel()?;
        let consumed_fuel = context
            .store
            .fuel_consumed()
            .expect("Failed to read consumed fuel");
        let remaining_fuel = initial_fuel.saturating_sub(consumed_fuel);

        runtime_sender.set_remaining_fuel(remaining_fuel)?;

        Ok(())
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

    fn configure_initial_fuel(_context: &mut WasmRuntimeContext<Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn persist_remaining_fuel(_context: &mut WasmRuntimeContext<Self>) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl WasmContract {
    /// Creates a new [`WasmContract`] using Wasmtime with the provided bytecodes.
    pub async fn new_with_wasmtime(
        contract_bytecode: Bytecode,
    ) -> Result<Self, WasmExecutionError> {
        let mut contract_cache = CONTRACT_CACHE.lock().await;
        let module = contract_cache
            .get_or_insert_with(contract_bytecode, |bytecode| {
                Module::new(&CONTRACT_ENGINE, bytecode)
            })
            .map_err(WasmExecutionError::LoadContractModule)?;

        Ok(WasmContract::Wasmtime { module })
    }

    /// Prepares a runtime instance to call into the Wasm contract.
    pub fn prepare_contract_runtime_with_wasmtime(
        contract_module: &Module,
        runtime: ContractRuntimeSender,
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
}

impl WasmService {
    /// Creates a new [`WasmService`] using Wasmtime with the provided bytecodes.
    pub async fn new_with_wasmtime(service_bytecode: Bytecode) -> Result<Self, WasmExecutionError> {
        let mut service_cache = SERVICE_CACHE.lock().await;
        let module = service_cache
            .get_or_insert_with(service_bytecode, |bytecode| {
                Module::new(&SERVICE_ENGINE, bytecode)
            })
            .map_err(WasmExecutionError::LoadServiceModule)?;

        Ok(WasmService::Wasmtime { module })
    }

    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare_service_runtime_with_wasmtime(
        service_module: &Module,
        runtime: ServiceRuntimeSender,
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
    system_api: ContractRuntimeSender,
    system_tables: ContractSystemApiTables<ContractRuntimeSender>,
    views_tables: ViewSystemApiTables<ContractRuntimeSender>,
}

/// Data stored by the runtime that's necessary for handling queries to and from the Wasm module.
pub struct ServiceState {
    data: ServiceData,
    system_api: ServiceRuntimeSender,
    system_tables: ServiceSystemApiTables<ServiceRuntimeSender>,
    views_tables: ViewSystemApiTables<ServiceRuntimeSender>,
}

impl ContractState {
    /// Creates a new instance of [`ContractState`].
    ///
    /// Uses `runtime` to export the system API.
    pub fn new(system_api: ContractRuntimeSender) -> Self {
        Self {
            data: ContractData::default(),
            system_api,
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
        &mut ContractRuntimeSender,
        &mut ContractSystemApiTables<ContractRuntimeSender>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }

    /// Obtains the data required by the runtime to export the views API.
    pub fn views_api(
        &mut self,
    ) -> (
        &mut ContractRuntimeSender,
        &mut ViewSystemApiTables<ContractRuntimeSender>,
    ) {
        (&mut self.system_api, &mut self.views_tables)
    }
}

impl ServiceState {
    /// Creates a new instance of [`ServiceState`].
    ///
    /// Uses `runtime` to export the system API.
    pub fn new(system_api: ServiceRuntimeSender) -> Self {
        Self {
            data: ServiceData::default(),
            system_api,
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
        &mut ServiceRuntimeSender,
        &mut ServiceSystemApiTables<ServiceRuntimeSender>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }

    /// Obtains the data required by the runtime to export the views API.
    pub fn views_api(
        &mut self,
    ) -> (
        &mut ServiceRuntimeSender,
        &mut ViewSystemApiTables<ServiceRuntimeSender>,
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

impl_contract_system_api!(ContractRuntimeSender, wasmtime::Trap);
impl_view_system_api_for_contract!(ContractRuntimeSender, wasmtime::Trap);
impl_service_system_api!(ServiceRuntimeSender, wasmtime::Trap);
impl_view_system_api_for_service!(ServiceRuntimeSender, wasmtime::Trap);

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
