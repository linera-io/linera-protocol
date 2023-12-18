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
use super::{module_cache::ModuleCache, WasmExecutionError};
use crate::{
    wasm::{WasmContractModule, WasmServiceModule},
    ApplicationCallResult, BaseRuntime, Bytecode, CalleeContext, ContractRuntime, ExecutionError,
    MessageContext, OperationContext, QueryContext, RawExecutionResult, ServiceRuntime,
    SessionCallResult, SessionId,
};
use once_cell::sync::Lazy;
use std::error::Error;
use tokio::sync::Mutex;
use wasmtime::{Config, Engine, Linker, Module, Store};
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

/// Type representing a running [Wasmtime](https://wasmtime.dev/) contract.
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub(crate) struct WasmtimeContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Sync + 'static,
{
    /// The application type.
    application: contract::Contract<ContractState<Runtime>>,

    /// The application's memory state.
    store: Store<ContractState<Runtime>>,
}

impl<Runtime> WasmtimeContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Sync,
{
    fn configure_initial_fuel(&mut self) -> Result<(), ExecutionError> {
        let runtime = &mut self.store.data_mut().runtime;
        let fuel = runtime.remaining_fuel()?;

        self.store
            .add_fuel(fuel)
            .expect("Fuel consumption wasn't properly enabled");

        Ok(())
    }

    fn persist_remaining_fuel(&mut self) -> Result<(), ExecutionError> {
        let consumed_fuel = self
            .store
            .fuel_consumed()
            .expect("Failed to read consumed fuel");
        let runtime = &mut self.store.data_mut().runtime;
        let initial_fuel = runtime.remaining_fuel()?;
        let remaining_fuel = initial_fuel.saturating_sub(consumed_fuel);

        runtime.set_remaining_fuel(remaining_fuel)
    }
}

/// Type representing a running [Wasmtime](https://wasmtime.dev/) service.
pub struct WasmtimeServiceInstance<Runtime>
where
    Runtime: ServiceRuntime + Send + Sync + 'static,
{
    /// The application type.
    application: service::Service<ServiceState<Runtime>>,

    /// The application's memory state.
    store: Store<ServiceState<Runtime>>,
}

impl WasmContractModule {
    /// Creates a new [`WasmContractModule`] using Wasmtime with the provided bytecodes.
    pub async fn new_with_wasmtime(
        contract_bytecode: Bytecode,
    ) -> Result<Self, WasmExecutionError> {
        let mut contract_cache = CONTRACT_CACHE.lock().await;
        let module = contract_cache
            .get_or_insert_with(contract_bytecode, |bytecode| {
                Module::new(&CONTRACT_ENGINE, bytecode)
            })
            .map_err(WasmExecutionError::LoadContractModule)?;
        Ok(WasmContractModule::Wasmtime { module })
    }
}

impl<Runtime> WasmtimeContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Sync + 'static,
{
    /// Prepares a runtime instance to call into the Wasm contract.
    pub fn prepare(contract_module: &Module, runtime: Runtime) -> Result<Self, WasmExecutionError> {
        let mut linker = Linker::new(&CONTRACT_ENGINE);

        contract_system_api::add_to_linker(&mut linker, ContractState::system_api)
            .map_err(WasmExecutionError::LoadContractModule)?;
        view_system_api::add_to_linker(&mut linker, ContractState::views_api)
            .map_err(WasmExecutionError::LoadContractModule)?;

        let state = ContractState::new(runtime);
        let mut store = Store::new(&CONTRACT_ENGINE, state);
        let (application, _instance) = contract::Contract::instantiate(
            &mut store,
            contract_module,
            &mut linker,
            ContractState::data,
        )
        .map_err(WasmExecutionError::LoadContractModule)?;

        Ok(Self { application, store })
    }
}

impl WasmServiceModule {
    /// Creates a new [`WasmServiceModule`] using Wasmtime with the provided bytecodes.
    pub async fn new_with_wasmtime(service_bytecode: Bytecode) -> Result<Self, WasmExecutionError> {
        let mut service_cache = SERVICE_CACHE.lock().await;
        let module = service_cache
            .get_or_insert_with(service_bytecode, |bytecode| {
                Module::new(&SERVICE_ENGINE, bytecode)
            })
            .map_err(WasmExecutionError::LoadServiceModule)?;
        Ok(WasmServiceModule::Wasmtime { module })
    }
}

impl<Runtime> WasmtimeServiceInstance<Runtime>
where
    Runtime: ServiceRuntime + Send + Sync + 'static,
{
    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare(service_module: &Module, runtime: Runtime) -> Result<Self, WasmExecutionError> {
        let mut linker = Linker::new(&SERVICE_ENGINE);

        service_system_api::add_to_linker(&mut linker, ServiceState::system_api)
            .map_err(WasmExecutionError::LoadServiceModule)?;
        view_system_api::add_to_linker(&mut linker, ServiceState::views_api)
            .map_err(WasmExecutionError::LoadServiceModule)?;

        let state = ServiceState::new(runtime);
        let mut store = Store::new(&SERVICE_ENGINE, state);
        let (application, _instance) = service::Service::instantiate(
            &mut store,
            service_module,
            &mut linker,
            ServiceState::data,
        )
        .map_err(WasmExecutionError::LoadServiceModule)?;

        Ok(Self { application, store })
    }
}

/// Data stored by the runtime that's necessary for handling calls to and from the Wasm module.
pub struct ContractState<Runtime>
where
    Runtime: ContractRuntime + Send + Sync + 'static,
{
    data: ContractData,
    runtime: Runtime,
    system_tables: ContractSystemApiTables<Runtime>,
    views_tables: ViewSystemApiTables<Runtime>,
}

/// Data stored by the runtime that's necessary for handling queries to and from the Wasm module.
pub struct ServiceState<Runtime>
where
    Runtime: ServiceRuntime + Send + Sync + 'static,
{
    data: ServiceData,
    runtime: Runtime,
    system_tables: ServiceSystemApiTables<Runtime>,
    views_tables: ViewSystemApiTables<Runtime>,
}

impl<Runtime> ContractState<Runtime>
where
    Runtime: ContractRuntime + Send + Sync + 'static,
{
    /// Creates a new instance of [`ContractState`].
    ///
    /// Uses `runtime` to export the system API.
    pub fn new(runtime: Runtime) -> Self {
        Self {
            data: ContractData::default(),
            runtime,
            system_tables: ContractSystemApiTables::default(),
            views_tables: ViewSystemApiTables::default(),
        }
    }

    /// Obtains the runtime instance specific [`ContractData`].
    pub fn data(&mut self) -> &mut ContractData {
        &mut self.data
    }

    /// Obtains the data required by the runtime to export the system API.
    pub fn system_api(&mut self) -> (&mut Runtime, &mut ContractSystemApiTables<Runtime>) {
        (&mut self.runtime, &mut self.system_tables)
    }

    /// Obtains the data required by the runtime to export the views API.
    pub fn views_api(&mut self) -> (&mut Runtime, &mut ViewSystemApiTables<Runtime>) {
        (&mut self.runtime, &mut self.views_tables)
    }
}

impl<Runtime> ServiceState<Runtime>
where
    Runtime: ServiceRuntime + Send + Sync + 'static,
{
    /// Creates a new instance of [`ServiceState`].
    ///
    /// Uses `runtime` to export the system API.
    pub fn new(runtime: Runtime) -> Self {
        Self {
            data: ServiceData::default(),
            runtime,
            system_tables: ServiceSystemApiTables::default(),
            views_tables: ViewSystemApiTables::default(),
        }
    }

    /// Obtains the runtime instance specific [`ServiceData`].
    pub fn data(&mut self) -> &mut ServiceData {
        &mut self.data
    }

    /// Obtains the data required by the runtime to export the system API.
    pub fn system_api(&mut self) -> (&mut Runtime, &mut ServiceSystemApiTables<Runtime>) {
        (&mut self.runtime, &mut self.system_tables)
    }

    /// Obtains the data required by the runtime to export the views API.
    pub fn views_api(&mut self) -> (&mut Runtime, &mut ViewSystemApiTables<Runtime>) {
        (&mut self.runtime, &mut self.views_tables)
    }
}

impl<Runtime> crate::UserContract for WasmtimeContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Sync + 'static,
{
    fn initialize(
        &mut self,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result = contract::Contract::initialize(
            &self.application,
            &mut self.store,
            context.into(),
            &argument,
        )
        .map(|inner| inner.map(RawExecutionResult::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }

    fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result = contract::Contract::execute_operation(
            &self.application,
            &mut self.store,
            context.into(),
            &operation,
        )
        .map(|inner| inner.map(RawExecutionResult::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }

    fn execute_message(
        &mut self,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result = contract::Contract::execute_message(
            &self.application,
            &mut self.store,
            context.into(),
            &message,
        )
        .map(|inner| inner.map(RawExecutionResult::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }

    fn handle_application_call(
        &mut self,
        context: CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        let forwarded_sessions = forwarded_sessions
            .into_iter()
            .map(contract::SessionId::from)
            .collect::<Vec<_>>();

        self.configure_initial_fuel()?;
        let result = contract::Contract::handle_application_call(
            &self.application,
            &mut self.store,
            context.into(),
            &argument,
            &forwarded_sessions,
        )
        .map(|inner| inner.map(ApplicationCallResult::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }

    fn handle_session_call(
        &mut self,
        context: CalleeContext,
        session: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<(SessionCallResult, Vec<u8>), ExecutionError> {
        let forwarded_sessions = forwarded_sessions
            .into_iter()
            .map(contract::SessionId::from)
            .collect::<Vec<_>>();

        self.configure_initial_fuel()?;
        let result = contract::Contract::handle_session_call(
            &self.application,
            &mut self.store,
            context.into(),
            &session,
            &argument,
            &forwarded_sessions,
        )
        .map(|inner| inner.map(<(SessionCallResult, Vec<u8>)>::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }
}

impl<Runtime> crate::UserService for WasmtimeServiceInstance<Runtime>
where
    Runtime: ServiceRuntime + Send + Sync + 'static,
{
    fn handle_query(
        &mut self,
        context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        service::Service::handle_query(
            &self.application,
            &mut self.store,
            context.into(),
            &argument,
        )?
        .map_err(ExecutionError::UserError)
    }
}

impl_contract_system_api!(wasmtime::Trap);
impl_service_system_api!(wasmtime::Trap);
impl_view_system_api!(wasmtime::Trap);

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
