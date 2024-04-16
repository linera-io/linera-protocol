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

#[path = "conversions_from_wit.rs"]
mod conversions_from_wit;
#[path = "conversions_to_wit.rs"]
mod conversions_to_wit;

use std::error::Error;

use linera_witty::{wasmtime::EntrypointInstance, ExportTo, Instance};
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use wasmtime::{AsContextMut, Config, Engine, Linker, Module, Store};

use super::{
    module_cache::ModuleCache,
    system_api::{ContractSystemApi, ServiceSystemApi, SystemApiData, ViewSystemApi},
    ContractEntrypoints, ServiceEntrypoints, WasmExecutionError,
};
use crate::{
    wasm::{WasmContractModule, WasmServiceModule},
    Bytecode, ContractRuntime, ExecutionError, FinalizeContext, MessageContext, OperationContext,
    QueryContext, ServiceRuntime,
};

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
    /// The Wasm module instance.
    instance: EntrypointInstance<SystemApiData<Runtime>>,

    /// The starting amount of fuel.
    initial_fuel: u64,
}

// TODO(#1785): Simplify by using proper fuel getter and setter methods from Wasmtime once the
// dependency is updated
impl<Runtime> WasmtimeContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Sync,
{
    fn configure_initial_fuel(&mut self) -> Result<(), ExecutionError> {
        let runtime = &mut self.instance.user_data_mut().runtime_mut();
        let fuel = runtime.remaining_fuel()?;
        let mut context = self.instance.as_context_mut();

        self.initial_fuel = fuel;

        context
            .add_fuel(1)
            .expect("Fuel consumption should be enabled");

        let existing_fuel = context
            .consume_fuel(0)
            .expect("Fuel consumption should be enabled");

        if existing_fuel > fuel {
            context
                .consume_fuel(existing_fuel - fuel)
                .expect("Existing fuel was incorrectly calculated");
        } else {
            context
                .add_fuel(fuel - existing_fuel)
                .expect("Fuel consumption wasn't properly enabled");
        }

        Ok(())
    }

    fn persist_remaining_fuel(&mut self) -> Result<(), ExecutionError> {
        let remaining_fuel = self
            .instance
            .as_context_mut()
            .consume_fuel(0)
            .expect("Failed to read remaining fuel");
        let runtime = &mut self.instance.user_data_mut().runtime_mut();

        assert!(self.initial_fuel >= remaining_fuel);

        runtime.consume_fuel(self.initial_fuel - remaining_fuel)
    }
}

/// Type representing a running [Wasmtime](https://wasmtime.dev/) service.
pub struct WasmtimeServiceInstance<Runtime> {
    /// The Wasm module instance.
    instance: EntrypointInstance<SystemApiData<Runtime>>,
}

impl WasmContractModule {
    /// Creates a new [`WasmContractModule`] using Wasmtime with the provided bytecodes.
    pub async fn from_wasmtime(contract_bytecode: Bytecode) -> Result<Self, WasmExecutionError> {
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

        ContractSystemApi::export_to(&mut linker)?;
        ViewSystemApi::export_to(&mut linker)?;

        let user_data = SystemApiData::new(runtime);
        let mut store = Store::new(&CONTRACT_ENGINE, user_data);
        let instance = linker
            .instantiate(&mut store, contract_module)
            .map_err(WasmExecutionError::LoadContractModule)?;

        Ok(Self {
            instance: EntrypointInstance::new(instance, store),
            initial_fuel: 0,
        })
    }
}

impl WasmServiceModule {
    /// Creates a new [`WasmServiceModule`] using Wasmtime with the provided bytecodes.
    pub async fn from_wasmtime(service_bytecode: Bytecode) -> Result<Self, WasmExecutionError> {
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

        ServiceSystemApi::export_to(&mut linker)?;
        ViewSystemApi::export_to(&mut linker)?;

        let user_data = SystemApiData::new(runtime);
        let mut store = Store::new(&SERVICE_ENGINE, user_data);
        let instance = linker
            .instantiate(&mut store, service_module)
            .map_err(WasmExecutionError::LoadServiceModule)?;

        Ok(Self {
            instance: EntrypointInstance::new(instance, store),
        })
    }
}

impl<Runtime> crate::UserContract for WasmtimeContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Sync + 'static,
{
    fn initialize(
        &mut self,
        _context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        self.configure_initial_fuel()?;
        let result = ContractEntrypoints::new(&mut self.instance).initialize(argument);
        self.persist_remaining_fuel()?;
        result
            .map_err(WasmExecutionError::from)?
            .map_err(ExecutionError::UserError)
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result = ContractEntrypoints::new(&mut self.instance).execute_operation(operation);
        self.persist_remaining_fuel()?;
        result
            .map_err(WasmExecutionError::from)?
            .map_err(ExecutionError::UserError)
    }

    fn execute_message(
        &mut self,
        _context: MessageContext,
        message: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        self.configure_initial_fuel()?;
        let result = ContractEntrypoints::new(&mut self.instance).execute_message(message);
        self.persist_remaining_fuel()?;
        result
            .map_err(WasmExecutionError::from)?
            .map_err(ExecutionError::UserError)
    }

    fn finalize(&mut self, _context: FinalizeContext) -> Result<(), ExecutionError> {
        self.configure_initial_fuel()?;
        let result = ContractEntrypoints::new(&mut self.instance).finalize();
        self.persist_remaining_fuel()?;
        result
            .map_err(WasmExecutionError::from)?
            .map_err(ExecutionError::UserError)
    }
}

impl<Runtime> crate::UserService for WasmtimeServiceInstance<Runtime>
where
    Runtime: ServiceRuntime + Send + Sync + 'static,
{
    fn handle_query(
        &mut self,
        _context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        ServiceEntrypoints::new(&mut self.instance)
            .handle_query(argument)
            .map_err(WasmExecutionError::from)?
            .map_err(ExecutionError::UserError)
    }
}

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
