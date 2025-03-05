// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmtime](https://wasmtime.dev/) runtime.

use std::sync::LazyLock;

use linera_base::data_types::Bytecode;
use linera_witty::{wasmtime::EntrypointInstance, ExportTo, Instance};
use tokio::sync::Mutex;
use wasmtime::{AsContextMut, Config, Engine, Linker, Module, Store};

use super::{
    module_cache::ModuleCache,
    runtime_api::{BaseRuntimeApi, ContractRuntimeApi, RuntimeApiData, ServiceRuntimeApi},
    ContractEntrypoints, ServiceEntrypoints, WasmExecutionError,
};
use crate::{
    wasm::{WasmContractModule, WasmServiceModule},
    ContractRuntime, ExecutionError, FinalizeContext, MessageContext, OperationContext,
    QueryContext, ServiceRuntime,
};

/// An [`Engine`] instance configured to run application contracts.
static CONTRACT_ENGINE: LazyLock<Engine> = LazyLock::new(|| {
    let mut config = Config::default();
    config
        .consume_fuel(true)
        .cranelift_nan_canonicalization(true);

    Engine::new(&config).expect("Failed to create Wasmtime `Engine` for contracts")
});

/// An [`Engine`] instance configured to run application services.
static SERVICE_ENGINE: LazyLock<Engine> = LazyLock::new(Engine::default);

/// A cache of compiled contract modules.
static CONTRACT_CACHE: LazyLock<Mutex<ModuleCache<Module>>> = LazyLock::new(Mutex::default);

/// A cache of compiled service modules.
static SERVICE_CACHE: LazyLock<Mutex<ModuleCache<Module>>> = LazyLock::new(Mutex::default);

/// Type representing a running [Wasmtime](https://wasmtime.dev/) contract.
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub(crate) struct WasmtimeContractInstance<Runtime>
where
    Runtime: ContractRuntime + 'static,
{
    /// The Wasm module instance.
    instance: EntrypointInstance<RuntimeApiData<Runtime>>,

    /// The starting amount of fuel.
    initial_fuel: u64,
}

// TODO(#1967): Remove once fuel consumption is instrumented in the bytecode
impl<Runtime> WasmtimeContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    fn configure_initial_fuel(&mut self) -> Result<(), ExecutionError> {
        let runtime = &mut self.instance.user_data_mut().runtime_mut();
        let fuel = runtime.remaining_fuel()?;
        let mut context = self.instance.as_context_mut();

        self.initial_fuel = fuel;

        context
            .set_fuel(fuel)
            .expect("Fuel consumption should be enabled");

        Ok(())
    }

    fn persist_remaining_fuel(&mut self) -> Result<(), ExecutionError> {
        let remaining_fuel = self
            .instance
            .as_context_mut()
            .get_fuel()
            .expect("Failed to read remaining fuel");
        let runtime = &mut self.instance.user_data_mut().runtime_mut();

        assert!(self.initial_fuel >= remaining_fuel);

        runtime.consume_fuel(self.initial_fuel - remaining_fuel)
    }
}

/// Type representing a running [Wasmtime](https://wasmtime.dev/) service.
pub struct WasmtimeServiceInstance<Runtime> {
    /// The Wasm module instance.
    instance: EntrypointInstance<RuntimeApiData<Runtime>>,
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
    Runtime: ContractRuntime + 'static,
{
    /// Prepares a runtime instance to call into the Wasm contract.
    pub fn prepare(contract_module: &Module, runtime: Runtime) -> Result<Self, WasmExecutionError> {
        let mut linker = Linker::new(&CONTRACT_ENGINE);

        BaseRuntimeApi::export_to(&mut linker)?;
        ContractRuntimeApi::export_to(&mut linker)?;

        let user_data = RuntimeApiData::new(runtime);
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
    Runtime: ServiceRuntime + 'static,
{
    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare(service_module: &Module, runtime: Runtime) -> Result<Self, WasmExecutionError> {
        let mut linker = Linker::new(&SERVICE_ENGINE);

        BaseRuntimeApi::export_to(&mut linker)?;
        ServiceRuntimeApi::export_to(&mut linker)?;

        let user_data = RuntimeApiData::new(runtime);
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
    Runtime: ContractRuntime + 'static,
{
    fn instantiate(
        &mut self,
        _context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        self.configure_initial_fuel()?;
        let result = ContractEntrypoints::new(&mut self.instance).instantiate(argument);
        self.persist_remaining_fuel()?;
        result.map_err(WasmExecutionError::from)?;
        Ok(())
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result = ContractEntrypoints::new(&mut self.instance).execute_operation(operation);
        self.persist_remaining_fuel()?;
        Ok(result.map_err(WasmExecutionError::from)?)
    }

    fn execute_message(
        &mut self,
        _context: MessageContext,
        message: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        self.configure_initial_fuel()?;
        let result = ContractEntrypoints::new(&mut self.instance).execute_message(message);
        self.persist_remaining_fuel()?;
        result.map_err(WasmExecutionError::from)?;
        Ok(())
    }

    fn finalize(&mut self, _context: FinalizeContext) -> Result<(), ExecutionError> {
        self.configure_initial_fuel()?;
        let result = ContractEntrypoints::new(&mut self.instance).finalize();
        self.persist_remaining_fuel()?;
        result.map_err(WasmExecutionError::from)?;
        Ok(())
    }
}

impl<Runtime> crate::UserService for WasmtimeServiceInstance<Runtime>
where
    Runtime: ServiceRuntime + 'static,
{
    fn handle_query(
        &mut self,
        _context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        Ok(ServiceEntrypoints::new(&mut self.instance)
            .handle_query(argument)
            .map_err(WasmExecutionError::from)?)
    }
}
