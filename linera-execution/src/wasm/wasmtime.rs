// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmtime](https://wasmtime.dev/) runtime.

use std::sync::LazyLock;

use linera_base::data_types::Bytecode;
use linera_witty::{wasmtime::EntrypointInstance, ExportTo};
use tokio::sync::Mutex;
use wasmtime::{Config, Engine, Linker, Module, Store};

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
    config.cranelift_nan_canonicalization(true);

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
}

/// Type representing a running [Wasmtime](https://wasmtime.dev/) service.
pub struct WasmtimeServiceInstance<Runtime> {
    /// The Wasm module instance.
    instance: EntrypointInstance<RuntimeApiData<Runtime>>,
}

impl WasmContractModule {
    /// Creates a new [`WasmContractModule`] using Wasmtime with the provided bytecode files.
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
        })
    }
}

impl WasmServiceModule {
    /// Creates a new [`WasmServiceModule`] using Wasmtime with the provided bytecode files.
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
        ContractEntrypoints::new(&mut self.instance)
            .instantiate(argument)
            .map_err(WasmExecutionError::from)?;
        Ok(())
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let result = ContractEntrypoints::new(&mut self.instance)
            .execute_operation(operation)
            .map_err(WasmExecutionError::from)?;
        Ok(result)
    }

    fn execute_message(
        &mut self,
        _context: MessageContext,
        message: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        ContractEntrypoints::new(&mut self.instance)
            .execute_message(message)
            .map_err(WasmExecutionError::from)?;
        Ok(())
    }

    fn finalize(&mut self, _context: FinalizeContext) -> Result<(), ExecutionError> {
        ContractEntrypoints::new(&mut self.instance)
            .finalize()
            .map_err(WasmExecutionError::from)?;
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
