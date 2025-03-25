// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

use std::{marker::Unpin, sync::LazyLock};

use linera_base::data_types::Bytecode;
use linera_witty::{
    wasmer::{EntrypointInstance, InstanceBuilder},
    ExportTo,
};
use tokio::sync::Mutex;

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

/// An [`Engine`] instance configured to run application services.
static SERVICE_ENGINE: LazyLock<wasmer::Engine> = LazyLock::new(|| {
    #[cfg(web)]
    {
        wasmer::Engine::default()
    }

    #[cfg(not(web))]
    {
        wasmer::sys::EngineBuilder::new(wasmer::Cranelift::new()).into()
    }
});

/// A cache of compiled contract modules, with their respective [`wasmer::Engine`] instances.
static CONTRACT_CACHE: LazyLock<Mutex<ModuleCache<CachedContractModule>>> =
    LazyLock::new(Mutex::default);

/// A cache of compiled service modules.
static SERVICE_CACHE: LazyLock<Mutex<ModuleCache<wasmer::Module>>> = LazyLock::new(Mutex::default);

/// Type representing a running [Wasmer](https://wasmer.io/) contract.
pub(crate) struct WasmerContractInstance<Runtime> {
    /// The Wasmer instance.
    instance: EntrypointInstance<RuntimeApiData<Runtime>>,
}

/// Type representing a running [Wasmer](https://wasmer.io/) service.
pub struct WasmerServiceInstance<Runtime> {
    /// The Wasmer instance.
    instance: EntrypointInstance<RuntimeApiData<Runtime>>,
}

impl WasmContractModule {
    /// Creates a new [`WasmContractModule`] using Wasmer with the provided bytecode files.
    pub async fn from_wasmer(contract_bytecode: Bytecode) -> Result<Self, WasmExecutionError> {
        let mut contract_cache = CONTRACT_CACHE.lock().await;
        let (engine, module) = contract_cache
            .get_or_insert_with(contract_bytecode, CachedContractModule::new)
            .map_err(WasmExecutionError::LoadContractModule)?
            .create_execution_instance()
            .map_err(WasmExecutionError::LoadContractModule)?;
        Ok(WasmContractModule::Wasmer { engine, module })
    }
}

impl<Runtime> WasmerContractInstance<Runtime>
where
    Runtime: ContractRuntime + Clone + Unpin + 'static,
{
    /// Prepares a runtime instance to call into the Wasm contract.
    pub fn prepare(
        contract_engine: wasmer::Engine,
        contract_module: &wasmer::Module,
        runtime: Runtime,
    ) -> Result<Self, WasmExecutionError> {
        let system_api_data = RuntimeApiData::new(runtime);
        let mut instance_builder = InstanceBuilder::new(contract_engine, system_api_data);

        BaseRuntimeApi::export_to(&mut instance_builder)?;
        ContractRuntimeApi::export_to(&mut instance_builder)?;

        let instance = instance_builder.instantiate(contract_module)?;

        Ok(Self { instance })
    }
}

impl WasmServiceModule {
    /// Creates a new [`WasmServiceModule`] using Wasmer with the provided bytecode files.
    pub async fn from_wasmer(service_bytecode: Bytecode) -> Result<Self, WasmExecutionError> {
        let mut service_cache = SERVICE_CACHE.lock().await;
        let module = service_cache
            .get_or_insert_with(service_bytecode, |bytecode| {
                wasmer::Module::new(&*SERVICE_ENGINE, bytecode).map_err(anyhow::Error::from)
            })
            .map_err(WasmExecutionError::LoadServiceModule)?;
        Ok(WasmServiceModule::Wasmer { module })
    }
}

impl<Runtime> WasmerServiceInstance<Runtime>
where
    Runtime: ServiceRuntime + Clone + Unpin + 'static,
{
    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare(
        service_module: &wasmer::Module,
        runtime: Runtime,
    ) -> Result<Self, WasmExecutionError> {
        let system_api_data = RuntimeApiData::new(runtime);
        let mut instance_builder = InstanceBuilder::new(SERVICE_ENGINE.clone(), system_api_data);

        BaseRuntimeApi::export_to(&mut instance_builder)?;
        ServiceRuntimeApi::export_to(&mut instance_builder)?;

        let instance = instance_builder.instantiate(service_module)?;

        Ok(Self { instance })
    }
}

impl<Runtime> crate::UserContract for WasmerContractInstance<Runtime>
where
    Runtime: ContractRuntime + Unpin + 'static,
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
        Ok(ContractEntrypoints::new(&mut self.instance)
            .execute_operation(operation)
            .map_err(WasmExecutionError::from)?)
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

impl<Runtime: 'static> crate::UserService for WasmerServiceInstance<Runtime> {
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

impl From<ExecutionError> for wasmer::RuntimeError {
    fn from(error: ExecutionError) -> Self {
        wasmer::RuntimeError::user(Box::new(error))
    }
}

impl From<wasmer::RuntimeError> for ExecutionError {
    fn from(error: wasmer::RuntimeError) -> Self {
        error
            .downcast::<ExecutionError>()
            .unwrap_or_else(|unknown_error| {
                ExecutionError::WasmError(WasmExecutionError::ExecuteModuleInWasmer(unknown_error))
            })
    }
}

/// Serialized bytes of a compiled contract bytecode.
// Cloning `Module`s is cheap.
#[derive(Clone)]
pub struct CachedContractModule(wasmer::Module);

impl CachedContractModule {
    /// Creates a new [`CachedContractModule`] by compiling a `contract_bytecode`.
    pub fn new(contract_bytecode: Bytecode) -> Result<Self, anyhow::Error> {
        let module = wasmer::Module::new(&Self::create_compilation_engine(), contract_bytecode)?;
        Ok(CachedContractModule(module))
    }

    /// Creates a new [`Engine`] to compile a contract bytecode.
    fn create_compilation_engine() -> wasmer::Engine {
        #[cfg(not(web))]
        {
            let mut compiler_config = wasmer_compiler_singlepass::Singlepass::default();
            compiler_config.canonicalize_nans(true);

            wasmer::sys::EngineBuilder::new(compiler_config).into()
        }

        #[cfg(web)]
        wasmer::Engine::default()
    }

    /// Creates a [`Module`] from a compiled contract using a headless [`Engine`].
    pub fn create_execution_instance(
        &self,
    ) -> Result<(wasmer::Engine, wasmer::Module), anyhow::Error> {
        #[cfg(web)]
        {
            Ok((wasmer::Engine::default(), self.0.clone()))
        }

        #[cfg(not(web))]
        {
            let engine = wasmer::Engine::default();
            let store = wasmer::Store::new(engine.clone());
            let bytes = self.0.serialize()?;
            let module = unsafe { wasmer::Module::deserialize(&store, bytes) }?;
            Ok((engine, module))
        }
    }
}
