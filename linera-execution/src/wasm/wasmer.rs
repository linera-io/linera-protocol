// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

use std::marker::Unpin;

use bytes::Bytes;
use linera_base::sync::Lazy;
use linera_witty::{
    wasmer::{EntrypointInstance, InstanceBuilder},
    ExportTo,
};
use tokio::sync::Mutex;
use wasm_instrument::{gas_metering, parity_wasm};
use wasmer::{sys::EngineBuilder, Cranelift, Engine, Module, Singlepass, Store};

use super::{
    module_cache::ModuleCache,
    system_api::{ContractSystemApi, ServiceSystemApi, SystemApiData, ViewSystemApi, WriteBatch},
    ContractEntrypoints, ServiceEntrypoints, WasmExecutionError,
};
use crate::{
    wasm::{WasmContractModule, WasmServiceModule},
    Bytecode, ContractRuntime, ExecutionError, FinalizeContext, MessageContext, OperationContext,
    QueryContext, ServiceRuntime,
};

/// An [`Engine`] instance configured to run application services.
static SERVICE_ENGINE: Lazy<Engine> = Lazy::new(|| {
    let compiler_config = Cranelift::new();
    EngineBuilder::new(compiler_config).into()
});

/// A cache of compiled contract modules, with their respective [`Engine`] instances.
static CONTRACT_CACHE: Lazy<Mutex<ModuleCache<CachedContractModule>>> = Lazy::new(Mutex::default);

/// A cache of compiled service modules.
static SERVICE_CACHE: Lazy<Mutex<ModuleCache<Module>>> = Lazy::new(Mutex::default);

/// Type representing a running [Wasmer](https://wasmer.io/) contract.
pub(crate) struct WasmerContractInstance<Runtime> {
    /// The Wasmer instance.
    instance: EntrypointInstance<SystemApiData<Runtime>>,
}

/// Type representing a running [Wasmer](https://wasmer.io/) service.
pub struct WasmerServiceInstance<Runtime> {
    /// The Wasmer instance.
    instance: EntrypointInstance<SystemApiData<Runtime>>,
}

impl WasmContractModule {
    /// Creates a new [`WasmContractModule`] using Wasmer with the provided bytecodes.
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
    Runtime: ContractRuntime + WriteBatch + Clone + Send + Sync + Unpin + 'static,
{
    /// Prepares a runtime instance to call into the Wasm contract.
    pub fn prepare(
        contract_engine: Engine,
        contract_module: &Module,
        runtime: Runtime,
    ) -> Result<Self, WasmExecutionError> {
        let system_api_data = SystemApiData::new(runtime);
        let mut instance_builder = InstanceBuilder::new(contract_engine, system_api_data);

        ContractSystemApi::export_to(&mut instance_builder)?;
        ViewSystemApi::export_to(&mut instance_builder)?;

        let instance = instance_builder.instantiate(contract_module)?;

        Ok(Self { instance })
    }
}

impl WasmServiceModule {
    /// Creates a new [`WasmServiceModule`] using Wasmer with the provided bytecodes.
    pub async fn from_wasmer(service_bytecode: Bytecode) -> Result<Self, WasmExecutionError> {
        let mut service_cache = SERVICE_CACHE.lock().await;
        let module = service_cache
            .get_or_insert_with(service_bytecode, |bytecode| {
                Module::new(&*SERVICE_ENGINE, bytecode).map_err(anyhow::Error::from)
            })
            .map_err(WasmExecutionError::LoadServiceModule)?;
        Ok(WasmServiceModule::Wasmer { module })
    }
}

impl<Runtime> WasmerServiceInstance<Runtime>
where
    Runtime: ServiceRuntime + WriteBatch + Clone + Send + Sync + Unpin + 'static,
{
    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare(service_module: &Module, runtime: Runtime) -> Result<Self, WasmExecutionError> {
        let system_api_data = SystemApiData::new(runtime);
        let mut instance_builder = InstanceBuilder::new(SERVICE_ENGINE.clone(), system_api_data);

        ServiceSystemApi::export_to(&mut instance_builder)?;
        ViewSystemApi::export_to(&mut instance_builder)?;

        let instance = instance_builder.instantiate(service_module)?;

        Ok(Self { instance })
    }
}

impl<Runtime> crate::UserContract for WasmerContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Unpin + 'static,
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

impl<Runtime> crate::UserService for WasmerServiceInstance<Runtime>
where
    Runtime: Send + 'static,
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
pub struct CachedContractModule {
    compiled_bytecode: Bytes,
}

pub fn add_metering(bytecode: Bytecode) -> anyhow::Result<Bytecode> {
    struct WasmtimeRules;

    impl gas_metering::Rules for WasmtimeRules {
        /// Calculates the fuel cost of a WebAssembly [`Operator`].
        ///
        /// The rules try to follow the hardcoded [rules in the Wasmtime runtime
        /// engine](https://docs.rs/wasmtime/5.0.0/wasmtime/struct.Store.html#method.add_fuel).
        fn instruction_cost(
            &self,
            instruction: &parity_wasm::elements::Instruction,
        ) -> Option<u32> {
            use parity_wasm::elements::Instruction::*;

            Some(match instruction {
                Nop | Drop | Block(_) | Loop(_) | Unreachable | Else | End => 0,
                _ => 1,
            })
        }

        fn memory_grow_cost(&self) -> gas_metering::MemoryGrowCost {
            gas_metering::MemoryGrowCost::Free
        }

        fn call_per_local_cost(&self) -> u32 {
            0
        }
    }

    let instrumented_module = gas_metering::inject(
        parity_wasm::deserialize_buffer(&bytecode.bytes)?,
        gas_metering::host_function::Injector::new(
            "linera:app/contract-system-api",
            "consume-fuel",
        ),
        &WasmtimeRules,
    )
    .map_err(|_| anyhow::anyhow!("failed to instrument module"))?;

    Ok(Bytecode {
        bytes: instrumented_module.into_bytes()?,
    })
}

impl CachedContractModule {
    /// Creates a new [`CachedContractModule`] by compiling a `contract_bytecode`.
    pub fn new(contract_bytecode: Bytecode) -> Result<Self, anyhow::Error> {
        let module = Module::new(
            &Self::create_compilation_engine(),
            add_metering(contract_bytecode)?,
        )?;
        let compiled_bytecode = module.serialize()?;
        Ok(CachedContractModule { compiled_bytecode })
    }

    /// Creates a new [`Engine`] to compile a contract bytecode.
    fn create_compilation_engine() -> Engine {
        let mut compiler_config = Singlepass::default();
        compiler_config.canonicalize_nans(true);

        EngineBuilder::new(compiler_config).into()
    }

    /// Creates a [`Module`] from a compiled contract using a headless [`Engine`].
    pub fn create_execution_instance(&self) -> Result<(Engine, Module), anyhow::Error> {
        use wasmer::NativeEngineExt;

        let engine = Engine::headless();
        let store = Store::new(engine.clone());
        let module = unsafe { Module::deserialize(&store, &*self.compiled_bytecode) }?;
        Ok((engine, module))
    }
}
