// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

use std::{marker::Unpin, sync::Arc};

use bytes::Bytes;
use linera_base::sync::Lazy;
use linera_witty::{
    wasmer::{EntrypointInstance, InstanceBuilder},
    ExportTo, Instance,
};
use tokio::sync::Mutex;
use wasmer::{
    wasmparser::Operator, CompilerConfig, Cranelift, Engine, EngineBuilder, Module, Singlepass,
    Store,
};
use wasmer_middlewares::metering::{self, Metering, MeteringPoints};

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

    /// The starting amount of fuel.
    initial_fuel: u64,
}

impl<Runtime> WasmerContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Unpin,
{
    fn configure_initial_fuel(&mut self) -> Result<(), ExecutionError> {
        self.initial_fuel = self
            .instance
            .user_data_mut()
            .runtime_mut()
            .remaining_fuel()?;

        let (store, mut instance_guard) = self.instance.as_store_and_instance_mut();
        let instance = instance_guard
            .as_mut()
            .expect("Uninitialized `wasmer::Instance` inside an `EntrypointInstance`");

        metering::set_remaining_points(store, instance, self.initial_fuel);

        Ok(())
    }

    fn persist_remaining_fuel(&mut self) -> Result<(), ExecutionError> {
        let remaining_fuel = {
            let (store, mut instance_guard) = self.instance.as_store_and_instance_mut();
            let instance = instance_guard
                .as_mut()
                .expect("Uninitialized `wasmer::Instance` inside an `EntrypointInstance`");

            match metering::get_remaining_points(store, instance) {
                MeteringPoints::Exhausted => 0,
                MeteringPoints::Remaining(fuel) => fuel,
            }
        };

        assert!(self.initial_fuel >= remaining_fuel);

        self.instance
            .user_data_mut()
            .runtime_mut()
            .consume_fuel(self.initial_fuel - remaining_fuel)
    }
}

/// Type representing a running [Wasmer](https://wasmer.io/) service.
pub struct WasmerServiceInstance<Runtime> {
    /// The application type.
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
    Runtime: ContractRuntime + Clone + Send + Sync + Unpin + 'static,
{
    /// Prepares a runtime instance to call into the Wasm contract.
    pub fn prepare(
        contract_engine: &Engine,
        contract_module: &Module,
        runtime: Runtime,
    ) -> Result<Self, WasmExecutionError> {
        let system_api_data = SystemApiData::new(runtime);
        let mut instance_builder = InstanceBuilder::new(contract_engine, system_api_data);

        ContractSystemApi::export_to(&mut instance_builder)?;
        ViewSystemApi::export_to(&mut instance_builder)?;

        let instance = instance_builder.instantiate(contract_module)?;

        Ok(Self {
            instance,
            initial_fuel: 0,
        })
    }
}

impl WasmContractModule {
    /// Calculates the fuel cost of a WebAssembly [`Operator`].
    ///
    /// The rules try to follow the hardcoded [rules in the Wasmtime runtime
    /// engine](https://docs.rs/wasmtime/5.0.0/wasmtime/struct.Store.html#method.add_fuel).
    fn operation_cost(operator: &Operator) -> u64 {
        match operator {
            Operator::Nop
            | Operator::Drop
            | Operator::Block { .. }
            | Operator::Loop { .. }
            | Operator::Unreachable
            | Operator::Else
            | Operator::End => 0,
            _ => 1,
        }
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
    Runtime: ServiceRuntime + Clone + Send + Sync + Unpin + 'static,
{
    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare(service_module: &Module, runtime: Runtime) -> Result<Self, WasmExecutionError> {
        let system_api_data = SystemApiData::new(runtime);
        let mut instance_builder = InstanceBuilder::new(&SERVICE_ENGINE, system_api_data);

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

impl<Runtime> crate::UserService for WasmerServiceInstance<Runtime>
where
    Runtime: Send + 'static,
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
///
/// Each [`Module`] needs to be compiled with a separate [`Engine`] instance, otherwise Wasmer
/// [panics](https://docs.rs/wasmer-middlewares/3.3.0/wasmer_middlewares/metering/struct.Metering.html#panic)
/// because fuel metering is configured and used across different modules.
pub struct CachedContractModule {
    compiled_bytecode: Bytes,
}

impl CachedContractModule {
    /// Creates a new [`CachedContractModule`] by compiling a `contract_bytecode`.
    pub fn new(contract_bytecode: Bytecode) -> Result<Self, anyhow::Error> {
        let module = Module::new(&Self::create_compilation_engine(), contract_bytecode)?;
        let compiled_bytecode = module.serialize()?;
        Ok(CachedContractModule { compiled_bytecode })
    }

    /// Creates a new [`Engine`] to compile a contract bytecode.
    fn create_compilation_engine() -> Engine {
        let metering = Arc::new(Metering::new(0, WasmContractModule::operation_cost));
        let mut compiler_config = Singlepass::default();
        compiler_config.push_middleware(metering);
        compiler_config.canonicalize_nans(true);

        EngineBuilder::new(compiler_config).into()
    }

    /// Creates a [`Module`] from a compiled contract using a headless [`Engine`].
    pub fn create_execution_instance(&self) -> Result<(Engine, Module), anyhow::Error> {
        let engine = Engine::headless();
        let store = Store::new(&engine);
        let module = unsafe { Module::deserialize(&store, &*self.compiled_bytecode) }?;
        Ok((engine, module))
    }
}
