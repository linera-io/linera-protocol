// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

// Export the system interface used by a user contract.
wit_bindgen_host_wasmer_rust::export!({
    custom_error: true,
    paths: ["contract_system_api.wit"],
});

// Export the system interface used by a user service.
wit_bindgen_host_wasmer_rust::export!({
    custom_error: true,
    paths: ["service_system_api.wit"],
});

// Export the system interface used by views.
wit_bindgen_host_wasmer_rust::export!({
    custom_error: true,
    paths: ["view_system_api.wit"],
});

// Import the interface implemented by a user contract.
wit_bindgen_host_wasmer_rust::import!("contract.wit");

// Import the interface implemented by a user service.
wit_bindgen_host_wasmer_rust::import!("service.wit");

#[path = "conversions_from_wit.rs"]
mod conversions_from_wit;
#[path = "conversions_to_wit.rs"]
mod conversions_to_wit;

use std::{marker::Unpin, sync::Arc};

use bytes::Bytes;
use linera_base::sync::Lazy;
use tokio::sync::Mutex;
use wasmer::{
    imports, wasmparser::Operator, CompilerConfig, Engine, EngineBuilder, Instance, Module,
    Singlepass, Store,
};
use wasmer_middlewares::metering::{self, Metering, MeteringPoints};

use super::{module_cache::ModuleCache, WasmExecutionError};
use crate::{
    wasm::{WasmContractModule, WasmServiceModule},
    ApplicationCallOutcome, BaseRuntime, Bytecode, CalleeContext, ContractRuntime, ExecutionError,
    FinalizeContext, MessageContext, OperationContext, QueryContext, RawExecutionOutcome,
    ServiceRuntime,
};

/// An [`Engine`] instance configured to run application services.
static SERVICE_ENGINE: Lazy<Engine> = Lazy::new(|| {
    let compiler_config = Singlepass::default();
    EngineBuilder::new(compiler_config).into()
});

/// A cache of compiled contract modules, with their respective [`Engine`] instances.
static CONTRACT_CACHE: Lazy<Mutex<ModuleCache<CachedContractModule>>> = Lazy::new(Mutex::default);

/// A cache of compiled service modules.
static SERVICE_CACHE: Lazy<Mutex<ModuleCache<Module>>> = Lazy::new(Mutex::default);

/// Type representing a running [Wasmer](https://wasmer.io/) contract.
pub(crate) struct WasmerContractInstance<Runtime> {
    /// The application type.
    application: contract::Contract,

    /// The application's memory state.
    store: Store,

    /// The system runtime.
    runtime: Runtime,

    /// The Wasmer instance.
    instance: Instance,

    /// The starting amount of fuel.
    initial_fuel: u64,
}

impl<Runtime> WasmerContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Unpin,
{
    fn configure_initial_fuel(&mut self) -> Result<(), ExecutionError> {
        self.initial_fuel = self.runtime.remaining_fuel()?;

        metering::set_remaining_points(&mut self.store, &self.instance, self.initial_fuel);

        Ok(())
    }

    fn persist_remaining_fuel(&mut self) -> Result<(), ExecutionError> {
        let remaining_fuel = match metering::get_remaining_points(&mut self.store, &self.instance) {
            MeteringPoints::Exhausted => 0,
            MeteringPoints::Remaining(fuel) => fuel,
        };

        assert!(self.initial_fuel >= remaining_fuel);

        self.runtime
            .consume_fuel(self.initial_fuel - remaining_fuel)
    }
}

/// Type representing a running [Wasmer](https://wasmer.io/) service.
pub struct WasmerServiceInstance {
    /// The application type.
    application: service::Service,

    /// The application's memory state.
    store: Store,
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
        let mut store = Store::new(contract_engine);
        let mut imports = imports! {};
        let system_api_setup =
            contract_system_api::add_to_imports(&mut store, &mut imports, runtime.clone());
        let views_api_setup =
            view_system_api::add_to_imports(&mut store, &mut imports, runtime.clone());
        let (application, instance) =
            contract::Contract::instantiate(&mut store, contract_module, &mut imports)
                .map_err(WasmExecutionError::LoadContractModule)?;

        system_api_setup(&instance, &store).map_err(WasmExecutionError::LoadContractModule)?;
        views_api_setup(&instance, &store).map_err(WasmExecutionError::LoadContractModule)?;

        Ok(Self {
            application,
            store,
            runtime,
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
                Module::new(&*SERVICE_ENGINE, bytecode)
                    .map_err(wit_bindgen_host_wasmer_rust::anyhow::Error::from)
            })
            .map_err(WasmExecutionError::LoadServiceModule)?;
        Ok(WasmServiceModule::Wasmer { module })
    }
}

impl WasmerServiceInstance {
    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare<Runtime>(
        service_module: &Module,
        runtime: Runtime,
    ) -> Result<Self, WasmExecutionError>
    where
        Runtime: ServiceRuntime + Clone + Send + Sync + Unpin + 'static,
    {
        let mut store = Store::new(&*SERVICE_ENGINE);
        let mut imports = imports! {};
        let system_api_setup =
            service_system_api::add_to_imports(&mut store, &mut imports, runtime.clone());
        let views_api_setup = view_system_api::add_to_imports(&mut store, &mut imports, runtime);
        let (application, instance) =
            service::Service::instantiate(&mut store, service_module, &mut imports)
                .map_err(WasmExecutionError::LoadServiceModule)?;

        system_api_setup(&instance, &store).map_err(WasmExecutionError::LoadServiceModule)?;
        views_api_setup(&instance, &store).map_err(WasmExecutionError::LoadServiceModule)?;

        Ok(Self { application, store })
    }
}

impl<Runtime> crate::UserContract for WasmerContractInstance<Runtime>
where
    Runtime: ContractRuntime + Send + Unpin,
{
    fn initialize(
        &mut self,
        _context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result = contract::Contract::initialize(&self.application, &mut self.store, &argument)
            .map(|inner| inner.map(RawExecutionOutcome::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result =
            contract::Contract::execute_operation(&self.application, &mut self.store, &operation)
                .map(|inner| inner.map(RawExecutionOutcome::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }

    fn execute_message(
        &mut self,
        _context: MessageContext,
        message: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result =
            contract::Contract::execute_message(&self.application, &mut self.store, &message)
                .map(|inner| inner.map(RawExecutionOutcome::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }

    fn handle_application_call(
        &mut self,
        _context: CalleeContext,
        argument: Vec<u8>,
    ) -> Result<ApplicationCallOutcome, ExecutionError> {
        self.configure_initial_fuel()?;
        let result = contract::Contract::handle_application_call(
            &self.application,
            &mut self.store,
            &argument,
        )
        .map(|inner| inner.map(ApplicationCallOutcome::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }

    fn finalize(
        &mut self,
        _context: FinalizeContext,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        self.configure_initial_fuel()?;
        let result = contract::Contract::finalize(&self.application, &mut self.store)
            .map(|inner| inner.map(RawExecutionOutcome::from));
        self.persist_remaining_fuel()?;
        result?.map_err(ExecutionError::UserError)
    }
}

impl crate::UserService for WasmerServiceInstance {
    fn handle_query(
        &mut self,
        _context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        service::Service::handle_query(&self.application, &mut self.store, &argument)?
            .map_err(ExecutionError::UserError)
    }
}

impl_contract_system_api!(wasmer::RuntimeError);
impl_service_system_api!(wasmer::RuntimeError);
impl_view_system_api!(wasmer::RuntimeError);

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
