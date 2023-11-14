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

use super::{
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    module_cache::ModuleCache,
    runtime_actor::{BaseRequest, ContractRequest, SendRequestExt, ServiceRequest},
    ApplicationCallResult, SessionCallResult, WasmApplication, WasmExecutionError,
};
use crate::{
    Bytecode, CalleeContext, ContractRuntime, ExecutionError, MessageContext, OperationContext,
    QueryContext, RawExecutionResult, ServiceRuntime,
};
use bytes::Bytes;
use futures::channel::mpsc;
use linera_base::identifiers::SessionId;
use linera_views::batch::Batch;
use once_cell::sync::Lazy;
use std::{marker::PhantomData, mem, sync::Arc};
use tokio::sync::Mutex;
use wasmer::{
    imports, wasmparser::Operator, CompilerConfig, Engine, EngineBuilder, Instance, Module,
    RuntimeError, Singlepass, Store,
};
use wasmer_middlewares::metering::{self, Metering, MeteringPoints};
use wit_bindgen_host_wasmer_rust::Le;

/// An [`Engine`] instance configured to run application services.
static SERVICE_ENGINE: Lazy<Engine> = Lazy::new(|| {
    let compiler_config = Singlepass::default();
    EngineBuilder::new(compiler_config).into()
});

/// A cache of compiled contract modules, with their respective [`Engine`] instances.
static CONTRACT_CACHE: Lazy<Mutex<ModuleCache<CachedContractModule>>> = Lazy::new(Mutex::default);

/// A cache of compiled service modules.
static SERVICE_CACHE: Lazy<Mutex<ModuleCache<Module>>> = Lazy::new(Mutex::default);

/// Type representing the [Wasmer](https://wasmer.io/) contract runtime.
pub struct Contract {
    contract: contract::Contract,
}

impl ApplicationRuntimeContext for Contract {
    type Store = Store;
    type Error = RuntimeError;
    type Extra = WasmerContractExtra;

    fn configure_initial_fuel(context: &mut WasmRuntimeContext<Self>) {
        let remaining_points = context
            .extra
            .runtime
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

        metering::set_remaining_points(
            &mut context.store,
            &context.extra.instance,
            remaining_points,
        );
    }

    fn persist_remaining_fuel(context: &mut WasmRuntimeContext<Self>) -> Result<(), ()> {
        let remaining_fuel =
            match metering::get_remaining_points(&mut context.store, &context.extra.instance) {
                MeteringPoints::Exhausted => 0,
                MeteringPoints::Remaining(fuel) => fuel,
            };

        context
            .extra
            .runtime
            .send_request(|response_sender| ContractRequest::SetRemainingFuel {
                remaining_fuel,
                response_sender,
            })
            .map_err(|_| ())?
            .recv()
            .map_err(|_| ())
    }
}

/// Type representing the [Wasmer](https://wasmer.io/) service runtime.
pub struct Service {
    service: service::Service,
}

impl ApplicationRuntimeContext for Service {
    type Store = Store;
    type Error = RuntimeError;
    type Extra = ();

    fn configure_initial_fuel(_context: &mut WasmRuntimeContext<Self>) {}

    fn persist_remaining_fuel(_context: &mut WasmRuntimeContext<Self>) -> Result<(), ()> {
        Ok(())
    }
}

impl WasmApplication {
    /// Creates a new [`WasmApplication`] using Wasmtime with the provided bytecodes.
    pub async fn new_with_wasmer(
        contract_bytecode: Bytecode,
        service_bytecode: Bytecode,
    ) -> Result<Self, WasmExecutionError> {
        let mut contract_cache = CONTRACT_CACHE.lock().await;
        let contract = contract_cache
            .get_or_insert_with(contract_bytecode, CachedContractModule::new)
            .map_err(WasmExecutionError::LoadContractModule)?
            .create_execution_instance()
            .map_err(WasmExecutionError::LoadContractModule)?;

        let mut service_cache = SERVICE_CACHE.lock().await;
        let service = service_cache
            .get_or_insert_with(service_bytecode, |bytecode| {
                Module::new(&*SERVICE_ENGINE, bytecode)
                    .map_err(wit_bindgen_host_wasmer_rust::anyhow::Error::from)
            })
            .map_err(WasmExecutionError::LoadServiceModule)?;

        Ok(WasmApplication::Wasmer { contract, service })
    }

    /// Prepares a runtime instance to call into the Wasm contract.
    pub fn prepare_contract_runtime_with_wasmer(
        (contract_engine, contract_module): &(Engine, Module),
        runtime: mpsc::UnboundedSender<ContractRequest>,
    ) -> Result<WasmRuntimeContext<Contract>, WasmExecutionError> {
        let mut store = Store::new(contract_engine);
        let mut imports = imports! {};
        let contract_system_api = ContractSystemApi::new(runtime.clone());
        let view_system_api = ContractViewSystemApi::new(runtime.clone());
        let system_api_setup =
            contract_system_api::add_to_imports(&mut store, &mut imports, contract_system_api);
        let views_api_setup =
            view_system_api::add_to_imports(&mut store, &mut imports, view_system_api);
        let (contract, instance) =
            contract::Contract::instantiate(&mut store, contract_module, &mut imports)
                .map_err(WasmExecutionError::LoadContractModule)?;
        let application = Contract { contract };

        system_api_setup(&instance, &store).map_err(WasmExecutionError::LoadContractModule)?;
        views_api_setup(&instance, &store).map_err(WasmExecutionError::LoadContractModule)?;

        Ok(WasmRuntimeContext {
            application,
            store,
            extra: WasmerContractExtra { runtime, instance },
        })
    }

    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare_service_runtime_with_wasmer(
        service_module: &Module,
        runtime: mpsc::UnboundedSender<ServiceRequest>,
    ) -> Result<WasmRuntimeContext<Service>, WasmExecutionError> {
        let mut store = Store::new(&*SERVICE_ENGINE);
        let mut imports = imports! {};
        let service_system_api = ServiceSystemApi::new(runtime.clone());
        let view_system_api = ServiceViewSystemApi::new(runtime);
        let system_api_setup =
            service_system_api::add_to_imports(&mut store, &mut imports, service_system_api);
        let views_api_setup =
            view_system_api::add_to_imports(&mut store, &mut imports, view_system_api);
        let (service, instance) =
            service::Service::instantiate(&mut store, service_module, &mut imports)
                .map_err(WasmExecutionError::LoadServiceModule)?;
        let application = Service { service };

        system_api_setup(&instance, &store).map_err(WasmExecutionError::LoadServiceModule)?;
        views_api_setup(&instance, &store).map_err(WasmExecutionError::LoadServiceModule)?;

        Ok(WasmRuntimeContext {
            application,
            store,
            extra: (),
        })
    }

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

impl common::Contract for Contract {
    fn initialize(
        &self,
        store: &mut Store,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, RuntimeError> {
        contract::Contract::initialize(&self.contract, store, context.into(), &argument)
            .map(|inner| inner.map(RawExecutionResult::from))
    }

    fn execute_operation(
        &self,
        store: &mut Store,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, RuntimeError> {
        contract::Contract::execute_operation(&self.contract, store, context.into(), &operation)
            .map(|inner| inner.map(RawExecutionResult::from))
    }

    fn execute_message(
        &self,
        store: &mut Store,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<Result<RawExecutionResult<Vec<u8>>, String>, RuntimeError> {
        contract::Contract::execute_message(&self.contract, store, context.into(), &message)
            .map(|inner| inner.map(RawExecutionResult::from))
    }

    fn handle_application_call(
        &self,
        store: &mut Store,
        context: CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<Result<ApplicationCallResult, String>, RuntimeError> {
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
        store: &mut Store,
        context: CalleeContext,
        session: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<Result<(SessionCallResult, Vec<u8>), String>, RuntimeError> {
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
        store: &mut Store,
        context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Result<Vec<u8>, String>, RuntimeError> {
        service::Service::handle_query(&self.service, store, context.into(), &argument)
    }
}

/// Unsafe trait to artificially transmute a type in order to extend its lifetime.
///
/// # Safety
///
/// It is the caller's responsibility to ensure that the resulting [`WithoutLifetime`] type is not
/// in use after the original lifetime expires.
pub unsafe trait RemoveLifetime {
    type WithoutLifetime: 'static;

    /// Removes the lifetime artificially.
    ///
    /// # Safety
    ///
    /// It is the caller's responsibility to ensure that the resulting [`WithoutLifetime`] type is not
    /// in use after the original lifetime expires.
    unsafe fn remove_lifetime(self) -> Self::WithoutLifetime;
}

unsafe impl<'runtime> RemoveLifetime for &'runtime dyn ContractRuntime {
    type WithoutLifetime = &'static dyn ContractRuntime;

    unsafe fn remove_lifetime(self) -> Self::WithoutLifetime {
        unsafe { mem::transmute(self) }
    }
}

unsafe impl<'runtime> RemoveLifetime for &'runtime dyn ServiceRuntime {
    type WithoutLifetime = &'static dyn ServiceRuntime;

    unsafe fn remove_lifetime(self) -> Self::WithoutLifetime {
        unsafe { mem::transmute(self) }
    }
}

/// Implementation to forward contract system calls from the guest Wasm module to the host
/// implementation.
pub struct ContractSystemApi {
    runtime: mpsc::UnboundedSender<ContractRequest>,
}

impl ContractSystemApi {
    /// Creates a new [`ContractSystemApi`] instance.
    pub fn new(runtime: mpsc::UnboundedSender<ContractRequest>) -> Self {
        ContractSystemApi { runtime }
    }
}

impl_contract_system_api!(ContractSystemApi, wasmer::RuntimeError);

/// Implementation to forward service system calls from the guest Wasm module to the host
/// implementation.
pub struct ServiceSystemApi {
    runtime: mpsc::UnboundedSender<ServiceRequest>,
}

impl ServiceSystemApi {
    /// Creates a new [`ServiceSystemApi`] instance.
    pub fn new(runtime: mpsc::UnboundedSender<ServiceRequest>) -> Self {
        ServiceSystemApi { runtime }
    }
}

impl_service_system_api!(ServiceSystemApi, wasmer::RuntimeError);

/// Implementation to forward view system calls from the contract guest Wasm module to the host
/// implementation.
pub struct ContractViewSystemApi {
    runtime: mpsc::UnboundedSender<ContractRequest>,
}

impl ContractViewSystemApi {
    /// Creates a new [`ContractViewSystemApi`] instance.
    pub fn new(runtime: mpsc::UnboundedSender<ContractRequest>) -> Self {
        ContractViewSystemApi { runtime }
    }
}

/// Implementation to forward view system calls from the guest WASM module to the host
/// implementation.
pub struct ServiceViewSystemApi {
    runtime: mpsc::UnboundedSender<ServiceRequest>,
}

impl ServiceViewSystemApi {
    /// Creates a new [`ServiceViewSystemApi`] instance.
    pub fn new(runtime: mpsc::UnboundedSender<ServiceRequest>) -> Self {
        ServiceViewSystemApi { runtime }
    }
}

impl_view_system_api_for_contract!(ContractViewSystemApi, wasmer::RuntimeError);
impl_view_system_api_for_service!(ServiceViewSystemApi, wasmer::RuntimeError);

/// Extra parameters necessary when cleaning up after contract execution.
pub struct WasmerContractExtra {
    runtime: mpsc::UnboundedSender<ContractRequest>,
    instance: Instance,
}

/// A guard to unsure that the [`ContractRuntime`] trait object isn't called after it's no longer
/// borrowed.
pub struct RuntimeGuard<'runtime, S> {
    runtime: Arc<Mutex<Option<S>>>,
    _lifetime: PhantomData<&'runtime ()>,
}

impl<S> Drop for RuntimeGuard<'_, S> {
    fn drop(&mut self) {
        self.runtime
            .try_lock()
            .expect("Guard dropped while runtime is still in use")
            .take();
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
        let metering = Arc::new(Metering::new(0, WasmApplication::operation_cost));
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
