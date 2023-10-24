// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

// Export the system interface used by a user contract.
wit_bindgen_host_wasmer_rust::export!({
    custom_error: true,
    paths: ["contract_system_api.wit"],
});

// Export the system interface used by a user service.
wit_bindgen_host_wasmer_rust::export!("service_system_api.wit");

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
#[path = "guest_futures.rs"]
mod guest_futures;

use super::{
    async_boundary::{HostFuture, WakerForwarder},
    async_determinism::{HostFutureQueue, QueuedHostFutureFactory},
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    module_cache::ModuleCache,
    WasmApplication, WasmExecutionError,
};
use crate::{Bytecode, ContractRuntime, ExecutionError, ServiceRuntime, SessionId};
use bytes::Bytes;
use linera_views::{batch::Batch, views::ViewError};
use once_cell::sync::Lazy;
use std::{future::Future, marker::PhantomData, mem, sync::Arc, task::Poll};
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};
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
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub struct Contract<'runtime> {
    contract: contract::Contract,
    _lifetime: PhantomData<&'runtime ()>,
}

impl<'runtime> ApplicationRuntimeContext for Contract<'runtime> {
    type Store = Store;
    type Error = RuntimeError;
    type Extra = WasmerContractExtra<'runtime>;

    fn finalize(context: &mut WasmRuntimeContext<Self>) {
        let runtime_guard = context
            .extra
            .runtime_guard
            .runtime
            .try_lock()
            .expect("Unexpected concurrent access to ContractRuntime");
        let runtime = runtime_guard
            .as_ref()
            .expect("Runtime guard dropped prematurely");

        let remaining_fuel =
            match metering::get_remaining_points(&mut context.store, &context.extra.instance) {
                MeteringPoints::Exhausted => 0,
                MeteringPoints::Remaining(fuel) => fuel,
            };

        runtime.set_remaining_fuel(remaining_fuel);
    }
}

/// Type representing the [Wasmer](https://wasmer.io/) service runtime.
pub struct Service<'runtime> {
    service: service::Service,
    _lifetime: PhantomData<&'runtime ()>,
}

impl<'runtime> ApplicationRuntimeContext for Service<'runtime> {
    type Store = Store;
    type Error = RuntimeError;
    type Extra = RuntimeGuard<'runtime, &'static dyn ServiceRuntime>;
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
    pub fn prepare_contract_runtime_with_wasmer<'runtime>(
        (contract_engine, contract_module): &(Engine, Module),
        runtime: &'runtime dyn ContractRuntime,
    ) -> Result<WasmRuntimeContext<'static, Contract<'runtime>>, WasmExecutionError> {
        let mut store = Store::new(contract_engine);
        let mut imports = imports! {};
        let waker_forwarder = WakerForwarder::default();
        let (future_queue, queued_future_factory) = HostFutureQueue::new();
        let (system_api, runtime_guard): (SystemApi<&'static dyn ContractRuntime>, _) =
            SystemApi::new(waker_forwarder.clone(), runtime);
        let system_api = Arc::new(Mutex::new(system_api));
        let contract_system_api =
            ContractSystemApi::new(system_api.clone(), queued_future_factory.clone());
        let view_system_api = ViewSystemApi::new(system_api, queued_future_factory);
        let system_api_setup =
            contract_system_api::add_to_imports(&mut store, &mut imports, contract_system_api);
        let views_api_setup =
            view_system_api::add_to_imports(&mut store, &mut imports, view_system_api);
        let (contract, instance) =
            contract::Contract::instantiate(&mut store, contract_module, &mut imports)
                .map_err(WasmExecutionError::LoadContractModule)?;
        let application = Contract {
            contract,
            _lifetime: PhantomData,
        };

        metering::set_remaining_points(&mut store, &instance, runtime.remaining_fuel());

        system_api_setup(&instance, &store).map_err(WasmExecutionError::LoadContractModule)?;
        views_api_setup(&instance, &store).map_err(WasmExecutionError::LoadContractModule)?;

        Ok(WasmRuntimeContext {
            waker_forwarder,
            application,
            future_queue,
            store,
            extra: WasmerContractExtra {
                instance,
                runtime_guard,
            },
        })
    }

    /// Prepares a runtime instance to call into the Wasm service.
    pub fn prepare_service_runtime_with_wasmer<'runtime>(
        service_module: &Module,
        runtime: &'runtime dyn ServiceRuntime,
    ) -> Result<WasmRuntimeContext<'static, Service<'runtime>>, WasmExecutionError> {
        let mut store = Store::new(&*SERVICE_ENGINE);
        let mut imports = imports! {};
        let waker_forwarder = WakerForwarder::default();
        let (future_queue, _queued_future_factory) = HostFutureQueue::new();
        let (system_api, runtime_guard) = SystemApi::new(waker_forwarder.clone(), runtime);
        let system_api = Arc::new(Mutex::new(system_api));
        let service_system_api = ServiceSystemApi::new(system_api.clone());
        let view_system_api = ViewSystemApi::new(system_api, ());
        let system_api_setup =
            service_system_api::add_to_imports(&mut store, &mut imports, service_system_api);
        let views_api_setup =
            view_system_api::add_to_imports(&mut store, &mut imports, view_system_api);
        let (service, instance) =
            service::Service::instantiate(&mut store, service_module, &mut imports)
                .map_err(WasmExecutionError::LoadServiceModule)?;
        let application = Service {
            service,
            _lifetime: PhantomData,
        };

        system_api_setup(&instance, &store).map_err(WasmExecutionError::LoadServiceModule)?;
        views_api_setup(&instance, &store).map_err(WasmExecutionError::LoadServiceModule)?;

        Ok(WasmRuntimeContext {
            waker_forwarder,
            application,
            future_queue,
            store,
            extra: runtime_guard,
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

impl<'runtime> common::Contract for Contract<'runtime> {
    type Initialize = contract::Initialize;
    type ExecuteOperation = contract::ExecuteOperation;
    type ExecuteMessage = contract::ExecuteMessage;
    type HandleApplicationCall = contract::HandleApplicationCall;
    type HandleSessionCall = contract::HandleSessionCall;
    type OperationContext = contract::OperationContext;
    type MessageContext = contract::MessageContext;
    type CalleeContext = contract::CalleeContext;
    type SessionId = contract::SessionId;
    type PollExecutionResult = contract::PollExecutionResult;
    type PollApplicationCallResult = contract::PollApplicationCallResult;
    type PollSessionCallResult = contract::PollSessionCallResult;

    fn initialize_new(
        &self,
        store: &mut Store,
        context: contract::OperationContext,
        argument: &[u8],
    ) -> Result<contract::Initialize, RuntimeError> {
        contract::Contract::initialize_new(&self.contract, store, context, argument)
    }

    fn initialize_poll(
        &self,
        store: &mut Store,
        future: &contract::Initialize,
    ) -> Result<contract::PollExecutionResult, RuntimeError> {
        contract::Contract::initialize_poll(&self.contract, store, future)
    }

    fn execute_operation_new(
        &self,
        store: &mut Store,
        context: contract::OperationContext,
        operation: &[u8],
    ) -> Result<contract::ExecuteOperation, RuntimeError> {
        contract::Contract::execute_operation_new(&self.contract, store, context, operation)
    }

    fn execute_operation_poll(
        &self,
        store: &mut Store,
        future: &contract::ExecuteOperation,
    ) -> Result<contract::PollExecutionResult, RuntimeError> {
        contract::Contract::execute_operation_poll(&self.contract, store, future)
    }

    fn execute_message_new(
        &self,
        store: &mut Store,
        context: contract::MessageContext,
        message: &[u8],
    ) -> Result<contract::ExecuteMessage, RuntimeError> {
        contract::Contract::execute_message_new(&self.contract, store, context, message)
    }

    fn execute_message_poll(
        &self,
        store: &mut Store,
        future: &contract::ExecuteMessage,
    ) -> Result<contract::PollExecutionResult, RuntimeError> {
        contract::Contract::execute_message_poll(&self.contract, store, future)
    }

    fn handle_application_call_new(
        &self,
        store: &mut Store,
        context: contract::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::HandleApplicationCall, RuntimeError> {
        contract::Contract::handle_application_call_new(
            &self.contract,
            store,
            context,
            argument,
            forwarded_sessions,
        )
    }

    fn handle_application_call_poll(
        &self,
        store: &mut Store,
        future: &contract::HandleApplicationCall,
    ) -> Result<contract::PollApplicationCallResult, RuntimeError> {
        contract::Contract::handle_application_call_poll(&self.contract, store, future)
    }

    fn handle_session_call_new(
        &self,
        store: &mut Store,
        context: contract::CalleeContext,
        session: &[u8],
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::HandleSessionCall, RuntimeError> {
        contract::Contract::handle_session_call_new(
            &self.contract,
            store,
            context,
            session,
            argument,
            forwarded_sessions,
        )
    }

    fn handle_session_call_poll(
        &self,
        store: &mut Store,
        future: &contract::HandleSessionCall,
    ) -> Result<contract::PollSessionCallResult, RuntimeError> {
        contract::Contract::handle_session_call_poll(&self.contract, store, future)
    }
}

impl<'runtime> common::Service for Service<'runtime> {
    type HandleQuery = service::HandleQuery;
    type QueryContext = service::QueryContext;
    type PollApplicationQueryResult = service::PollApplicationQueryResult;

    fn handle_query_new(
        &self,
        store: &mut Store,
        context: service::QueryContext,
        argument: &[u8],
    ) -> Result<service::HandleQuery, RuntimeError> {
        service::Service::handle_query_new(&self.service, store, context, argument)
    }

    fn handle_query_poll(
        &self,
        store: &mut Store,
        future: &service::HandleQuery,
    ) -> Result<service::PollApplicationQueryResult, RuntimeError> {
        service::Service::handle_query_poll(&self.service, store, future)
    }
}

/// Helper type with common functionality across the contract and service system API
/// implementations.
pub struct SystemApi<S> {
    waker: WakerForwarder,
    runtime: Arc<Mutex<Option<S>>>,
}

impl<Runtime> SystemApi<Runtime> {
    /// Creates a new [`SystemApi`] instance, ensuring that the lifetime of the borrowed `runtime`
    /// reference is respected.
    ///
    /// # Safety
    ///
    /// This method uses a [`mem::transmute`] call to erase the lifetime of the `runtime` reference.
    /// However, this is safe because the lifetime is transfered to the returned [`RuntimeGuard`],
    /// which removes the unsafe reference from memory when it is dropped, ensuring the lifetime is
    /// respected.
    ///
    /// The [`RuntimeGuard`] instance must be kept alive while the `runtime` reference is still
    /// expected to be alive and usable by the Wasm application.
    pub fn new<'runtime>(
        waker: WakerForwarder,
        runtime: impl RemoveLifetime<WithoutLifetime = Runtime> + 'runtime,
    ) -> (Self, RuntimeGuard<'runtime, Runtime>) {
        let runtime = Arc::new(Mutex::new(Some(unsafe { runtime.remove_lifetime() })));

        let guard = RuntimeGuard {
            runtime: runtime.clone(),
            _lifetime: PhantomData,
        };

        (SystemApi { waker, runtime }, guard)
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
    shared: Arc<Mutex<SystemApi<&'static dyn ContractRuntime>>>,
    queued_future_factory: QueuedHostFutureFactory<'static>,
}

impl ContractSystemApi {
    /// Creates a new [`ContractSystemApi`] instance.
    pub fn new(
        shared: Arc<Mutex<SystemApi<&'static dyn ContractRuntime>>>,
        queued_future_factory: QueuedHostFutureFactory<'static>,
    ) -> Self {
        ContractSystemApi {
            shared,
            queued_future_factory,
        }
    }

    /// Safely obtains the [`ContractRuntime`] trait object instance to handle a system call.
    ///
    /// # Panics
    ///
    /// If there is a concurrent call from the Wasm application (which is impossible as long as it
    /// is executed in a single thread) or if the trait object is no longer alive (or more
    /// accurately, if the [`RuntimeGuard`] returned by [`Self::new`] was dropped to indicate it's
    /// no longer alive).
    fn runtime(&self) -> &'static dyn ContractRuntime {
        *self
            .shared
            .try_lock()
            .expect("Unexpected concurrent system API access by application")
            .runtime
            .try_lock()
            .expect("Unexpected concurrent runtime access by application")
            .as_ref()
            .expect("Application called runtime after it should have stopped")
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn waker(&mut self) -> MappedMutexGuard<'_, WakerForwarder> {
        MutexGuard::map(
            self.shared
                .try_lock()
                .expect("Unexpected concurrent system API access by application"),
            |system_api| &mut system_api.waker,
        )
    }
}

impl_contract_system_api!(ContractSystemApi);

/// Implementation to forward service system calls from the guest Wasm module to the host
/// implementation.
pub struct ServiceSystemApi {
    shared: Arc<Mutex<SystemApi<&'static dyn ServiceRuntime>>>,
}

impl ServiceSystemApi {
    /// Creates a new [`ServiceSystemApi`] instance.
    pub fn new(shared: Arc<Mutex<SystemApi<&'static dyn ServiceRuntime>>>) -> Self {
        ServiceSystemApi { shared }
    }

    /// Safely obtains the [`ServiceRuntime`] trait object instance to handle a system call.
    ///
    /// # Panics
    ///
    /// If there is a concurrent call from the Wasm application (which is impossible as long as it
    /// is executed in a single thread) or if the trait object is no longer alive (or more
    /// accurately, if the [`RuntimeGuard`] returned by [`Self::new`] was dropped to indicate it's
    /// no longer alive).
    fn runtime(&self) -> &'static dyn ServiceRuntime {
        *self
            .shared
            .try_lock()
            .expect("Unexpected concurrent system API access by application")
            .runtime
            .try_lock()
            .expect("Unexpected concurrent runtime access by application")
            .as_ref()
            .expect("Application called runtime after it should have stopped")
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn waker(&mut self) -> MappedMutexGuard<'_, WakerForwarder> {
        MutexGuard::map(
            self.shared
                .try_lock()
                .expect("Unexpected concurrent system API access by application"),
            |system_api| &mut system_api.waker,
        )
    }
}

impl_service_system_api!(ServiceSystemApi);

/// Implementation to forward view system calls from the guest Wasm module to the host
/// implementation.
pub struct ViewSystemApi<Runtime, MaybeQueuedFutureFactory> {
    shared: Arc<Mutex<SystemApi<Runtime>>>,
    queued_future_factory: MaybeQueuedFutureFactory,
}

impl<Runtime, MaybeQueuedFutureFactory> ViewSystemApi<Runtime, MaybeQueuedFutureFactory> {
    /// Creates a new [`ViewSystemApi`] instance.
    pub fn new(
        shared: Arc<Mutex<SystemApi<Runtime>>>,
        queued_future_factory: MaybeQueuedFutureFactory,
    ) -> Self {
        ViewSystemApi {
            shared,
            queued_future_factory,
        }
    }
}

impl ViewSystemApi<&'static dyn ContractRuntime, QueuedHostFutureFactory<'static>> {
    /// Safely obtains the [`ContractRuntime`] trait object instance to handle a system call.
    ///
    /// # Panics
    ///
    /// If there is a concurrent call from the Wasm application (which is impossible as long as it
    /// is executed in a single thread) or if the trait object is no longer alive (or more
    /// accurately, if the [`RuntimeGuard`] returned by [`Self::new`] was dropped to indicate it's
    /// no longer alive).
    fn runtime(&self) -> &'static dyn ContractRuntime {
        *self
            .shared
            .try_lock()
            .expect("Unexpected concurrent system API access by application")
            .runtime
            .try_lock()
            .expect("Unexpected concurrent runtime access by application")
            .as_ref()
            .expect("Application called runtime after it should have stopped")
    }

    /// Same as [`Self::runtime`].
    fn runtime_with_writable_storage(
        &self,
    ) -> Result<&'static dyn ContractRuntime, ExecutionError> {
        Ok(self.runtime())
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn waker(&mut self) -> MappedMutexGuard<'_, WakerForwarder> {
        MutexGuard::map(
            self.shared
                .try_lock()
                .expect("Unexpected concurrent system API access by application"),
            |system_api| &mut system_api.waker,
        )
    }

    /// Creates a new [`HostFuture`] instance with the provided `future`.
    fn new_host_future<Output>(
        &mut self,
        future: impl Future<Output = Output> + Send + 'static,
    ) -> HostFuture<'static, Output>
    where
        Output: Send + 'static,
    {
        self.queued_future_factory.enqueue(future)
    }
}

impl ViewSystemApi<&'static dyn ServiceRuntime, ()> {
    /// Safely obtains the [`ServiceRuntime`] trait object instance to handle a system call.
    ///
    /// # Panics
    ///
    /// If there is a concurrent call from the Wasm application (which is impossible as long as it
    /// is executed in a single thread) or if the trait object is no longer alive (or more
    /// accurately, if the [`RuntimeGuard`] returned by [`Self::new`] was dropped to indicate it's
    /// no longer alive).
    fn runtime(&self) -> &'static dyn ServiceRuntime {
        *self
            .shared
            .try_lock()
            .expect("Unexpected concurrent system API access by application")
            .runtime
            .try_lock()
            .expect("Unexpected concurrent runtime access by application")
            .as_ref()
            .expect("Application called runtime after it should have stopped")
    }

    /// Returns an error due to an attempt to write to storage from a service.
    fn runtime_with_writable_storage(
        &self,
    ) -> Result<&'static dyn ContractRuntime, ExecutionError> {
        Err(WasmExecutionError::WriteAttemptToReadOnlyStorage.into())
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn waker(&mut self) -> MappedMutexGuard<'_, WakerForwarder> {
        MutexGuard::map(
            self.shared
                .try_lock()
                .expect("Unexpected concurrent system API access by application"),
            |system_api| &mut system_api.waker,
        )
    }

    /// Enqueues a `future` to be executed deterministically.
    fn new_host_future<Output>(
        &mut self,
        future: impl Future<Output = Output> + Send + 'static,
    ) -> HostFuture<'static, Output> {
        HostFuture::new(future)
    }
}

impl_view_system_api!(
    ViewSystemApi<&'static dyn ContractRuntime, QueuedHostFutureFactory<'static>>
);
impl_view_system_api!(ViewSystemApi<&'static dyn ServiceRuntime, ()>);

/// Extra parameters necessary when cleaning up after contract execution.
pub struct WasmerContractExtra<'runtime> {
    runtime_guard: RuntimeGuard<'runtime, &'static dyn ContractRuntime>,
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
