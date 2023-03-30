// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

// Export the writable system interface used by a user contract.
wit_bindgen_host_wasmer_rust::export!({
    custom_error: true,
    paths: ["../linera-sdk/writable_system.wit"],
});

// Export the queryable system interface used by a user service.
wit_bindgen_host_wasmer_rust::export!("../linera-sdk/queryable_system.wit");

// Import the interface implemented by a user contract.
wit_bindgen_host_wasmer_rust::import!("../linera-sdk/contract.wit");

// Import the interface implemented by a user service.
wit_bindgen_host_wasmer_rust::import!("../linera-sdk/service.wit");

#[path = "conversions_from_wit.rs"]
mod conversions_from_wit;
#[path = "conversions_to_wit.rs"]
mod conversions_to_wit;
#[path = "guest_futures.rs"]
mod guest_futures;

use self::{queryable_system::QueryableSystem, writable_system::WritableSystem};
use super::{
    async_boundary::{HostFuture, WakerForwarder},
    async_determinism::{HostFutureQueue, QueuedHostFutureFactory},
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};

use crate::{ExecutionError, QueryableStorage, SessionId, WritableStorage};
use linera_views::{batch::Batch, views::ViewError};
use std::{marker::PhantomData, mem, sync::Arc, task::Poll};
use tokio::sync::Mutex;
use wasmer::{
    imports, wasmparser::Operator, CompilerConfig, EngineBuilder, Instance, Module, RuntimeError,
    Singlepass, Store,
};
use wasmer_middlewares::metering::{self, Metering, MeteringPoints};
use wit_bindgen_host_wasmer_rust::Le;

/// Type representing the [Wasmer](https://wasmer.io/) contract runtime.
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub struct Contract<'storage> {
    contract: contract::Contract,
    _lifetime: PhantomData<&'storage ()>,
}

impl<'storage> ApplicationRuntimeContext for Contract<'storage> {
    type Store = Store;
    type Error = RuntimeError;
    type Extra = WasmerContractExtra<'storage>;

    fn finalize(context: &mut WasmRuntimeContext<Self>) {
        let storage_guard = context
            .extra
            .storage_guard
            .storage
            .try_lock()
            .expect("Unexpected concurrent access to WritableStorage");
        let storage = storage_guard
            .as_ref()
            .expect("Storage guard dropped prematurely");

        let remaining_fuel =
            match metering::get_remaining_points(&mut context.store, &context.extra.instance) {
                MeteringPoints::Exhausted => 0,
                MeteringPoints::Remaining(fuel) => fuel,
            };

        storage.set_remaining_fuel(remaining_fuel);
    }
}

/// Type representing the [Wasmer](https://wasmer.io/) service runtime.
pub struct Service<'storage> {
    service: service::Service,
    _lifetime: PhantomData<&'storage ()>,
}

impl<'storage> ApplicationRuntimeContext for Service<'storage> {
    type Store = Store;
    type Error = RuntimeError;
    type Extra = StorageGuard<'storage, &'static dyn QueryableStorage>;
}

impl WasmApplication {
    /// Prepare a runtime instance to call into the WASM contract.
    pub fn prepare_contract_runtime_with_wasmer<'storage>(
        &self,
        storage: &'storage dyn WritableStorage,
    ) -> Result<WasmRuntimeContext<'static, Contract<'storage>>, WasmExecutionError> {
        let metering = Arc::new(Metering::new(
            storage.remaining_fuel(),
            Self::operation_cost,
        ));
        let mut compiler_config = Singlepass::default();
        compiler_config.push_middleware(metering);
        compiler_config.canonicalize_nans(true);

        let mut store = Store::new(EngineBuilder::new(compiler_config));
        let module = Module::new(&store, &self.contract_bytecode)
            .map_err(wit_bindgen_host_wasmer_rust::anyhow::Error::from)?;
        let mut imports = imports! {};
        let waker_forwarder = WakerForwarder::default();
        let (future_queue, queued_future_factory) = HostFutureQueue::new();
        let (system_api, storage_guard) =
            ContractSystemApi::new(waker_forwarder.clone(), storage, queued_future_factory);
        let system_api_setup =
            writable_system::add_to_imports(&mut store, &mut imports, system_api);
        let (contract, instance) =
            contract::Contract::instantiate(&mut store, &module, &mut imports)?;
        let application = Contract {
            contract,
            _lifetime: PhantomData,
        };

        system_api_setup(&instance, &store)?;

        Ok(WasmRuntimeContext {
            waker_forwarder,
            application,
            future_queue,
            store,
            extra: WasmerContractExtra {
                instance,
                storage_guard,
            },
        })
    }

    /// Prepare a runtime instance to call into the WASM service.
    pub fn prepare_service_runtime_with_wasmer<'storage>(
        &self,
        storage: &'storage dyn QueryableStorage,
    ) -> Result<WasmRuntimeContext<'static, Service<'storage>>, WasmExecutionError> {
        let mut store = Store::default();
        let module = Module::new(&store, &self.service_bytecode)
            .map_err(wit_bindgen_host_wasmer_rust::anyhow::Error::from)?;
        let mut imports = imports! {};
        let waker_forwarder = WakerForwarder::default();
        let (future_queue, _queued_future_factory) = HostFutureQueue::new();
        let (system_api, storage_guard) = ServiceSystemApi::new(waker_forwarder.clone(), storage);
        let system_api_setup =
            queryable_system::add_to_imports(&mut store, &mut imports, system_api);
        let (service, instance) = service::Service::instantiate(&mut store, &module, &mut imports)?;
        let application = Service {
            service,
            _lifetime: PhantomData,
        };

        system_api_setup(&instance, &store)?;

        Ok(WasmRuntimeContext {
            waker_forwarder,
            application,
            future_queue,
            store,
            extra: storage_guard,
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

impl<'storage> common::Contract for Contract<'storage> {
    type Initialize = contract::Initialize;
    type ExecuteOperation = contract::ExecuteOperation;
    type ExecuteEffect = contract::ExecuteEffect;
    type HandleApplicationCall = contract::HandleApplicationCall;
    type HandleSessionCall = contract::HandleSessionCall;
    type OperationContext = contract::OperationContext;
    type EffectContext = contract::EffectContext;
    type CalleeContext = contract::CalleeContext;
    type SessionParam<'param> = contract::SessionParam<'param>;
    type SessionId = contract::SessionId;
    type PollExecutionResult = contract::PollExecutionResult;
    type PollCallApplication = contract::PollCallApplication;
    type PollCallSession = contract::PollCallSession;

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

    fn execute_effect_new(
        &self,
        store: &mut Store,
        context: contract::EffectContext,
        effect: &[u8],
    ) -> Result<contract::ExecuteEffect, RuntimeError> {
        contract::Contract::execute_effect_new(&self.contract, store, context, effect)
    }

    fn execute_effect_poll(
        &self,
        store: &mut Store,
        future: &contract::ExecuteEffect,
    ) -> Result<contract::PollExecutionResult, RuntimeError> {
        contract::Contract::execute_effect_poll(&self.contract, store, future)
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
    ) -> Result<contract::PollCallApplication, RuntimeError> {
        contract::Contract::handle_application_call_poll(&self.contract, store, future)
    }

    fn handle_session_call_new(
        &self,
        store: &mut Store,
        context: contract::CalleeContext,
        session: contract::SessionParam,
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
    ) -> Result<contract::PollCallSession, RuntimeError> {
        contract::Contract::handle_session_call_poll(&self.contract, store, future)
    }
}

impl<'storage> common::Service for Service<'storage> {
    type QueryApplication = service::QueryApplication;
    type QueryContext = service::QueryContext;
    type PollQuery = service::PollQuery;

    fn query_application_new(
        &self,
        store: &mut Store,
        context: service::QueryContext,
        argument: &[u8],
    ) -> Result<service::QueryApplication, RuntimeError> {
        service::Service::query_application_new(&self.service, store, context, argument)
    }

    fn query_application_poll(
        &self,
        store: &mut Store,
        future: &service::QueryApplication,
    ) -> Result<service::PollQuery, RuntimeError> {
        service::Service::query_application_poll(&self.service, store, future)
    }
}

/// Helper type with common functionality across the contract and service system API
/// implementations.
struct SystemApi<S> {
    waker: WakerForwarder,
    storage: Arc<Mutex<Option<S>>>,
}

/// Implementation to forward contract system calls from the guest WASM module to the host
/// implementation.
pub struct ContractSystemApi {
    shared: SystemApi<&'static dyn WritableStorage>,
    queued_future_factory: QueuedHostFutureFactory<'static>,
}

impl ContractSystemApi {
    /// Creates a new [`ContractSystemApi`] instance, ensuring that the lifetime of the
    /// [`WritableStorage`] trait object is respected.
    ///
    /// # Safety
    ///
    /// This method uses a [`mem::transmute`] call to erase the lifetime of the `storage` trait
    /// object reference. However, this is safe because the lifetime is transfered to the returned
    /// [`StorageGuard`], which removes the unsafe reference from memory when it is dropped,
    /// ensuring the lifetime is respected.
    ///
    /// The [`StorageGuard`] instance must be kept alive while the trait object is still expected to
    /// be alive and usable by the WASM application.
    pub fn new<'storage>(
        waker: WakerForwarder,
        storage: &'storage dyn WritableStorage,
        queued_future_factory: QueuedHostFutureFactory<'static>,
    ) -> (Self, StorageGuard<'storage, &'static dyn WritableStorage>) {
        let storage_without_lifetime = unsafe { mem::transmute(storage) };
        let storage = Arc::new(Mutex::new(Some(storage_without_lifetime)));

        let guard = StorageGuard {
            storage: storage.clone(),
            _lifetime: PhantomData,
        };

        (
            ContractSystemApi {
                shared: SystemApi { waker, storage },
                queued_future_factory,
            },
            guard,
        )
    }

    /// Safely obtains the [`WritableStorage`] trait object instance to handle a system call.
    ///
    /// # Panics
    ///
    /// If there is a concurrent call from the WASM application (which is impossible as long as it
    /// is executed in a single thread) or if the trait object is no longer alive (or more
    /// accurately, if the [`StorageGuard`] returned by [`Self::new`] was dropped to indicate it's
    /// no longer alive).
    fn storage(&self) -> &'static dyn WritableStorage {
        *self
            .shared
            .storage
            .try_lock()
            .expect("Unexpected concurrent storage access by application")
            .as_ref()
            .expect("Application called storage after it should have stopped")
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn context(&mut self) -> &mut WakerForwarder {
        &mut self.shared.waker
    }
}

impl_writable_system!(ContractSystemApi);

/// Implementation to forward service system calls from the guest WASM module to the host
/// implementation.
pub struct ServiceSystemApi {
    shared: SystemApi<&'static dyn QueryableStorage>,
}

impl ServiceSystemApi {
    /// Creates a new [`ServiceSystemApi`] instance, ensuring that the lifetime of the
    /// [`QueryableStorage`] trait object is respected.
    ///
    /// # Safety
    ///
    /// This method uses a [`mem::transmute`] call to erase the lifetime of the `storage` trait
    /// object reference. However, this is safe because the lifetime is transfered to the returned
    /// [`StorageGuard`], which removes the unsafe reference from memory when it is dropped,
    /// ensuring the lifetime is respected.
    ///
    /// The [`StorageGuard`] instance must be kept alive while the trait object is still expected to
    /// be alive and usable by the WASM application.
    pub fn new<'storage>(
        waker: WakerForwarder,
        storage: &'storage dyn QueryableStorage,
    ) -> (Self, StorageGuard<'storage, &'static dyn QueryableStorage>) {
        let storage_without_lifetime = unsafe { mem::transmute(storage) };
        let storage = Arc::new(Mutex::new(Some(storage_without_lifetime)));

        let guard = StorageGuard {
            storage: storage.clone(),
            _lifetime: PhantomData,
        };

        (
            ServiceSystemApi {
                shared: SystemApi { waker, storage },
            },
            guard,
        )
    }

    /// Safely obtains the [`QueryableStorage`] trait object instance to handle a system call.
    ///
    /// # Panics
    ///
    /// If there is a concurrent call from the WASM application (which is impossible as long as it
    /// is executed in a single thread) or if the trait object is no longer alive (or more
    /// accurately, if the [`StorageGuard`] returned by [`Self::new`] was dropped to indicate it's
    /// no longer alive).
    fn storage(&self) -> &'static dyn QueryableStorage {
        *self
            .shared
            .storage
            .try_lock()
            .expect("Unexpected concurrent storage access by application")
            .as_ref()
            .expect("Application called storage after it should have stopped")
    }

    /// Returns the [`WakerForwarder`] to be used for asynchronous system calls.
    fn context(&mut self) -> &mut WakerForwarder {
        &mut self.shared.waker
    }
}

impl_queryable_system!(ServiceSystemApi);

/// Extra parameters necessary when cleaning up after contract execution.
pub struct WasmerContractExtra<'storage> {
    storage_guard: StorageGuard<'storage, &'static dyn WritableStorage>,
    instance: Instance,
}

/// A guard to unsure that the [`WritableStorage`] trait object isn't called after it's no longer
/// borrowed.
pub struct StorageGuard<'storage, S> {
    storage: Arc<Mutex<Option<S>>>,
    _lifetime: PhantomData<&'storage ()>,
}

impl<S> Drop for StorageGuard<'_, S> {
    fn drop(&mut self) {
        self.storage
            .try_lock()
            .expect("Guard dropped while storage is still in use")
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
