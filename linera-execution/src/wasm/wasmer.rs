// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

// Export the writable system interface used by a user contract.
wit_bindgen_host_wasmer_rust::export!("../linera-sdk/writable_system.wit");

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

use super::{
    async_boundary::{ContextForwarder, HostFuture},
    async_determinism::{HostFutureQueue, QueuedHostFutureFactory},
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};
use crate::{CallResult, ExecutionError, QueryableStorage, SessionId, WritableStorage};
use linera_views::{common::Batch, views::ViewError};
use std::{marker::PhantomData, mem, sync::Arc, task::Poll};
use tokio::sync::{oneshot, Mutex};
use wasmer::{
    imports, wasmparser::Operator, CompilerConfig, Cranelift, EngineBuilder, Instance, Module,
    RuntimeError, Store,
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
        let mut compiler_config = Cranelift::default();
        compiler_config.push_middleware(metering);

        let mut store = Store::new(EngineBuilder::new(compiler_config));
        let module = Module::new(&store, &self.contract_bytecode)
            .map_err(wit_bindgen_host_wasmer_rust::anyhow::Error::from)?;
        let mut imports = imports! {};
        let context_forwarder = ContextForwarder::default();
        let (future_queue, queued_future_factory) = HostFutureQueue::new();
        let (internal_error_sender, internal_error_receiver) = oneshot::channel();
        let (system_api, storage_guard) = SystemApi::new_writable(
            context_forwarder.clone(),
            storage,
            queued_future_factory,
            internal_error_sender,
        );
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
            context_forwarder,
            application,
            future_queue,
            internal_error_receiver,
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
        let context_forwarder = ContextForwarder::default();
        let (future_queue, _queued_future_factory) = HostFutureQueue::new();
        let (internal_error_sender, internal_error_receiver) = oneshot::channel();
        let (system_api, storage_guard) =
            SystemApi::new_queryable(context_forwarder.clone(), storage, internal_error_sender);
        let system_api_setup =
            queryable_system::add_to_imports(&mut store, &mut imports, system_api);
        let (service, instance) = service::Service::instantiate(&mut store, &module, &mut imports)?;
        let application = Service {
            service,
            _lifetime: PhantomData,
        };

        system_api_setup(&instance, &store)?;

        Ok(WasmRuntimeContext {
            context_forwarder,
            application,
            future_queue,
            internal_error_receiver,
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
    type CallApplication = contract::CallApplication;
    type CallSession = contract::CallSession;
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

    fn call_application_new(
        &self,
        store: &mut Store,
        context: contract::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallApplication, RuntimeError> {
        contract::Contract::call_application_new(
            &self.contract,
            store,
            context,
            argument,
            forwarded_sessions,
        )
    }

    fn call_application_poll(
        &self,
        store: &mut Store,
        future: &contract::CallApplication,
    ) -> Result<contract::PollCallApplication, RuntimeError> {
        contract::Contract::call_application_poll(&self.contract, store, future)
    }

    fn call_session_new(
        &self,
        store: &mut Store,
        context: contract::CalleeContext,
        session: contract::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[contract::SessionId],
    ) -> Result<contract::CallSession, RuntimeError> {
        contract::Contract::call_session_new(
            &self.contract,
            store,
            context,
            session,
            argument,
            forwarded_sessions,
        )
    }

    fn call_session_poll(
        &self,
        store: &mut Store,
        future: &contract::CallSession,
    ) -> Result<contract::PollCallSession, RuntimeError> {
        contract::Contract::call_session_poll(&self.contract, store, future)
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

/// Implementation to forward system calls from the guest WASM module to the host implementation.
pub struct SystemApi<S, Q> {
    context: ContextForwarder,
    storage: Arc<Mutex<Option<S>>>,
    queued_future_factory: Q,
    internal_error_sender: Option<oneshot::Sender<ExecutionError>>,
}

impl SystemApi<&'static dyn WritableStorage, QueuedHostFutureFactory<'static>> {
    /// Create a new [`SystemApi`] instance, ensuring that the lifetime of the [`WritableStorage`]
    /// trait object is respected.
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
    pub fn new_writable<'storage>(
        context: ContextForwarder,
        storage: &'storage dyn WritableStorage,
        queued_future_factory: QueuedHostFutureFactory<'static>,
        internal_error_sender: oneshot::Sender<ExecutionError>,
    ) -> (Self, StorageGuard<'storage, &'static dyn WritableStorage>) {
        let storage_without_lifetime = unsafe { mem::transmute(storage) };
        let storage = Arc::new(Mutex::new(Some(storage_without_lifetime)));

        let guard = StorageGuard {
            storage: storage.clone(),
            _lifetime: PhantomData,
        };

        (
            SystemApi {
                context,
                storage,
                queued_future_factory,
                internal_error_sender: Some(internal_error_sender),
            },
            guard,
        )
    }
}

impl SystemApi<&'static dyn QueryableStorage, ()> {
    /// Same as `new_writable`. Didn't find how to factorizing the code.
    pub fn new_queryable<'storage>(
        context: ContextForwarder,
        storage: &'storage dyn QueryableStorage,
        internal_error_sender: oneshot::Sender<ExecutionError>,
    ) -> (Self, StorageGuard<'storage, &'static dyn QueryableStorage>) {
        let storage_without_lifetime = unsafe { mem::transmute(storage) };
        let storage = Arc::new(Mutex::new(Some(storage_without_lifetime)));

        let guard = StorageGuard {
            storage: storage.clone(),
            _lifetime: PhantomData,
        };

        (
            SystemApi {
                context,
                storage,
                queued_future_factory: (),
                internal_error_sender: Some(internal_error_sender),
            },
            guard,
        )
    }
}

impl<S: Copy, Q> SystemApi<S, Q> {
    /// Safely obtain the [`WritableStorage`] trait object instance to handle a system call.
    ///
    /// # Panics
    ///
    /// If there is a concurrent call from the WASM application (which is impossible as long as it
    /// is executed in a single thread) or if the trait object is no longer alive (or more
    /// accurately, if the [`StorageGuard`] returned by [`Self::new`] was dropped to indicate it's
    /// no longer alive).
    fn storage(&self) -> S {
        *self
            .storage
            .try_lock()
            .expect("Unexpected concurrent storage access by application")
            .as_ref()
            .expect("Application called storage after it should have stopped")
    }

    /// Reports an error to the [`GuestFuture`] responsible for executing the application.
    ///
    /// This causes the [`GuestFuture`] to return that error the next time it is polled.
    fn report_internal_error(&mut self, error: ExecutionError) {
        if let Some(sender) = self.internal_error_sender.take() {
            sender
                .send(error)
                .expect("Internal error receiver has unexpectedly been dropped");
        }
    }
}

impl writable_system::WritableSystem
    for SystemApi<&'static dyn WritableStorage, QueuedHostFutureFactory<'static>>
{
    type Load = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;
    type LoadAndLock = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;
    type Lock = HostFuture<'static, Result<(), ExecutionError>>;
    type ReadKeyBytes = HostFuture<'static, Result<Option<Vec<u8>>, ExecutionError>>;
    type FindKeys = HostFuture<'static, Result<Vec<Vec<u8>>, ExecutionError>>;
    type FindKeyValues = HostFuture<'static, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;
    type WriteBatch = HostFuture<'static, Result<(), ExecutionError>>;
    type TryCallApplication = HostFuture<'static, Result<CallResult, ExecutionError>>;
    type TryCallSession = HostFuture<'static, Result<CallResult, ExecutionError>>;

    fn chain_id(&mut self) -> writable_system::ChainId {
        self.storage().chain_id().into()
    }

    fn application_id(&mut self) -> writable_system::ApplicationId {
        self.storage().application_id().into()
    }

    fn application_parameters(&mut self) -> Vec<u8> {
        self.storage().application_parameters()
    }

    fn read_system_balance(&mut self) -> writable_system::SystemBalance {
        self.storage().read_system_balance().into()
    }

    fn read_system_timestamp(&mut self) -> writable_system::Timestamp {
        self.storage().read_system_timestamp().micros()
    }

    fn load_new(&mut self) -> Self::Load {
        self.queued_future_factory
            .enqueue(self.storage().try_read_my_state())
    }

    fn load_poll(&mut self, future: &Self::Load) -> writable_system::PollLoad {
        use writable_system::PollLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(bytes),
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollLoad::Pending
            }
        }
    }

    fn load_and_lock_new(&mut self) -> Self::LoadAndLock {
        self.queued_future_factory
            .enqueue(self.storage().try_read_and_lock_my_state())
    }

    fn load_and_lock_poll(
        &mut self,
        future: &Self::LoadAndLock,
    ) -> writable_system::PollLoadAndLock {
        use writable_system::PollLoadAndLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoadAndLock::Pending,
            Poll::Ready(Ok(bytes)) => PollLoadAndLock::Ready(Some(bytes)),
            Poll::Ready(Err(ExecutionError::ViewError(ViewError::NotFound(_)))) => {
                PollLoadAndLock::Ready(None)
            }
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollLoadAndLock::Pending
            }
        }
    }

    fn store_and_unlock(&mut self, state: &[u8]) -> bool {
        self.storage()
            .save_and_unlock_my_state(state.to_owned())
            .is_ok()
    }

    fn lock_new(&mut self) -> Self::Lock {
        self.queued_future_factory
            .enqueue(self.storage().lock_view_user_state())
    }

    fn lock_poll(&mut self, future: &Self::Lock) -> writable_system::PollLock {
        use writable_system::PollLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLock::Pending,
            Poll::Ready(Ok(())) => PollLock::ReadyLocked,
            Poll::Ready(Err(ExecutionError::ViewError(ViewError::TryLockError(_)))) => {
                PollLock::ReadyNotLocked
            }
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollLock::Pending
            }
        }
    }

    fn read_key_bytes_new(&mut self, key: &[u8]) -> Self::ReadKeyBytes {
        self.queued_future_factory
            .enqueue(self.storage().read_key_bytes(key.to_owned()))
    }

    fn read_key_bytes_poll(
        &mut self,
        future: &Self::ReadKeyBytes,
    ) -> writable_system::PollReadKeyBytes {
        use writable_system::PollReadKeyBytes;
        match future.poll(&mut self.context) {
            Poll::Pending => PollReadKeyBytes::Pending,
            Poll::Ready(Ok(opt_list)) => PollReadKeyBytes::Ready(opt_list),
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollReadKeyBytes::Pending
            }
        }
    }

    fn find_keys_new(&mut self, key_prefix: &[u8]) -> Self::FindKeys {
        self.queued_future_factory
            .enqueue(self.storage().find_keys_by_prefix(key_prefix.to_owned()))
    }

    fn find_keys_poll(&mut self, future: &Self::FindKeys) -> writable_system::PollFindKeys {
        use writable_system::PollFindKeys;
        match future.poll(&mut self.context) {
            Poll::Pending => PollFindKeys::Pending,
            Poll::Ready(Ok(keys)) => PollFindKeys::Ready(keys),
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollFindKeys::Pending
            }
        }
    }

    fn find_key_values_new(&mut self, key_prefix: &[u8]) -> Self::FindKeyValues {
        self.queued_future_factory.enqueue(
            self.storage()
                .find_key_values_by_prefix(key_prefix.to_owned()),
        )
    }

    fn find_key_values_poll(
        &mut self,
        future: &Self::FindKeyValues,
    ) -> writable_system::PollFindKeyValues {
        use writable_system::PollFindKeyValues;
        match future.poll(&mut self.context) {
            Poll::Pending => PollFindKeyValues::Pending,
            Poll::Ready(Ok(key_values)) => PollFindKeyValues::Ready(key_values),
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollFindKeyValues::Pending
            }
        }
    }

    fn write_batch_new(
        &mut self,
        list_oper: Vec<writable_system::WriteOperation>,
    ) -> Self::WriteBatch {
        let mut batch = Batch::new();
        for x in list_oper {
            match x {
                writable_system::WriteOperation::Delete(key) => batch.delete_key(key.to_vec()),
                writable_system::WriteOperation::Deleteprefix(key_prefix) => {
                    batch.delete_key_prefix(key_prefix.to_vec())
                }
                writable_system::WriteOperation::Put(key_value) => {
                    batch.put_key_value_bytes(key_value.0.to_vec(), key_value.1.to_vec())
                }
            }
        }
        self.queued_future_factory
            .enqueue(self.storage().write_batch_and_unlock(batch))
    }

    fn write_batch_poll(&mut self, future: &Self::WriteBatch) -> writable_system::PollUnit {
        use writable_system::PollUnit;
        match future.poll(&mut self.context) {
            Poll::Pending => PollUnit::Pending,
            Poll::Ready(Ok(())) => PollUnit::Ready,
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollUnit::Pending
            }
        }
    }

    fn try_call_application_new(
        &mut self,
        authenticated: bool,
        application: writable_system::ApplicationId,
        argument: &[u8],
        forwarded_sessions: &[Le<writable_system::SessionId>],
    ) -> Self::TryCallApplication {
        let storage = self.storage();
        let forwarded_sessions = forwarded_sessions
            .iter()
            .map(Le::get)
            .map(SessionId::from)
            .collect();
        let argument = Vec::from(argument);

        self.queued_future_factory.enqueue(async move {
            storage
                .try_call_application(
                    authenticated,
                    application.into(),
                    &argument,
                    forwarded_sessions,
                )
                .await
        })
    }

    fn try_call_application_poll(
        &mut self,
        future: &Self::TryCallApplication,
    ) -> writable_system::PollCallResult {
        use writable_system::PollCallResult;
        match future.poll(&mut self.context) {
            Poll::Pending => PollCallResult::Pending,
            Poll::Ready(Ok(result)) => PollCallResult::Ready(result.into()),
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollCallResult::Pending
            }
        }
    }

    fn try_call_session_new(
        &mut self,
        authenticated: bool,
        session: writable_system::SessionId,
        argument: &[u8],
        forwarded_sessions: &[Le<writable_system::SessionId>],
    ) -> Self::TryCallApplication {
        let storage = self.storage();
        let forwarded_sessions = forwarded_sessions
            .iter()
            .map(Le::get)
            .map(SessionId::from)
            .collect();
        let argument = Vec::from(argument);

        self.queued_future_factory.enqueue(async move {
            storage
                .try_call_session(authenticated, session.into(), &argument, forwarded_sessions)
                .await
        })
    }

    fn try_call_session_poll(
        &mut self,
        future: &Self::TryCallApplication,
    ) -> writable_system::PollCallResult {
        use writable_system::PollCallResult;
        match future.poll(&mut self.context) {
            Poll::Pending => PollCallResult::Pending,
            Poll::Ready(Ok(result)) => PollCallResult::Ready(result.into()),
            Poll::Ready(Err(error)) => {
                self.report_internal_error(error);
                PollCallResult::Pending
            }
        }
    }

    fn log(&mut self, message: &str, level: writable_system::LogLevel) {
        log::log!(level.into(), "{message}");
    }
}

impl queryable_system::QueryableSystem for SystemApi<&'static dyn QueryableStorage, ()> {
    type Load = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;
    type Lock = HostFuture<'static, Result<(), ExecutionError>>;
    type Unlock = HostFuture<'static, Result<(), ExecutionError>>;
    type ReadKeyBytes = HostFuture<'static, Result<Option<Vec<u8>>, ExecutionError>>;
    type FindKeys = HostFuture<'static, Result<Vec<Vec<u8>>, ExecutionError>>;
    type FindKeyValues = HostFuture<'static, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;
    type TryQueryApplication = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;

    fn chain_id(&mut self) -> queryable_system::ChainId {
        self.storage().chain_id().into()
    }

    fn application_id(&mut self) -> queryable_system::ApplicationId {
        self.storage().application_id().into()
    }

    fn application_parameters(&mut self) -> Vec<u8> {
        self.storage().application_parameters()
    }

    fn read_system_balance(&mut self) -> queryable_system::SystemBalance {
        self.storage().read_system_balance().into()
    }

    fn read_system_timestamp(&mut self) -> queryable_system::Timestamp {
        self.storage().read_system_timestamp().micros()
    }

    fn load_new(&mut self) -> Self::Load {
        HostFuture::new(self.storage().try_read_my_state())
    }

    fn load_poll(&mut self, future: &Self::Load) -> queryable_system::PollLoad {
        use queryable_system::PollLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn lock_new(&mut self) -> Self::Lock {
        HostFuture::new(self.storage().lock_view_user_state())
    }

    fn lock_poll(&mut self, future: &Self::Lock) -> queryable_system::PollLock {
        use queryable_system::PollLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLock::Pending,
            Poll::Ready(Ok(())) => PollLock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollLock::Ready(Err(error.to_string())),
        }
    }

    fn unlock_new(&mut self) -> Self::Unlock {
        HostFuture::new(self.storage().unlock_view_user_state())
    }

    fn unlock_poll(&mut self, future: &Self::Lock) -> queryable_system::PollUnlock {
        use queryable_system::PollUnlock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollUnlock::Pending,
            Poll::Ready(Ok(())) => PollUnlock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollUnlock::Ready(Err(error.to_string())),
        }
    }

    fn read_key_bytes_new(&mut self, key: &[u8]) -> Self::ReadKeyBytes {
        HostFuture::new(self.storage().read_key_bytes(key.to_owned()))
    }

    fn read_key_bytes_poll(
        &mut self,
        future: &Self::ReadKeyBytes,
    ) -> queryable_system::PollReadKeyBytes {
        use queryable_system::PollReadKeyBytes;
        match future.poll(&mut self.context) {
            Poll::Pending => PollReadKeyBytes::Pending,
            Poll::Ready(Ok(opt_list)) => PollReadKeyBytes::Ready(Ok(opt_list)),
            Poll::Ready(Err(error)) => PollReadKeyBytes::Ready(Err(error.to_string())),
        }
    }

    fn find_keys_new(&mut self, key_prefix: &[u8]) -> Self::FindKeys {
        HostFuture::new(self.storage().find_keys_by_prefix(key_prefix.to_owned()))
    }

    fn find_keys_poll(&mut self, future: &Self::FindKeys) -> queryable_system::PollFindKeys {
        use queryable_system::PollFindKeys;
        match future.poll(&mut self.context) {
            Poll::Pending => PollFindKeys::Pending,
            Poll::Ready(Ok(keys)) => PollFindKeys::Ready(Ok(keys)),
            Poll::Ready(Err(error)) => PollFindKeys::Ready(Err(error.to_string())),
        }
    }

    fn find_key_values_new(&mut self, key_prefix: &[u8]) -> Self::FindKeyValues {
        HostFuture::new(
            self.storage()
                .find_key_values_by_prefix(key_prefix.to_owned()),
        )
    }

    fn find_key_values_poll(
        &mut self,
        future: &Self::FindKeyValues,
    ) -> queryable_system::PollFindKeyValues {
        use queryable_system::PollFindKeyValues;
        match future.poll(&mut self.context) {
            Poll::Pending => PollFindKeyValues::Pending,
            Poll::Ready(Ok(key_values)) => PollFindKeyValues::Ready(Ok(key_values)),
            Poll::Ready(Err(error)) => PollFindKeyValues::Ready(Err(error.to_string())),
        }
    }

    fn try_query_application_new(
        &mut self,
        application: queryable_system::ApplicationId,
        argument: &[u8],
    ) -> Self::TryQueryApplication {
        let storage = self.storage();
        let argument = Vec::from(argument);

        HostFuture::new(async move {
            storage
                .try_query_application(application.into(), &argument)
                .await
        })
    }

    fn try_query_application_poll(
        &mut self,
        future: &Self::TryQueryApplication,
    ) -> queryable_system::PollLoad {
        use queryable_system::PollLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(result)) => PollLoad::Ready(Ok(result)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn log(&mut self, message: &str, level: queryable_system::LogLevel) {
        log::log!(level.into(), "{message}");
    }
}

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
