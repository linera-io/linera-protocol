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
    async_boundary::{ContextForwarder, HostFuture, HostFutureQueue},
    common::{self, ApplicationRuntimeContext, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};
use crate::{CallResult, ExecutionError, QueryableStorage, SessionId, WritableStorage};
use linera_views::common::Batch;
use std::{marker::PhantomData, mem, sync::Arc, task::Poll};
use tokio::sync::Mutex;
use wasmer::{imports, Module, RuntimeError, Store};
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
    type StorageGuard = StorageGuard<'storage, &'static dyn WritableStorage>;
    type Error = RuntimeError;
}

/// Type representing the [Wasmer](https://wasmer.io/) service runtime.
pub struct Service<'storage> {
    service: service::Service,
    _lifetime: PhantomData<&'storage ()>,
}

impl<'storage> ApplicationRuntimeContext for Service<'storage> {
    type Store = Store;
    type StorageGuard = StorageGuard<'storage, &'static dyn QueryableStorage>;
    type Error = RuntimeError;
}

impl WasmApplication {
    /// Prepare a runtime instance to call into the WASM contract.
    pub fn prepare_contract_runtime_with_wasmer<'storage>(
        &self,
        storage: &'storage dyn WritableStorage,
    ) -> Result<WasmRuntimeContext<Contract<'storage>>, WasmExecutionError> {
        let mut store = Store::default();
        let module = Module::new(&store, &self.contract_bytecode)
            .map_err(wit_bindgen_host_wasmer_rust::anyhow::Error::from)?;
        let mut imports = imports! {};
        let context_forwarder = ContextForwarder::default();
        let (system_api, storage_guard) =
            SystemApi::new_writable(context_forwarder.clone(), storage);
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
            store,
            _storage_guard: storage_guard,
        })
    }

    /// Prepare a runtime instance to call into the WASM service.
    pub fn prepare_service_runtime_with_wasmer<'storage>(
        &self,
        storage: &'storage dyn QueryableStorage,
    ) -> Result<WasmRuntimeContext<Service<'storage>>, WasmExecutionError> {
        let mut store = Store::default();
        let module = Module::new(&store, &self.service_bytecode)
            .map_err(wit_bindgen_host_wasmer_rust::anyhow::Error::from)?;
        let mut imports = imports! {};
        let context_forwarder = ContextForwarder::default();
        let (system_api, storage_guard) =
            SystemApi::new_queryable(context_forwarder.clone(), storage);
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
            store,
            _storage_guard: storage_guard,
        })
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
pub struct SystemApi<S> {
    context: ContextForwarder,
    storage: Arc<Mutex<Option<S>>>,
    future_queue: HostFutureQueue,
}

impl SystemApi<&'static dyn WritableStorage> {
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
    pub fn new_writable(
        context: ContextForwarder,
        storage: &dyn WritableStorage,
    ) -> (Self, StorageGuard<'_, &'static dyn WritableStorage>) {
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
                future_queue: HostFutureQueue::new(),
            },
            guard,
        )
    }
}

impl SystemApi<&'static dyn QueryableStorage> {
    /// Same as `new_writable`. Didn't find how to factorizing the code.
    pub fn new_queryable(
        context: ContextForwarder,
        storage: &dyn QueryableStorage,
    ) -> (Self, StorageGuard<'_, &'static dyn QueryableStorage>) {
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
                future_queue: HostFutureQueue::new(),
            },
            guard,
        )
    }
}

impl<S: Copy> SystemApi<S> {
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
}

impl writable_system::WritableSystem for SystemApi<&'static dyn WritableStorage> {
    type SimpleLoad = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;
    type SimpleLoadAndLock = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;
    type ViewLock = HostFuture<'static, Result<(), ExecutionError>>;
    type ViewReadKeyBytes = HostFuture<'static, Result<Option<Vec<u8>>, ExecutionError>>;
    type ViewFindKeys = HostFuture<'static, Result<Vec<Vec<u8>>, ExecutionError>>;
    type ViewFindKeyValues = HostFuture<'static, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;
    type ViewWriteBatch = HostFuture<'static, Result<(), ExecutionError>>;
    type TryCallApplication = HostFuture<'static, Result<CallResult, ExecutionError>>;
    type TryCallSession = HostFuture<'static, Result<CallResult, ExecutionError>>;

    fn chain_id(&mut self) -> writable_system::ChainId {
        self.storage().chain_id().into()
    }

    fn application_id(&mut self) -> writable_system::ApplicationId {
        self.storage().application_id().into()
    }

    fn read_system_balance(&mut self) -> writable_system::SystemBalance {
        self.storage().read_system_balance().into()
    }

    fn read_system_timestamp(&mut self) -> writable_system::Timestamp {
        self.storage().read_system_timestamp().micros()
    }

    fn simple_load_new(&mut self) -> Self::SimpleLoad {
        self.future_queue.add(self.storage().try_read_my_state())
    }

    fn simple_load_poll(&mut self, future: &Self::SimpleLoad) -> writable_system::PollSimpleLoad {
        use writable_system::PollSimpleLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollSimpleLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollSimpleLoad::Ready(Err(error.to_string())),
        }
    }

    fn simple_load_and_lock_new(&mut self) -> Self::SimpleLoadAndLock {
        self.future_queue
            .add(self.storage().try_read_and_lock_my_state())
    }

    fn simple_load_and_lock_poll(
        &mut self,
        future: &Self::SimpleLoadAndLock,
    ) -> writable_system::PollSimpleLoad {
        use writable_system::PollSimpleLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollSimpleLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollSimpleLoad::Ready(Err(error.to_string())),
        }
    }

    fn simple_store_and_unlock(&mut self, state: &[u8]) -> bool {
        self.storage()
            .save_and_unlock_my_state(state.to_owned())
            .is_ok()
    }

    fn view_lock_new(&mut self) -> Self::ViewLock {
        HostFuture::new(self.storage().view_lock_user_state())
    }

    fn view_lock_poll(&mut self, future: &Self::ViewLock) -> writable_system::PollViewLock {
        use writable_system::PollViewLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewLock::Pending,
            Poll::Ready(Ok(())) => PollViewLock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollViewLock::Ready(Err(error.to_string())),
        }
    }

    fn view_read_key_bytes_new(&mut self, key: &[u8]) -> Self::ViewReadKeyBytes {
        HostFuture::new(self.storage().view_read_key_bytes(key.to_owned()))
    }

    fn view_read_key_bytes_poll(
        &mut self,
        future: &Self::ViewReadKeyBytes,
    ) -> writable_system::PollViewReadKeyBytes {
        use writable_system::PollViewReadKeyBytes;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewReadKeyBytes::Pending,
            Poll::Ready(Ok(opt_list)) => PollViewReadKeyBytes::Ready(Ok(opt_list)),
            Poll::Ready(Err(error)) => PollViewReadKeyBytes::Ready(Err(error.to_string())),
        }
    }

    fn view_find_keys_new(&mut self, key_prefix: &[u8]) -> Self::ViewFindKeys {
        HostFuture::new(
            self.storage()
                .view_find_keys_by_prefix(key_prefix.to_owned()),
        )
    }

    fn view_find_keys_poll(
        &mut self,
        future: &Self::ViewFindKeys,
    ) -> writable_system::PollViewFindKeys {
        use writable_system::PollViewFindKeys;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewFindKeys::Pending,
            Poll::Ready(Ok(keys)) => PollViewFindKeys::Ready(Ok(keys)),
            Poll::Ready(Err(error)) => PollViewFindKeys::Ready(Err(error.to_string())),
        }
    }

    fn view_find_key_values_new(&mut self, key_prefix: &[u8]) -> Self::ViewFindKeyValues {
        HostFuture::new(
            self.storage()
                .view_find_key_values_by_prefix(key_prefix.to_owned()),
        )
    }

    fn view_find_key_values_poll(
        &mut self,
        future: &Self::ViewFindKeyValues,
    ) -> writable_system::PollViewFindKeyValues {
        use writable_system::PollViewFindKeyValues;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewFindKeyValues::Pending,
            Poll::Ready(Ok(key_values)) => PollViewFindKeyValues::Ready(Ok(key_values)),
            Poll::Ready(Err(error)) => PollViewFindKeyValues::Ready(Err(error.to_string())),
        }
    }

    fn view_write_batch_new(
        &mut self,
        list_oper: Vec<writable_system::ViewWriteOperation>,
    ) -> Self::ViewWriteBatch {
        let mut batch = Batch::default();
        for x in list_oper {
            match x {
                writable_system::ViewWriteOperation::Delete(key) => batch.delete_key(key.to_vec()),
                writable_system::ViewWriteOperation::Deleteprefix(key_prefix) => {
                    batch.delete_key_prefix(key_prefix.to_vec())
                }
                writable_system::ViewWriteOperation::Put(key_value) => {
                    batch.put_key_value_bytes(key_value.0.to_vec(), key_value.1.to_vec())
                }
            }
        }
        HostFuture::new(self.storage().view_write_batch_and_unlock(batch))
    }

    fn view_write_batch_poll(
        &mut self,
        future: &Self::ViewWriteBatch,
    ) -> writable_system::PollViewWriteBatch {
        use writable_system::PollViewWriteBatch;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewWriteBatch::Pending,
            Poll::Ready(Ok(())) => PollViewWriteBatch::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollViewWriteBatch::Ready(Err(error.to_string())),
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

        self.future_queue.add(async move {
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
            Poll::Ready(Ok(result)) => PollCallResult::Ready(Ok(result.into())),
            Poll::Ready(Err(error)) => PollCallResult::Ready(Err(error.to_string())),
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

        self.future_queue.add(async move {
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
            Poll::Ready(Ok(result)) => PollCallResult::Ready(Ok(result.into())),
            Poll::Ready(Err(error)) => PollCallResult::Ready(Err(error.to_string())),
        }
    }

    fn log(&mut self, message: &str, level: writable_system::LogLevel) {
        log::log!(level.into(), "{message}");
    }
}

impl queryable_system::QueryableSystem for SystemApi<&'static dyn QueryableStorage> {
    type SimpleLoad = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;
    type ViewLock = HostFuture<'static, Result<(), ExecutionError>>;
    type ViewUnlock = HostFuture<'static, Result<(), ExecutionError>>;
    type ViewReadKeyBytes = HostFuture<'static, Result<Option<Vec<u8>>, ExecutionError>>;
    type ViewFindKeys = HostFuture<'static, Result<Vec<Vec<u8>>, ExecutionError>>;
    type ViewFindKeyValues = HostFuture<'static, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;

    fn chain_id(&mut self) -> queryable_system::ChainId {
        self.storage().chain_id().into()
    }

    fn application_id(&mut self) -> queryable_system::ApplicationId {
        self.storage().application_id().into()
    }

    fn read_system_balance(&mut self) -> queryable_system::SystemBalance {
        self.storage().read_system_balance().into()
    }

    fn read_system_timestamp(&mut self) -> queryable_system::Timestamp {
        self.storage().read_system_timestamp().micros()
    }

    fn view_lock_new(&mut self) -> Self::ViewLock {
        HostFuture::new(self.storage().view_lock_user_state())
    }

    fn simple_load_new(&mut self) -> Self::SimpleLoad {
        self.future_queue.add(self.storage().try_read_my_state())
    }

    fn simple_load_poll(&mut self, future: &Self::SimpleLoad) -> queryable_system::PollSimpleLoad {
        use queryable_system::PollSimpleLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollSimpleLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollSimpleLoad::Ready(Err(error.to_string())),
        }
    }

    fn view_lock_poll(&mut self, future: &Self::ViewLock) -> queryable_system::PollViewLock {
        use queryable_system::PollViewLock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewLock::Pending,
            Poll::Ready(Ok(())) => PollViewLock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollViewLock::Ready(Err(error.to_string())),
        }
    }

    fn view_unlock_new(&mut self) -> Self::ViewUnlock {
        HostFuture::new(self.storage().view_unlock_user_state())
    }

    fn view_unlock_poll(&mut self, future: &Self::ViewUnlock) -> queryable_system::PollViewUnlock {
        use queryable_system::PollViewUnlock;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewUnlock::Pending,
            Poll::Ready(Ok(())) => PollViewUnlock::Ready(Ok(())),
            Poll::Ready(Err(error)) => PollViewUnlock::Ready(Err(error.to_string())),
        }
    }

    fn view_read_key_bytes_new(&mut self, key: &[u8]) -> Self::ViewReadKeyBytes {
        HostFuture::new(self.storage().view_read_key_bytes(key.to_owned()))
    }

    fn view_read_key_bytes_poll(
        &mut self,
        future: &Self::ViewReadKeyBytes,
    ) -> queryable_system::PollViewReadKeyBytes {
        use queryable_system::PollViewReadKeyBytes;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewReadKeyBytes::Pending,
            Poll::Ready(Ok(opt_list)) => PollViewReadKeyBytes::Ready(Ok(opt_list)),
            Poll::Ready(Err(error)) => PollViewReadKeyBytes::Ready(Err(error.to_string())),
        }
    }

    fn view_find_keys_new(&mut self, key_prefix: &[u8]) -> Self::ViewFindKeys {
        HostFuture::new(
            self.storage()
                .view_find_keys_by_prefix(key_prefix.to_owned()),
        )
    }

    fn view_find_keys_poll(
        &mut self,
        future: &Self::ViewFindKeys,
    ) -> queryable_system::PollViewFindKeys {
        use queryable_system::PollViewFindKeys;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewFindKeys::Pending,
            Poll::Ready(Ok(keys)) => PollViewFindKeys::Ready(Ok(keys)),
            Poll::Ready(Err(error)) => PollViewFindKeys::Ready(Err(error.to_string())),
        }
    }

    fn view_find_key_values_new(&mut self, key_prefix: &[u8]) -> Self::ViewFindKeyValues {
        HostFuture::new(
            self.storage()
                .view_find_key_values_by_prefix(key_prefix.to_owned()),
        )
    }

    fn view_find_key_values_poll(
        &mut self,
        future: &Self::ViewFindKeyValues,
    ) -> queryable_system::PollViewFindKeyValues {
        use queryable_system::PollViewFindKeyValues;
        match future.poll(&mut self.context) {
            Poll::Pending => PollViewFindKeyValues::Pending,
            Poll::Ready(Ok(key_values)) => PollViewFindKeyValues::Ready(Ok(key_values)),
            Poll::Ready(Err(error)) => PollViewFindKeyValues::Ready(Err(error.to_string())),
        }
    }

    fn try_query_application_new(
        &mut self,
        application: queryable_system::ApplicationId,
        argument: &[u8],
    ) -> Self::TryQueryApplication {
        let storage = self.storage();
        let argument = Vec::from(argument);

        self.future_queue.add(async move {
            storage
                .try_query_application(application.into(), &argument)
                .await
        })
    }

    fn try_query_application_poll(
        &mut self,
        future: &Self::TryQueryApplication,
    ) -> queryable_system::PollSimpleLoad {
        use queryable_system::PollSimpleLoad;
        match future.poll(&mut self.context) {
            Poll::Pending => PollSimpleLoad::Pending,
            Poll::Ready(Ok(result)) => PollSimpleLoad::Ready(Ok(result)),
            Poll::Ready(Err(error)) => PollSimpleLoad::Ready(Err(error.to_string())),
        }
    }

    fn log(&mut self, message: &str, level: queryable_system::LogLevel) {
        log::log!(level.into(), "{message}");
    }
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
