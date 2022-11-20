// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmtime](https://wasmtime.dev/) runtime.

// Export the system interface used by a user application.
wit_bindgen_host_wasmtime_rust::export!("../linera-sdk/system.wit");

// Import the interface implemented by a user application.
wit_bindgen_host_wasmtime_rust::import!("../linera-sdk/application.wit");

use self::{
    application::{Application, ApplicationData},
    system::{PollLoad, SystemTables},
};
use super::{
    async_boundary::{ContextForwarder, HostFuture},
    common::{self, Runtime, WasmRuntimeContext},
    WasmApplication, WasmExecutionError,
};
use crate::{ExecutionError, QueryableStorage, WritableStorage};
use std::{marker::PhantomData, task::Poll};
use wasmtime::{Engine, Linker, Module, Store, Trap};

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for contracts.
///
/// The runtime has a lifetime so that it does not outlive the trait object used to export the
/// system API.
pub struct ContractWasmtime<'storage> {
    _lifetime: PhantomData<&'storage ()>,
}

impl<'storage> Runtime for ContractWasmtime<'storage> {
    type Application = Application<ContractData<'storage>>;
    type Store = Store<ContractData<'storage>>;
    type StorageGuard = ();
    type Error = Trap;
}

/// Type representing the [Wasmtime](https://wasmtime.dev/) runtime for services.
pub struct ServiceWasmtime<'storage> {
    _lifetime: PhantomData<&'storage ()>,
}

impl<'storage> Runtime for ServiceWasmtime<'storage> {
    type Application = Application<ServiceData<'storage>>;
    type Store = Store<ServiceData<'storage>>;
    type StorageGuard = ();
    type Error = Trap;
}

impl WasmApplication {
    /// Prepare a runtime instance to call into the WASM contract.
    pub fn prepare_contract_runtime<'storage>(
        &self,
        storage: &'storage dyn WritableStorage,
    ) -> Result<WasmRuntimeContext<ContractWasmtime<'storage>>, WasmExecutionError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        system::add_to_linker(&mut linker, ContractData::system_api)?;

        let module = Module::new(&engine, &self.bytecode)?;
        let context_forwarder = ContextForwarder::default();
        let data = ContractData::new(storage, context_forwarder.clone());
        let mut store = Store::new(&engine, data);
        let (application, _instance) =
            Application::instantiate(&mut store, &module, &mut linker, ContractData::application)?;

        Ok(WasmRuntimeContext {
            context_forwarder,
            application,
            store,
            _storage_guard: (),
        })
    }

    /// Prepare a runtime instance to call into the WASM service.
    pub fn prepare_service_runtime<'storage>(
        &self,
        storage: &'storage dyn QueryableStorage,
    ) -> Result<WasmRuntimeContext<ServiceWasmtime<'storage>>, WasmExecutionError> {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        system::add_to_linker(&mut linker, ServiceData::system_api)?;

        let module = Module::new(&engine, &self.bytecode)?;
        let context_forwarder = ContextForwarder::default();
        let data = ServiceData::new(storage, context_forwarder.clone());
        let mut store = Store::new(&engine, data);
        let (application, _instance) =
            Application::instantiate(&mut store, &module, &mut linker, ServiceData::application)?;

        Ok(WasmRuntimeContext {
            context_forwarder,
            application,
            store,
            _storage_guard: (),
        })
    }
}

/// Data stored by the runtime that's necessary for handling calls to and from the WASM module.
pub struct ContractData<'storage> {
    application: ApplicationData,
    system_api: SystemApi<&'storage dyn WritableStorage>,
    system_tables: SystemTables<SystemApi<&'storage dyn WritableStorage>>,
}

/// Data stored by the runtime that's necessary for handling queries to and from the WASM module.
pub struct ServiceData<'storage> {
    application: ApplicationData,
    system_api: SystemApi<&'storage dyn QueryableStorage>,
    system_tables: SystemTables<SystemApi<&'storage dyn QueryableStorage>>,
}

impl<'storage> ContractData<'storage> {
    /// Create a new instance of [`Data`].
    ///
    /// Uses `storage` to export the system API, and the `context` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(storage: &'storage dyn WritableStorage, context: ContextForwarder) -> Self {
        Self {
            application: ApplicationData::default(),
            system_api: SystemApi { storage, context },
            system_tables: SystemTables::default(),
        }
    }

    /// Obtain the runtime instance specific [`ApplicationData`].
    pub fn application(&mut self) -> &mut ApplicationData {
        &mut self.application
    }

    /// Obtain the data required by the runtime to export the system API.
    pub fn system_api(
        &mut self,
    ) -> (
        &mut SystemApi<&'storage dyn WritableStorage>,
        &mut SystemTables<SystemApi<&'storage dyn WritableStorage>>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }
}

impl<'storage> ServiceData<'storage> {
    /// Create a new instance of [`Data`].
    ///
    /// Uses `storage` to export the system API, and the `context` to be able to correctly handle
    /// asynchronous calls from the guest WASM module.
    pub fn new(storage: &'storage dyn QueryableStorage, context: ContextForwarder) -> Self {
        Self {
            application: ApplicationData::default(),
            system_api: SystemApi { storage, context },
            system_tables: SystemTables::default(),
        }
    }

    /// Obtain the runtime instance specific [`ApplicationData`].
    pub fn application(&mut self) -> &mut ApplicationData {
        &mut self.application
    }

    /// Obtain the data required by the runtime to export the system API.
    pub fn system_api(
        &mut self,
    ) -> (
        &mut SystemApi<&'storage dyn QueryableStorage>,
        &mut SystemTables<SystemApi<&'storage dyn QueryableStorage>>,
    ) {
        (&mut self.system_api, &mut self.system_tables)
    }
}

impl<'storage> common::Contract<ContractWasmtime<'storage>>
    for Application<ContractData<'storage>>
{
    fn execute_operation_new(
        &self,
        store: &mut Store<ContractData<'storage>>,
        context: application::OperationContext,
        operation: &[u8],
    ) -> Result<application::ExecuteOperation, Trap> {
        Application::execute_operation_new(self, store, context, operation)
    }

    fn execute_operation_poll(
        &self,
        store: &mut Store<ContractData<'storage>>,
        future: &application::ExecuteOperation,
    ) -> Result<application::PollExecutionResult, Trap> {
        Application::execute_operation_poll(self, store, future)
    }

    fn execute_effect_new(
        &self,
        store: &mut Store<ContractData<'storage>>,
        context: application::EffectContext,
        effect: &[u8],
    ) -> Result<application::ExecuteEffect, Trap> {
        Application::execute_effect_new(self, store, context, effect)
    }

    fn execute_effect_poll(
        &self,
        store: &mut Store<ContractData<'storage>>,
        future: &application::ExecuteEffect,
    ) -> Result<application::PollExecutionResult, Trap> {
        Application::execute_effect_poll(self, store, future)
    }

    fn call_application_new(
        &self,
        store: &mut Store<ContractData<'storage>>,
        context: application::CalleeContext,
        argument: &[u8],
        forwarded_sessions: &[application::SessionId],
    ) -> Result<application::CallApplication, Trap> {
        Application::call_application_new(self, store, context, argument, forwarded_sessions)
    }

    fn call_application_poll(
        &self,
        store: &mut Store<ContractData<'storage>>,
        future: &application::CallApplication,
    ) -> Result<application::PollCallApplication, Trap> {
        Application::call_application_poll(self, store, future)
    }

    fn call_session_new(
        &self,
        store: &mut Store<ContractData<'storage>>,
        context: application::CalleeContext,
        session: application::SessionParam,
        argument: &[u8],
        forwarded_sessions: &[application::SessionId],
    ) -> Result<application::CallSession, Trap> {
        Application::call_session_new(self, store, context, session, argument, forwarded_sessions)
    }

    fn call_session_poll(
        &self,
        store: &mut Store<ContractData<'storage>>,
        future: &application::CallSession,
    ) -> Result<application::PollCallSession, Trap> {
        Application::call_session_poll(self, store, future)
    }
}

impl<'storage> common::Service<ServiceWasmtime<'storage>> for Application<ServiceData<'storage>> {
    fn query_application_new(
        &self,
        store: &mut Store<ServiceData<'storage>>,
        context: application::QueryContext,
        argument: &[u8],
    ) -> Result<application::QueryApplication, Trap> {
        Application::query_application_new(self, store, context, argument)
    }

    fn query_application_poll(
        &self,
        store: &mut Store<ServiceData<'storage>>,
        future: &application::QueryApplication,
    ) -> Result<application::PollQuery, Trap> {
        Application::query_application_poll(self, store, future)
    }
}

/// Implementation to forward system calls from the guest WASM module to the host implementation.
pub struct SystemApi<S> {
    context: ContextForwarder,
    storage: S,
}

impl<'storage> system::System for SystemApi<&'storage dyn WritableStorage> {
    type Load = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;
    type LoadAndLock = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;

    fn load_new(&mut self) -> Self::Load {
        HostFuture::new(self.storage.try_read_my_state())
    }

    fn load_poll(&mut self, future: &Self::Load) -> PollLoad {
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn load_and_lock_new(&mut self) -> Self::LoadAndLock {
        HostFuture::new(self.storage.try_read_and_lock_my_state())
    }

    fn load_and_lock_poll(&mut self, future: &Self::LoadAndLock) -> PollLoad {
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn store_and_unlock(&mut self, state: &[u8]) -> bool {
        self.storage
            .save_and_unlock_my_state(state.to_owned())
            .is_ok()
    }
}

impl<'storage> system::System for SystemApi<&'storage dyn QueryableStorage> {
    type Load = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;
    type LoadAndLock = HostFuture<'storage, Result<Vec<u8>, ExecutionError>>;

    fn load_new(&mut self) -> Self::Load {
        HostFuture::new(self.storage.try_read_my_state())
    }

    fn load_poll(&mut self, future: &Self::Load) -> PollLoad {
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn load_and_lock_new(&mut self) -> Self::LoadAndLock {
        panic!("not available")
    }

    fn load_and_lock_poll(&mut self, future: &Self::LoadAndLock) -> PollLoad {
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn store_and_unlock(&mut self, _state: &[u8]) -> bool {
        panic!("not available")
    }
}
