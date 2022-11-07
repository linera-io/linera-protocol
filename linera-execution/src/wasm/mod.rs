#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

mod async_boundary;
mod common;
mod conversions_from_wit;
mod conversions_to_wit;
#[cfg(feature = "wasmer")]
#[path = "wasmer.rs"]
mod runtime;
#[cfg(feature = "wasmtime")]
#[path = "wasmtime.rs"]
mod runtime;

use self::common::WrappedQueryableStorage;
use crate::{
    ApplicationCallResult, CalleeContext, EffectContext, ExecutionError, OperationContext,
    QueryContext, QueryableStorage, RawExecutionResult, SessionCallResult, SessionId,
    UserApplication, WritableStorage,
};
use async_trait::async_trait;
use std::path::PathBuf;

pub struct WasmApplication {
    bytecode_file: PathBuf,
}

impl WasmApplication {
    pub fn new(bytecode_file: impl Into<PathBuf>) -> Self {
        WasmApplication {
            bytecode_file: bytecode_file.into(),
        }
    }
}

#[async_trait]
impl UserApplication for WasmApplication {
    async fn execute_operation(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        operation: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.prepare_runtime(storage)?
            .execute_operation(context, operation)
            .await
    }

    async fn execute_effect(
        &self,
        context: &EffectContext,
        storage: &dyn WritableStorage,
        effect: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        self.prepare_runtime(storage)?
            .execute_effect(context, effect)
            .await
    }

    async fn call_application(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        self.prepare_runtime(storage)?
            .call_application(context, argument, forwarded_sessions)
            .await
    }

    async fn call_session(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        session_kind: u64,
        session_data: &mut Vec<u8>,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, ExecutionError> {
        self.prepare_runtime(storage)?
            .call_session(
                context,
                session_kind,
                session_data,
                argument,
                forwarded_sessions,
            )
            .await
    }

    async fn query_application(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorage,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError> {
        let wrapped_storage = WrappedQueryableStorage::new(storage);
        let storage_reference = &wrapped_storage;
        let result = self
            .prepare_runtime(storage_reference)?
            .query_application(context, argument)
            .await;
        result
    }
}
