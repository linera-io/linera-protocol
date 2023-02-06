// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for user applications compiled as WebAssembly (WASM) modules.
//!
//! Requires a WebAssembly runtime to be selected and enabled using one of the following features:
//!
//! - `wasmer` enables the [Wasmer](https://wasmer.io/) runtime
//! - `wasmtime` enables the [Wasmtime](https://wasmtime.dev/) runtime

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

use crate::{
    ApplicationCallResult, Bytecode, CalleeContext, EffectContext, ExecutionError,
    OperationContext, QueryContext, QueryableStorage, RawExecutionResult, SessionCallResult,
    SessionId, UserApplication, WritableStorage,
};
use async_trait::async_trait;
use std::{io, path::Path};
use thiserror::Error;

/// A user application in a compiled WebAssembly module.
pub struct WasmApplication {
    contract_bytecode: Bytecode,
    service_bytecode: Bytecode,
}

impl WasmApplication {
    /// Create a new [`WasmApplication`] using the WebAssembly module with the provided bytecodes.
    pub fn new(contract_bytecode: Bytecode, service_bytecode: Bytecode) -> Self {
        WasmApplication {
            contract_bytecode,
            service_bytecode,
        }
    }

    /// Create a new [`WasmApplication`] using the WebAssembly module in `bytecode_file`.
    pub async fn from_files(
        contract_bytecode_file: impl AsRef<Path>,
        service_bytecode_file: impl AsRef<Path>,
    ) -> Result<Self, io::Error> {
        Ok(WasmApplication {
            contract_bytecode: Bytecode::load_from_file(contract_bytecode_file).await?,
            service_bytecode: Bytecode::load_from_file(service_bytecode_file).await?,
        })
    }
}

/// Errors that can occur when executing a user application in a WebAssembly module.
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[derive(Debug, Error)]
pub enum WasmExecutionError {
    #[cfg(feature = "wasmer")]
    #[error("Failed to load WASM module")]
    LoadModule(#[from] wit_bindgen_host_wasmer_rust::anyhow::Error),
    #[cfg(feature = "wasmtime")]
    #[error("Failed to load WASM module")]
    LoadModule(#[from] wit_bindgen_host_wasmtime_rust::anyhow::Error),
    #[cfg(feature = "wasmer")]
    #[error("Failed to execute WASM module")]
    ExecuteModule(#[from] wasmer::RuntimeError),
    #[cfg(feature = "wasmtime")]
    #[error("Failed to execute WASM module")]
    ExecuteModule(#[from] wasmtime::Trap),
    #[error("Error reported from user application: {0}")]
    UserApplication(String),
}

#[async_trait]
impl UserApplication for WasmApplication {
    async fn initialize(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        argument: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        let result = self
            .prepare_contract_runtime(storage)?
            .initialize(context, argument)
            .await?;
        Ok(result)
    }

    async fn execute_operation(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        operation: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        let result = self
            .prepare_contract_runtime(storage)?
            .execute_operation(context, operation)
            .await?;
        Ok(result)
    }

    async fn execute_effect(
        &self,
        context: &EffectContext,
        storage: &dyn WritableStorage,
        effect: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        let result = self
            .prepare_contract_runtime(storage)?
            .execute_effect(context, effect)
            .await?;
        Ok(result)
    }

    async fn call_application(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        let result = self
            .prepare_contract_runtime(storage)?
            .call_application(context, argument, forwarded_sessions)
            .await?;
        Ok(result)
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
        let result = self
            .prepare_contract_runtime(storage)?
            .call_session(
                context,
                session_kind,
                session_data,
                argument,
                forwarded_sessions,
            )
            .await?;
        Ok(result)
    }

    async fn query_application(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorage,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError> {
        let result = self
            .prepare_service_runtime(storage)?
            .query_application(context, argument)
            .await?;
        Ok(result)
    }
}

/// This assumes that the current directory is one of the crates.
#[cfg(any(test, feature = "test"))]
pub mod test {

    use crate::WasmApplication;

    fn build_applications() -> Result<(), std::io::Error> {
        log::info!("Building example applications with cargo");
        let status = std::process::Command::new("cargo")
            .current_dir("../linera-examples")
            .env("RUSTFLAGS", "-C opt-level=z -C debuginfo=0")
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .status()?;
        assert!(status.success());
        Ok(())
    }

    pub fn get_counter_bytecode_paths() -> Result<(&'static str, &'static str), std::io::Error> {
        build_applications()?;
        Ok((
            "../linera-examples/target/wasm32-unknown-unknown/release/examples/counter_contract.wasm",
            "../linera-examples/target/wasm32-unknown-unknown/release/examples/counter_service.wasm",
        ))
    }

    pub async fn build_counter_application() -> Result<WasmApplication, std::io::Error> {
        let (contract, service) = get_counter_bytecode_paths()?;
        WasmApplication::from_files(contract, service).await
    }
}
