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
#[cfg(feature = "wasmer")]
#[path = "wasmer.rs"]
mod wasmer;
#[cfg(feature = "wasmtime")]
#[path = "wasmtime.rs"]
mod wasmtime;

use crate::{
    ApplicationCallResult, Bytecode, CalleeContext, EffectContext, ExecutionError,
    OperationContext, QueryContext, QueryableStorage, RawExecutionResult, SessionCallResult,
    SessionId, UserApplication, WasmRuntime, WritableStorage,
};
use async_trait::async_trait;
use std::{io, path::Path};
use thiserror::Error;

/// A user application in a compiled WebAssembly module.
pub struct WasmApplication {
    contract_bytecode: Bytecode,
    service_bytecode: Bytecode,
    runtime: WasmRuntime,
}

impl WasmApplication {
    /// Create a new [`WasmApplication`] using the WebAssembly module with the provided bytecodes.
    pub fn new(
        contract_bytecode: Bytecode,
        service_bytecode: Bytecode,
        runtime: WasmRuntime,
    ) -> Self {
        WasmApplication {
            contract_bytecode,
            service_bytecode,
            runtime,
        }
    }

    /// Create a new [`WasmApplication`] using the WebAssembly module in `bytecode_file`.
    pub async fn from_files(
        contract_bytecode_file: impl AsRef<Path>,
        service_bytecode_file: impl AsRef<Path>,
        runtime: WasmRuntime,
    ) -> Result<Self, io::Error> {
        Ok(WasmApplication {
            contract_bytecode: Bytecode::load_from_file(contract_bytecode_file).await?,
            service_bytecode: Bytecode::load_from_file(service_bytecode_file).await?,
            runtime,
        })
    }
}

/// Errors that can occur when executing a user application in a WebAssembly module.
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[derive(Debug, Error)]
pub enum WasmExecutionError {
    #[cfg(any(feature = "wasmer", feature = "wasmtime"))]
    #[error("Failed to load WASM module")]
    LoadModule(#[from] anyhow::Error),
    #[cfg(feature = "wasmer")]
    #[error("Failed to execute WASM module (Wasmer)")]
    ExecuteModuleInWasmer(#[from] ::wasmer::RuntimeError),
    #[cfg(feature = "wasmtime")]
    #[error("Failed to execute WASM module (Wasmtime)")]
    ExecuteModuleInWasmtime(#[from] ::wasmtime::Trap),
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
        let result = match self.runtime {
            #[cfg(feature = "wasmtime")]
            WasmRuntime::Wasmtime => {
                self.prepare_contract_runtime_with_wasmtime(storage)?
                    .initialize(context, argument)
                    .await?
            }
            #[cfg(feature = "wasmer")]
            WasmRuntime::Wasmer => {
                self.prepare_contract_runtime_with_wasmer(storage)?
                    .initialize(context, argument)
                    .await?
            }
        };
        Ok(result)
    }

    async fn execute_operation(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        operation: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        let result = match self.runtime {
            #[cfg(feature = "wasmtime")]
            WasmRuntime::Wasmtime => {
                self.prepare_contract_runtime_with_wasmtime(storage)?
                    .execute_operation(context, operation)
                    .await?
            }
            #[cfg(feature = "wasmer")]
            WasmRuntime::Wasmer => {
                self.prepare_contract_runtime_with_wasmer(storage)?
                    .execute_operation(context, operation)
                    .await?
            }
        };
        Ok(result)
    }

    async fn execute_effect(
        &self,
        context: &EffectContext,
        storage: &dyn WritableStorage,
        effect: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        let result = match self.runtime {
            #[cfg(feature = "wasmtime")]
            WasmRuntime::Wasmtime => {
                self.prepare_contract_runtime_with_wasmtime(storage)?
                    .execute_effect(context, effect)
                    .await?
            }
            #[cfg(feature = "wasmer")]
            WasmRuntime::Wasmer => {
                self.prepare_contract_runtime_with_wasmer(storage)?
                    .execute_effect(context, effect)
                    .await?
            }
        };
        Ok(result)
    }

    async fn call_application(
        &self,
        context: &CalleeContext,
        storage: &dyn WritableStorage,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        let result = match self.runtime {
            #[cfg(feature = "wasmtime")]
            WasmRuntime::Wasmtime => {
                self.prepare_contract_runtime_with_wasmtime(storage)?
                    .call_application(context, argument, forwarded_sessions)
                    .await?
            }
            #[cfg(feature = "wasmer")]
            WasmRuntime::Wasmer => {
                self.prepare_contract_runtime_with_wasmer(storage)?
                    .call_application(context, argument, forwarded_sessions)
                    .await?
            }
        };
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
        let result = match self.runtime {
            #[cfg(feature = "wasmtime")]
            WasmRuntime::Wasmtime => {
                self.prepare_contract_runtime_with_wasmtime(storage)?
                    .call_session(
                        context,
                        session_kind,
                        session_data,
                        argument,
                        forwarded_sessions,
                    )
                    .await?
            }
            #[cfg(feature = "wasmer")]
            WasmRuntime::Wasmer => {
                self.prepare_contract_runtime_with_wasmer(storage)?
                    .call_session(
                        context,
                        session_kind,
                        session_data,
                        argument,
                        forwarded_sessions,
                    )
                    .await?
            }
        };
        Ok(result)
    }

    async fn query_application(
        &self,
        context: &QueryContext,
        storage: &dyn QueryableStorage,
        argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError> {
        let result = match self.runtime {
            #[cfg(feature = "wasmtime")]
            WasmRuntime::Wasmtime => {
                self.prepare_service_runtime_with_wasmtime(storage)?
                    .query_application(context, argument)
                    .await?
            }
            #[cfg(feature = "wasmer")]
            WasmRuntime::Wasmer => {
                self.prepare_service_runtime_with_wasmer(storage)?
                    .query_application(context, argument)
                    .await?
            }
        };
        Ok(result)
    }
}

/// This assumes that the current directory is one of the crates.
#[cfg(any(test, feature = "test"))]
pub mod test {
    use crate::{WasmApplication, WasmRuntime};

    fn build_applications() -> Result<(), std::io::Error> {
        log::info!("Building example applications with cargo");
        let status = std::process::Command::new("cargo")
            .current_dir("../linera-examples")
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .status()?;
        assert!(status.success());
        Ok(())
    }

    pub fn get_example_bytecode_paths(name: &str) -> Result<(String, String), std::io::Error> {
        build_applications()?;
        Ok((
            format!(
                "../linera-examples/target/wasm32-unknown-unknown/release/{name}_contract.wasm"
            ),
            format!("../linera-examples/target/wasm32-unknown-unknown/release/{name}_service.wasm"),
        ))
    }

    pub async fn build_example_application(
        name: &str,
        wasm_runtime: impl Into<Option<WasmRuntime>>,
    ) -> Result<WasmApplication, std::io::Error> {
        let (contract, service) = get_example_bytecode_paths(name)?;
        WasmApplication::from_files(&contract, &service, wasm_runtime.into().unwrap_or_default())
            .await
    }
}
