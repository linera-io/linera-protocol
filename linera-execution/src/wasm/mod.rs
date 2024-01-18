// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for user applications compiled as WebAssembly (Wasm) modules.
//!
//! Requires a WebAssembly runtime to be selected and enabled using one of the following features:
//!
//! - `wasmer` enables the [Wasmer](https://wasmer.io/) runtime
//! - `wasmtime` enables the [Wasmtime](https://wasmtime.dev/) runtime

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

mod module_cache;
mod sanitizer;
#[macro_use]
mod system_api;
#[cfg(feature = "wasmer")]
#[path = "wasmer.rs"]
mod wasmer;
#[cfg(feature = "wasmtime")]
#[path = "wasmtime.rs"]
mod wasmtime;

use self::sanitizer::sanitize;
use crate::{
    Bytecode, ContractSyncRuntime, ExecutionError, ServiceSyncRuntime, UserContractInstance,
    UserContractModule, UserServiceInstance, UserServiceModule, WasmRuntime,
};
use std::{path::Path, sync::Arc};
use thiserror::Error;

#[cfg(feature = "wasmer")]
use wasmer::{WasmerContractInstance, WasmerServiceInstance};
#[cfg(feature = "wasmtime")]
use wasmtime::{WasmtimeContractInstance, WasmtimeServiceInstance};

/// A user contract in a compiled WebAssembly module.
#[derive(Clone)]
pub enum WasmContractModule {
    #[cfg(feature = "wasmer")]
    Wasmer {
        engine: ::wasmer::Engine,
        module: ::wasmer::Module,
    },
    #[cfg(feature = "wasmtime")]
    Wasmtime { module: Arc<::wasmtime::Module> },
}

impl WasmContractModule {
    /// Creates a new [`WasmContractModule`] using the WebAssembly module with the provided bytecodes.
    pub async fn new(
        contract_bytecode: Bytecode,
        runtime: WasmRuntime,
    ) -> Result<Self, WasmExecutionError> {
        let contract_bytecode = if runtime.needs_sanitizer() {
            // Ensure bytecode normalization whenever wasmer and wasmtime are possibly
            // compared.
            sanitize(contract_bytecode).map_err(WasmExecutionError::LoadContractModule)?
        } else {
            contract_bytecode
        };
        match runtime {
            #[cfg(feature = "wasmer")]
            WasmRuntime::Wasmer | WasmRuntime::WasmerWithSanitizer => {
                Self::from_wasmer(contract_bytecode).await
            }
            #[cfg(feature = "wasmtime")]
            WasmRuntime::Wasmtime | WasmRuntime::WasmtimeWithSanitizer => {
                Self::from_wasmtime(contract_bytecode).await
            }
        }
    }

    /// Creates a new [`WasmContractModule`] using the WebAssembly module in `bytecode_file`.
    pub async fn from_file(
        contract_bytecode_file: impl AsRef<Path>,
        runtime: WasmRuntime,
    ) -> Result<Self, WasmExecutionError> {
        Self::new(
            Bytecode::load_from_file(contract_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(WasmExecutionError::LoadContractModule)?,
            runtime,
        )
        .await
    }
}

impl UserContractModule for WasmContractModule {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntime,
    ) -> Result<UserContractInstance, ExecutionError> {
        match self {
            #[cfg(feature = "wasmtime")]
            WasmContractModule::Wasmtime { module } => Ok(Box::new(
                WasmtimeContractInstance::prepare(module, runtime)?,
            )),
            #[cfg(feature = "wasmer")]
            WasmContractModule::Wasmer { engine, module } => Ok(Box::new(
                WasmerContractInstance::prepare(engine, module, runtime)?,
            )),
        }
    }
}

/// A user service in a compiled WebAssembly module.
#[derive(Clone)]
pub enum WasmServiceModule {
    #[cfg(feature = "wasmer")]
    Wasmer { module: Arc<::wasmer::Module> },
    #[cfg(feature = "wasmtime")]
    Wasmtime { module: Arc<::wasmtime::Module> },
}

impl WasmServiceModule {
    /// Creates a new [`WasmServiceModule`] using the WebAssembly module with the provided bytecodes.
    pub async fn new(
        service_bytecode: Bytecode,
        runtime: WasmRuntime,
    ) -> Result<Self, WasmExecutionError> {
        match runtime {
            #[cfg(feature = "wasmer")]
            WasmRuntime::Wasmer | WasmRuntime::WasmerWithSanitizer => {
                Self::from_wasmer(service_bytecode).await
            }
            #[cfg(feature = "wasmtime")]
            WasmRuntime::Wasmtime | WasmRuntime::WasmtimeWithSanitizer => {
                Self::from_wasmtime(service_bytecode).await
            }
        }
    }

    /// Creates a new [`WasmServiceModule`] using the WebAssembly module in `bytecode_file`.
    pub async fn from_file(
        service_bytecode_file: impl AsRef<Path>,
        runtime: WasmRuntime,
    ) -> Result<Self, WasmExecutionError> {
        Self::new(
            Bytecode::load_from_file(service_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(WasmExecutionError::LoadServiceModule)?,
            runtime,
        )
        .await
    }
}

impl UserServiceModule for WasmServiceModule {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntime,
    ) -> Result<UserServiceInstance, ExecutionError> {
        match self {
            #[cfg(feature = "wasmtime")]
            WasmServiceModule::Wasmtime { module } => {
                Ok(Box::new(WasmtimeServiceInstance::prepare(module, runtime)?))
            }
            #[cfg(feature = "wasmer")]
            WasmServiceModule::Wasmer { module } => {
                Ok(Box::new(WasmerServiceInstance::prepare(module, runtime)?))
            }
        }
    }
}

/// Errors that can occur when executing a user application in a WebAssembly module.
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
#[derive(Debug, Error)]
pub enum WasmExecutionError {
    #[error("Failed to load contract Wasm module: {_0}")]
    LoadContractModule(#[source] anyhow::Error),
    #[error("Failed to load service Wasm module: {_0}")]
    LoadServiceModule(#[source] anyhow::Error),
    #[cfg(feature = "wasmtime")]
    #[error("Failed to create and configure Wasmtime runtime")]
    CreateWasmtimeEngine(#[source] anyhow::Error),
    #[cfg(feature = "wasmer")]
    #[error("Failed to execute Wasm module (Wasmer)")]
    ExecuteModuleInWasmer(#[from] ::wasmer::RuntimeError),
    #[cfg(feature = "wasmtime")]
    #[error("Failed to execute Wasm module (Wasmtime)")]
    ExecuteModuleInWasmtime(#[from] ::wasmtime::Trap),
}

/// This assumes that the current directory is one of the crates.
#[cfg(any(test, feature = "test"))]
pub mod test {
    use super::{WasmContractModule, WasmRuntime, WasmServiceModule};
    use once_cell::sync::OnceCell;

    fn build_applications() -> Result<(), std::io::Error> {
        tracing::info!("Building example applications with cargo");
        let output = std::process::Command::new("cargo")
            .current_dir("../examples")
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .output()?;
        if !output.status.success() {
            panic!(
                "Failed to build example applications.\n\n\
                stdout:\n-------\n{}\n\n\
                stderr:\n-------\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        Ok(())
    }

    pub fn get_example_bytecode_paths(name: &str) -> Result<(String, String), std::io::Error> {
        let name = name.replace('-', "_");
        static INSTANCE: OnceCell<()> = OnceCell::new();
        INSTANCE.get_or_try_init(build_applications)?;
        Ok((
            format!("../examples/target/wasm32-unknown-unknown/release/{name}_contract.wasm"),
            format!("../examples/target/wasm32-unknown-unknown/release/{name}_service.wasm"),
        ))
    }

    pub async fn build_example_application(
        name: &str,
        wasm_runtime: impl Into<Option<WasmRuntime>>,
    ) -> Result<(WasmContractModule, WasmServiceModule), anyhow::Error> {
        let (contract_path, service_path) = get_example_bytecode_paths(name)?;
        let wasm_runtime = wasm_runtime.into().unwrap_or_default();
        let contract = WasmContractModule::from_file(&contract_path, wasm_runtime).await?;
        let service = WasmServiceModule::from_file(&service_path, wasm_runtime).await?;
        Ok((contract, service))
    }
}
