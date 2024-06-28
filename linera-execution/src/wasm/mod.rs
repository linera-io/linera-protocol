// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for user applications compiled as WebAssembly (Wasm) modules.
//!
//! Requires a WebAssembly runtime to be selected and enabled using one of the following features:
//!
//! - `wasmer` enables the [Wasmer](https://wasmer.io/) runtime
//! - `wasmtime` enables the [Wasmtime](https://wasmtime.dev/) runtime

#![cfg(with_wasm_runtime)]

mod entrypoints;
mod module_cache;
mod sanitizer;
#[macro_use]
mod system_api;
#[cfg(with_wasmer)]
mod wasmer;
#[cfg(with_wasmtime)]
mod wasmtime;

use std::sync::Arc;

#[cfg(with_metrics)]
use linera_base::{
    prometheus_util::{self, MeasureLatency},
    sync::Lazy,
};
#[cfg(with_metrics)]
use prometheus::HistogramVec;
use thiserror::Error;
#[cfg(with_wasmer)]
use wasmer::{WasmerContractInstance, WasmerServiceInstance};
#[cfg(with_wasmtime)]
use wasmtime::{WasmtimeContractInstance, WasmtimeServiceInstance};

use self::sanitizer::sanitize;
pub use self::{
    entrypoints::{ContractEntrypoints, ServiceEntrypoints},
    system_api::{ContractSystemApi, ServiceSystemApi, SystemApiData, ViewSystemApi},
};
use crate::{
    Bytecode, ContractSyncRuntimeHandle, ExecutionError, ServiceSyncRuntimeHandle,
    UserContractInstance, UserContractModule, UserServiceInstance, UserServiceModule, WasmRuntime,
};

#[cfg(with_metrics)]
static CONTRACT_INSTANTIATION_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "contract_instantiation_latency",
        "Contract instantiation latency",
        &[],
        Some(vec![
            0.000_1, 0.000_3, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
        ]),
    )
    .expect("Histogram creation should not fail")
});

#[cfg(with_metrics)]
static SERVICE_INSTANTIATION_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "service_instantiation_latency",
        "Service instantiation latency",
        &[],
        Some(vec![
            0.000_1, 0.000_3, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
        ]),
    )
    .expect("Histogram creation should not fail")
});

/// A user contract in a compiled WebAssembly module.
#[derive(Clone)]
pub enum WasmContractModule {
    #[cfg(with_wasmer)]
    Wasmer {
        engine: ::wasmer::Engine,
        module: ::wasmer::Module,
    },
    #[cfg(with_wasmtime)]
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
            #[cfg(with_wasmer)]
            WasmRuntime::Wasmer | WasmRuntime::WasmerWithSanitizer => {
                Self::from_wasmer(contract_bytecode).await
            }
            #[cfg(with_wasmtime)]
            WasmRuntime::Wasmtime | WasmRuntime::WasmtimeWithSanitizer => {
                Self::from_wasmtime(contract_bytecode).await
            }
        }
    }

    /// Creates a new [`WasmContractModule`] using the WebAssembly module in `bytecode_file`.
    #[cfg(with_fs)]
    pub async fn from_file(
        contract_bytecode_file: impl AsRef<std::path::Path>,
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
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<UserContractInstance, ExecutionError> {
        #[cfg(with_metrics)]
        let _instantiation_latency = CONTRACT_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserContractInstance = match self {
            #[cfg(with_wasmtime)]
            WasmContractModule::Wasmtime { module } => {
                Box::new(WasmtimeContractInstance::prepare(module, runtime)?)
            }
            #[cfg(with_wasmer)]
            WasmContractModule::Wasmer { engine, module } => Box::new(
                WasmerContractInstance::prepare(engine.clone(), module, runtime)?,
            ),
        };

        Ok(instance)
    }
}

/// A user service in a compiled WebAssembly module.
#[derive(Clone)]
pub enum WasmServiceModule {
    #[cfg(with_wasmer)]
    Wasmer { module: Arc<::wasmer::Module> },
    #[cfg(with_wasmtime)]
    Wasmtime { module: Arc<::wasmtime::Module> },
}

impl WasmServiceModule {
    /// Creates a new [`WasmServiceModule`] using the WebAssembly module with the provided bytecodes.
    pub async fn new(
        service_bytecode: Bytecode,
        runtime: WasmRuntime,
    ) -> Result<Self, WasmExecutionError> {
        match runtime {
            #[cfg(with_wasmer)]
            WasmRuntime::Wasmer | WasmRuntime::WasmerWithSanitizer => {
                Self::from_wasmer(service_bytecode).await
            }
            #[cfg(with_wasmtime)]
            WasmRuntime::Wasmtime | WasmRuntime::WasmtimeWithSanitizer => {
                Self::from_wasmtime(service_bytecode).await
            }
        }
    }

    /// Creates a new [`WasmServiceModule`] using the WebAssembly module in `bytecode_file`.
    #[cfg(with_fs)]
    pub async fn from_file(
        service_bytecode_file: impl AsRef<std::path::Path>,
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
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<UserServiceInstance, ExecutionError> {
        #[cfg(with_metrics)]
        let _instantiation_latency = SERVICE_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserServiceInstance = match self {
            #[cfg(with_wasmtime)]
            WasmServiceModule::Wasmtime { module } => {
                Box::new(WasmtimeServiceInstance::prepare(module, runtime)?)
            }
            #[cfg(with_wasmer)]
            WasmServiceModule::Wasmer { module } => {
                Box::new(WasmerServiceInstance::prepare(module, runtime)?)
            }
        };

        Ok(instance)
    }
}

/// Errors that can occur when executing a user application in a WebAssembly module.
#[cfg(any(with_wasmer, with_wasmtime))]
#[derive(Debug, Error)]
pub enum WasmExecutionError {
    #[error("Failed to load contract Wasm module: {_0}")]
    LoadContractModule(#[source] anyhow::Error),
    #[error("Failed to load service Wasm module: {_0}")]
    LoadServiceModule(#[source] anyhow::Error),
    #[cfg(with_wasmer)]
    #[error("Failed to instantiate Wasm module: {_0}")]
    InstantiateModuleWithWasmer(#[from] Box<::wasmer::InstantiationError>),
    #[cfg(with_wasmtime)]
    #[error("Failed to create and configure Wasmtime runtime")]
    CreateWasmtimeEngine(#[source] anyhow::Error),
    #[cfg(with_wasmer)]
    #[error(
        "Failed to execute Wasm module in Wasmer. This may be caused by panics or insufficient fuel. {0}"
    )]
    ExecuteModuleInWasmer(#[from] ::wasmer::RuntimeError),
    #[cfg(with_wasmtime)]
    #[error("Failed to execute Wasm module in Wasmtime: {0}")]
    ExecuteModuleInWasmtime(#[from] ::wasmtime::Trap),
    #[error("Failed to execute Wasm module: {0}")]
    ExecuteModule(#[from] linera_witty::RuntimeError),
    #[error("Attempt to wait for an unknown promise")]
    UnknownPromise,
    #[error("Attempt to call incorrect `wait` function for a promise")]
    IncorrectPromise,
}

#[cfg(with_wasmer)]
impl From<::wasmer::InstantiationError> for WasmExecutionError {
    fn from(instantiation_error: ::wasmer::InstantiationError) -> Self {
        WasmExecutionError::InstantiateModuleWithWasmer(Box::new(instantiation_error))
    }
}

/// This assumes that the current directory is one of the crates.
#[cfg(with_testing)]
pub mod test {
    use once_cell::sync::OnceCell;

    #[cfg(with_fs)]
    use super::{WasmContractModule, WasmRuntime, WasmServiceModule};

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

    #[cfg(with_fs)]
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
