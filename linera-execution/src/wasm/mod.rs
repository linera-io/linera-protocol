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

use linera_base::data_types::Bytecode;
#[cfg(with_wasmer)]
use wasmer::{WasmerContractInstance, WasmerServiceInstance};
#[cfg(with_wasmtime)]
use wasmtime::{WasmtimeContractInstance, WasmtimeServiceInstance};
#[cfg(with_metrics)]
use {
    crate::{CONTRACT_INSTANTIATION_LATENCY, SERVICE_INSTANTIATION_LATENCY},
    linera_base::prometheus_util::MeasureLatency as _,
};

use self::sanitizer::sanitize;
pub use self::{
    entrypoints::{ContractEntrypoints, ServiceEntrypoints},
    system_api::{ContractSystemApi, ServiceSystemApi, SystemApiData, ViewSystemApi},
};
use crate::{
    ContractSyncRuntimeHandle, ExecutionError, ServiceSyncRuntimeHandle, UserContractInstance,
    UserContractModule, UserServiceInstance, UserServiceModule, VmExecutionError, WasmRuntime,
};

/// A user contract in a compiled WebAssembly module.
#[derive(Clone)]
pub enum WasmContractModule {
    #[cfg(with_wasmer)]
    Wasmer {
        engine: ::wasmer::Engine,
        module: ::wasmer::Module,
    },
    #[cfg(with_wasmtime)]
    Wasmtime { module: ::wasmtime::Module },
}

impl WasmContractModule {
    /// Creates a new [`WasmContractModule`] using the WebAssembly module with the provided bytecodes.
    pub async fn new(
        contract_bytecode: Bytecode,
        runtime: WasmRuntime,
    ) -> Result<Self, VmExecutionError> {
        let contract_bytecode = if runtime.needs_sanitizer() {
            // Ensure bytecode normalization whenever wasmer and wasmtime are possibly
            // compared.
            sanitize(contract_bytecode).map_err(VmExecutionError::LoadContractModule)?
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
    ) -> Result<Self, VmExecutionError> {
        Self::new(
            Bytecode::load_from_file(contract_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(VmExecutionError::LoadContractModule)?,
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
    Wasmer { module: ::wasmer::Module },
    #[cfg(with_wasmtime)]
    Wasmtime { module: ::wasmtime::Module },
}

impl WasmServiceModule {
    /// Creates a new [`WasmServiceModule`] using the WebAssembly module with the provided bytecodes.
    pub async fn new(
        service_bytecode: Bytecode,
        runtime: WasmRuntime,
    ) -> Result<Self, VmExecutionError> {
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
    ) -> Result<Self, VmExecutionError> {
        Self::new(
            Bytecode::load_from_file(service_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(VmExecutionError::LoadServiceModule)?,
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

#[cfg(web)]
const _: () = {
    use js_sys::wasm_bindgen::JsValue;

    impl TryFrom<JsValue> for WasmServiceModule {
        type Error = JsValue;

        fn try_from(value: JsValue) -> Result<Self, JsValue> {
            // TODO(#2775): be generic over possible implementations

            cfg_if::cfg_if! {
                if #[cfg(with_wasmer)] {
                    Ok(Self::Wasmer {
                        module: value.try_into()?,
                    })
                } else {
                    Err(value)
                }
            }
        }
    }

    impl From<WasmServiceModule> for JsValue {
        fn from(module: WasmServiceModule) -> JsValue {
            match module {
                #[cfg(with_wasmer)]
                WasmServiceModule::Wasmer { module } => ::wasmer::Module::clone(&module).into(),
            }
        }
    }

    impl TryFrom<JsValue> for WasmContractModule {
        type Error = JsValue;

        fn try_from(value: JsValue) -> Result<Self, JsValue> {
            // TODO(#2775): be generic over possible implementations
            cfg_if::cfg_if! {
                if #[cfg(with_wasmer)] {
                    Ok(Self::Wasmer {
                        module: value.try_into()?,
                        engine: Default::default(),
                    })
                } else {
                    Err(value)
                }
            }
        }
    }

    impl From<WasmContractModule> for JsValue {
        fn from(module: WasmContractModule) -> JsValue {
            match module {
                #[cfg(with_wasmer)]
                WasmContractModule::Wasmer { module, engine: _ } => {
                    ::wasmer::Module::clone(&module).into()
                }
            }
        }
    }
};

#[cfg(with_wasmer)]
impl From<::wasmer::InstantiationError> for VmExecutionError {
    fn from(instantiation_error: ::wasmer::InstantiationError) -> Self {
        VmExecutionError::InstantiateModuleWithWasmer(Box::new(instantiation_error))
    }
}

/// This assumes that the current directory is one of the crates.
#[cfg(with_testing)]
pub mod test {
    use std::sync::LazyLock;

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
        static INSTANCE: LazyLock<()> = LazyLock::new(|| build_applications().unwrap());
        LazyLock::force(&INSTANCE);
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
