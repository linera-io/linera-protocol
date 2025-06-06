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
#[macro_use]
mod runtime_api;
#[cfg(with_wasmer)]
mod wasmer;
#[cfg(with_wasmtime)]
mod wasmtime;

use linera_base::data_types::Bytecode;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use thiserror::Error;
use wasm_instrument::{gas_metering, parity_wasm};
#[cfg(with_wasmer)]
use wasmer::{WasmerContractInstance, WasmerServiceInstance};
#[cfg(with_wasmtime)]
use wasmtime::{WasmtimeContractInstance, WasmtimeServiceInstance};

pub use self::{
    entrypoints::{ContractEntrypoints, ServiceEntrypoints},
    runtime_api::{BaseRuntimeApi, ContractRuntimeApi, RuntimeApiData, ServiceRuntimeApi},
};
use crate::{
    ContractSyncRuntimeHandle, ExecutionError, ServiceSyncRuntimeHandle, UserContractInstance,
    UserContractModule, UserServiceInstance, UserServiceModule, WasmRuntime,
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    pub static CONTRACT_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "wasm_contract_instantiation_latency",
            "Wasm contract instantiation latency",
            &[],
            exponential_bucket_latencies(1.0),
        )
    });

    pub static SERVICE_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "wasm_service_instantiation_latency",
            "Wasm service instantiation latency",
            &[],
            exponential_bucket_latencies(1.0),
        )
    });
}

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
    /// Creates a new [`WasmContractModule`] using the WebAssembly module with the provided bytecode.
    pub async fn new(
        contract_bytecode: Bytecode,
        runtime: WasmRuntime,
    ) -> Result<Self, WasmExecutionError> {
        let contract_bytecode = add_metering(contract_bytecode)?;
        match runtime {
            #[cfg(with_wasmer)]
            WasmRuntime::Wasmer => Self::from_wasmer(contract_bytecode).await,
            #[cfg(with_wasmtime)]
            WasmRuntime::Wasmtime => Self::from_wasmtime(contract_bytecode).await,
        }
    }

    /// Creates a new [`WasmContractModule`] using the WebAssembly module in `contract_bytecode_file`.
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
        let _instantiation_latency = metrics::CONTRACT_INSTANTIATION_LATENCY.measure_latency();

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
    /// Creates a new [`WasmServiceModule`] using the WebAssembly module with the provided bytecode.
    pub async fn new(
        service_bytecode: Bytecode,
        runtime: WasmRuntime,
    ) -> Result<Self, WasmExecutionError> {
        match runtime {
            #[cfg(with_wasmer)]
            WasmRuntime::Wasmer => Self::from_wasmer(service_bytecode).await,
            #[cfg(with_wasmtime)]
            WasmRuntime::Wasmtime => Self::from_wasmtime(service_bytecode).await,
        }
    }

    /// Creates a new [`WasmServiceModule`] using the WebAssembly module in `service_bytecode_file`.
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
        let _instantiation_latency = metrics::SERVICE_INSTANTIATION_LATENCY.measure_latency();

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

/// Instrument the [`Bytecode`] to add fuel metering.
pub fn add_metering(bytecode: Bytecode) -> Result<Bytecode, WasmExecutionError> {
    struct WasmtimeRules;

    impl gas_metering::Rules for WasmtimeRules {
        /// Calculates the fuel cost of a WebAssembly [`Operator`].
        ///
        /// The rules try to follow the hardcoded [rules in the Wasmtime runtime
        /// engine](https://docs.rs/wasmtime/5.0.0/wasmtime/struct.Store.html#method.add_fuel).
        fn instruction_cost(
            &self,
            instruction: &parity_wasm::elements::Instruction,
        ) -> Option<u32> {
            use parity_wasm::elements::Instruction::*;

            Some(match instruction {
                Nop | Drop | Block(_) | Loop(_) | Unreachable | Else | End => 0,
                _ => 1,
            })
        }

        fn memory_grow_cost(&self) -> gas_metering::MemoryGrowCost {
            gas_metering::MemoryGrowCost::Free
        }

        fn call_per_local_cost(&self) -> u32 {
            0
        }
    }

    let instrumented_module = gas_metering::inject(
        parity_wasm::deserialize_buffer(&bytecode.bytes)?,
        gas_metering::host_function::Injector::new(
            "linera:app/contract-runtime-api",
            "consume-fuel",
        ),
        &WasmtimeRules,
    )
    .map_err(|_| WasmExecutionError::InstrumentModule)?;

    Ok(Bytecode::new(instrumented_module.into_bytes()?))
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

/// Errors that can occur when executing a user application in a WebAssembly module.
#[derive(Debug, Error)]
pub enum WasmExecutionError {
    #[error("Failed to load contract Wasm module: {_0}")]
    LoadContractModule(#[source] anyhow::Error),
    #[error("Failed to load service Wasm module: {_0}")]
    LoadServiceModule(#[source] anyhow::Error),
    #[error("Failed to instrument Wasm module to add fuel metering")]
    InstrumentModule,
    #[error("Invalid Wasm module: {0}")]
    InvalidBytecode(#[from] wasm_instrument::parity_wasm::SerializationError),
    #[cfg(with_wasmer)]
    #[error("Failed to instantiate Wasm module: {_0}")]
    InstantiateModuleWithWasmer(#[from] Box<::wasmer::InstantiationError>),
    #[cfg(with_wasmtime)]
    #[error("Failed to create and configure Wasmtime runtime: {_0}")]
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
