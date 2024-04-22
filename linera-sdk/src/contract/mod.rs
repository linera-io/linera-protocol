// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application contract.

mod conversions_from_wit;
mod conversions_to_wit;
mod runtime;
mod storage;
pub(crate) mod wit;

pub use self::{runtime::ContractRuntime, storage::ContractStateStorage};
use crate::{log::ContractLogger, util::BlockingWait, Contract};

/// Declares an implementation of the [`Contract`][`crate::Contract`] trait, exporting it from the
/// Wasm module.
///
/// Generates the necessary boilerplate for implementing the contract WIT interface, exporting the
/// necessary resource types and functions so that the host can call the contract application.
#[macro_export]
macro_rules! contract {
    ($application:ty) => {
        #[doc(hidden)]
        static mut APPLICATION: Option<$application> = None;

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_instantiate(argument: Vec<u8>) -> Result<(), String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
                    let argument = serde_json::from_slice(&argument)?;

                    application.initialize(argument).blocking_wait()
                },
            )
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_execute_operation(operation: Vec<u8>) -> Result<Vec<u8>, String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
                    let operation: <$application as $crate::abi::ContractAbi>::Operation =
                        bcs::from_bytes(&operation)?;

                    application
                        .execute_operation(operation)
                        .blocking_wait()
                        .map(|response| {
                            bcs::to_bytes(&response)
                                .expect("Failed to serialize contract's `Response`")
                        })
                },
            )
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_execute_message(message: Vec<u8>) -> Result<(), String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
                    let message: <$application as $crate::Contract>::Message =
                        bcs::from_bytes(&message)?;

                    application.execute_message(message).blocking_wait()
                },
            )
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_finalize() -> Result<(), String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| application.finalize().blocking_wait(),
            )
        }

        /// Stub of a `main` entrypoint so that the binary doesn't fail to compile on targets other
        /// than WebAssembly.
        #[cfg(not(target_arch = "wasm32"))]
        fn main() {}

        #[doc(hidden)]
        #[no_mangle]
        fn __service_handle_query(argument: Vec<u8>) -> Result<Vec<u8>, String> {
            unreachable!("Service entrypoint should not be called in contract");
        }
    };
}

/// Runs an asynchronous entrypoint in a blocking manner, by repeatedly polling the entrypoint
/// future.
pub fn run_async_entrypoint<Application, Output, Error, RawOutput>(
    application: &mut Option<Application>,
    entrypoint: impl FnOnce(&mut Application) -> Result<Output, Error> + Send,
) -> Result<RawOutput, String>
where
    Application: Contract,
    Output: Into<RawOutput> + Send + 'static,
    Error: ToString + 'static,
{
    ContractLogger::install();

    let application = application.get_or_insert_with(|| {
        let state = Application::Storage::load().blocking_wait();
        Application::new(state, ContractRuntime::new())
            .blocking_wait()
            .expect("Failed to create application contract hnadler instance")
    });

    let output = entrypoint(application).map_err(|error| error.to_string())?;

    Application::Storage::store(application.state_mut()).blocking_wait();

    Ok(output.into())
}

// Import entrypoint proxy functions that applications implement with the `contract!` macro.
extern "Rust" {
    fn __contract_instantiate(argument: Vec<u8>) -> Result<(), String>;
    fn __contract_execute_operation(argument: Vec<u8>) -> Result<Vec<u8>, String>;
    fn __contract_execute_message(message: Vec<u8>) -> Result<(), String>;
    fn __contract_finalize() -> Result<(), String>;
}
