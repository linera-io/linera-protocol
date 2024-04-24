// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application service.

mod conversions_from_wit;
mod conversions_to_wit;
mod runtime;
mod storage;
pub(crate) mod wit;

use std::future::Future;

pub use self::{runtime::ServiceRuntime, storage::ServiceStateStorage};
use crate::{util::BlockingWait, ServiceLogger};

/// Declares an implementation of the [`Service`][`crate::Service`] trait, exporting it from the
/// Wasm module.
///
/// Generates the necessary boilerplate for implementing the service WIT interface, exporting the
/// necessary resource types and functions so that the host can call the service application.
#[macro_export]
macro_rules! service {
    ($application:ty) => {
        #[doc(hidden)]
        #[no_mangle]
        fn __service_handle_query(argument: Vec<u8>) -> Result<Vec<u8>, String> {
            let request = serde_json::from_slice(&argument).map_err(|error| error.to_string())?;
            let response = $crate::service::run_async_entrypoint(move |runtime| async move {
                let state =
                    <<$application as $crate::Service>::State as $crate::State>::load().await;
                let application = <$application as $crate::Service>::new(state, runtime)
                    .await
                    .map_err(|error| error.to_string())?;
                application
                    .handle_query(request)
                    .await
                    .map_err(|error| error.to_string())
            })?;
            serde_json::to_vec(&response).map_err(|error| error.to_string())
        }

        /// Stub of a `main` entrypoint so that the binary doesn't fail to compile on targets other
        /// than WebAssembly.
        #[cfg(not(target_arch = "wasm32"))]
        fn main() {}

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_instantiate(_: Vec<u8>) -> Result<(), String> {
            unreachable!("Contract entrypoint should not be called in service");
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_execute_operation(_: Vec<u8>) -> Result<Vec<u8>, String> {
            unreachable!("Contract entrypoint should not be called in service");
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_execute_message(message: Vec<u8>) -> Result<(), String> {
            unreachable!("Contract entrypoint should not be called in service");
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_finalize() -> Result<(), String> {
            unreachable!("Contract entrypoint should not be called in service");
        }
    };
}

/// Runs an asynchronous entrypoint in a blocking manner, by repeatedly polling the entrypoint
/// future.
pub fn run_async_entrypoint<Service, Entrypoint, Output, Error>(
    entrypoint: impl FnOnce(ServiceRuntime<Service>) -> Entrypoint,
) -> Result<Output, String>
where
    Service: crate::Service,
    Entrypoint: Future<Output = Result<Output, Error>>,
    Error: ToString + 'static,
{
    ServiceLogger::install();

    entrypoint(ServiceRuntime::new())
        .blocking_wait()
        .map_err(|error| error.to_string())
}

// Import entrypoint proxy functions that applications implement with the `service!` macro.
extern "Rust" {
    fn __service_handle_query(argument: Vec<u8>) -> Result<Vec<u8>, String>;
}
