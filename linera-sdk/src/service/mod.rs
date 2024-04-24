// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application service.

mod conversions_from_wit;
mod conversions_to_wit;
mod runtime;
#[doc(hidden)]
pub mod wit;

use std::future::Future;

pub use self::runtime::ServiceRuntime;
#[doc(hidden)]
pub use self::wit::export_service;
use crate::{util::BlockingWait, ServiceLogger};

/// Declares an implementation of the [`Service`][`crate::Service`] trait, exporting it from the
/// Wasm module.
///
/// Generates the necessary boilerplate for implementing the service WIT interface, exporting the
/// necessary resource types and functions so that the host can call the service application.
#[macro_export]
macro_rules! service {
    ($application:ident) => {
        /// Export the service interface.
        $crate::export_service!($application with_types_in $crate::service::wit);

        /// Mark the service type to be exported.
        impl $crate::service::wit::exports::linera::app::service_entrypoints::Guest for $application {
            fn handle_query(argument: Vec<u8>) -> Result<Vec<u8>, String> {
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
        }

        /// Stub of a `main` entrypoint so that the binary doesn't fail to compile on targets other
        /// than WebAssembly.
        #[cfg(not(target_arch = "wasm32"))]
        fn main() {}
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
