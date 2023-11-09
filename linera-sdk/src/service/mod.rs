// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application service.

mod conversions_from_wit;
mod conversions_to_wit;
pub mod exported_futures;
pub mod system_api;
pub mod wit_types;

use crate::{util::BlockingWait, ServiceLogger};
use std::future::Future;

// Import the system interface.
wit_bindgen_guest_rust::import!("service_system_api.wit");

/// Declares an implementation of the [`Service`][`crate::Service`] trait, exporting it from the
/// Wasm module.
///
/// Generates the necessary boilerplate for implementing the service WIT interface, exporting the
/// necessary resource types and functions so that the host can call the service application.
#[macro_export]
macro_rules! service {
    ($application:ty) => {
        // Export the service interface.
        $crate::export_service!($application);

        /// Marks the service type to be exported.
        impl $crate::service::wit_types::Service for $application {
            fn handle_query(
                context: $crate::service::wit_types::QueryContext,
                argument: Vec<u8>,
            ) -> Result<Vec<u8>, String> {
                $crate::service::run_async_entrypoint(
                            <
                                <$application as $crate::Service>::Storage
                                    as $crate::service::exported_futures::ServiceStateStorage
                            >::handle_query(context, argument),
                        )
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
pub fn run_async_entrypoint<Entrypoint, Output, Error, RawOutput>(
    entrypoint: Entrypoint,
) -> Result<RawOutput, String>
where
    Entrypoint: Future<Output = Result<Output, Error>> + Send,
    Output: Into<RawOutput> + Send + 'static,
    Error: ToString + 'static,
{
    ServiceLogger::install();

    entrypoint
        .blocking_wait()
        .map(|output| output.into())
        .map_err(|error| error.to_string())
}
