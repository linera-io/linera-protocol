// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application service.

mod conversions_from_wit;
mod conversions_to_wit;
#[cfg(not(with_testing))]
mod runtime;
#[cfg(with_testing)]
mod test_runtime;
#[doc(hidden)]
pub mod wit;

use std::future::Future;

#[cfg(not(with_testing))]
pub use self::runtime::ServiceRuntime;
#[cfg(with_testing)]
pub use self::test_runtime::MockServiceRuntime;
#[doc(hidden)]
pub use self::wit::export_service;
use crate::{util::BlockingWait, ServiceLogger};

/// Inside tests, use the [`MockServiceRuntime`] instead of the real [`ServiceRuntime`].
#[cfg(with_testing)]
pub type ServiceRuntime<Application> = MockServiceRuntime<Application>;

/// Declares an implementation of the [`Service`][`crate::Service`] trait, exporting it from the
/// Wasm module.
///
/// Generates the necessary boilerplate for implementing the service WIT interface, exporting the
/// necessary resource types and functions so that the host can call the application service.
#[macro_export]
macro_rules! service {
    ($service:ident) => {
        /// Export the service interface.
        $crate::export_service!($service with_types_in $crate::service::wit);

        /// Mark the service type to be exported.
        impl $crate::service::wit::exports::linera::app::service_entrypoints::Guest for $service {
            fn handle_query(argument: Vec<u8>) -> Vec<u8> {
                let request = $crate::serde_json::from_slice(&argument)
                    .expect("Query is invalid and could not be deserialized");
                let response = $crate::service::run_async_entrypoint(move |runtime| async move {
                    let service = <$service as $crate::Service>::new(runtime).await;
                    service.handle_query(request).await
                });
                $crate::serde_json::to_vec(&response)
                    .expect("Failed to deserialize query response")
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
pub fn run_async_entrypoint<Service, Entrypoint, Output>(
    entrypoint: impl FnOnce(ServiceRuntime<Service>) -> Entrypoint,
) -> Output
where
    Service: crate::Service,
    Entrypoint: Future<Output = Output>,
{
    ServiceLogger::install();

    entrypoint(ServiceRuntime::new()).blocking_wait()
}
