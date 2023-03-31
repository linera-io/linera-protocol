// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application service.

mod conversions_from_wit;
mod conversions_to_wit;
pub mod exported_futures;
pub mod system_api;

// Import the system interface.
wit_bindgen_guest_rust::import!("queryable_system.wit");

// Export the service interface.
wit_bindgen_guest_rust::export!(
    export_macro = "export_service"
    types_path = "service"
    reexported_crate_path = "wit_bindgen_guest_rust"
    "service.wit"
);

/// Declares an implementation of the [`Service`][`crate::Service`] trait, exporting it from the
/// WASM module.
///
/// Generates the necessary boilerplate for implementing the service WIT interface, exporting the
/// necessary resource types and functions so that the host can call the service application.
#[macro_export]
macro_rules! service {
    ($application:ty) => {
        // Export the service interface.
        $crate::export_service!($application);

        /// Mark the service type to be exported.
        impl $crate::service::Service for $application {
            type QueryApplication = QueryApplication;
        }

        $crate::instance_exported_future! {
            service::QueryApplication<$application>(
                context: $crate::service::QueryContext,
                argument: Vec<u8>,
            ) -> PollQuery
        }

        /// Stub of a `main` entrypoint so that the binary doesn't fail to compile on targets other
        /// than WebAssembly.
        #[cfg(not(target_arch = "wasm32"))]
        fn main() {}
    };
}
