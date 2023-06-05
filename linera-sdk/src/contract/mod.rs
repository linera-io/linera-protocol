// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application contract.

mod conversions_from_wit;
mod conversions_to_wit;
pub mod exported_futures;
pub mod system_api;
pub mod wit_types;

// Import the system interface.
wit_bindgen_guest_rust::import!("contract_system_api.wit");

/// Declares an implementation of the [`Contract`][`crate::Contract`] trait, exporting it from the
/// WASM module.
///
/// Generates the necessary boilerplate for implementing the contract WIT interface, exporting the
/// necessary resource types and functions so that the host can call the contract application.
#[macro_export]
macro_rules! contract {
    ($application:ty) => {
        // Export the contract interface.
        $crate::export_contract!($application);

        /// Mark the contract type to be exported.
        impl $crate::contract::wit_types::Contract for $application {
            type Initialize = Initialize;
            type ExecuteOperation = ExecuteOperation;
            type ExecuteMessage = ExecuteMessage;
            type HandleApplicationCall = HandleApplicationCall;
            type HandleSessionCall = HandleSessionCall;
        }

        $crate::instance_exported_future! {
            contract::Initialize<$application>(
                context: $crate::contract::wit_types::OperationContext,
                argument: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::ExecuteOperation<$application>(
                context: $crate::contract::wit_types::OperationContext,
                operation: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::ExecuteMessage<$application>(
                context: $crate::contract::wit_types::MessageContext,
                message: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::HandleApplicationCall<$application>(
                context: $crate::contract::wit_types::CalleeContext,
                argument: Vec<u8>,
                forwarded_sessions: Vec<$crate::contract::wit_types::SessionId>,
            ) -> PollCallApplication
        }

        $crate::instance_exported_future! {
            contract::HandleSessionCall<$application>(
                context: $crate::contract::wit_types::CalleeContext,
                session: Vec<u8>,
                argument: Vec<u8>,
                forwarded_sessions: Vec<$crate::contract::wit_types::SessionId>,
            ) -> PollCallSession
        }

        /// Stub of a `main` entrypoint so that the binary doesn't fail to compile on targets other
        /// than WebAssembly.
        #[cfg(not(target_arch = "wasm32"))]
        fn main() {}
    };
}
