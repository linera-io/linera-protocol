// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application contract.

mod conversions_from_wit;
mod conversions_to_wit;
pub mod exported_futures;
pub mod system_api;

// Import the system interface.
wit_bindgen_guest_rust::import!("writable_system.wit");

// Export the contract interface.
wit_bindgen_guest_rust::export!(
    export_macro = "export_contract"
    types_path = "contract"
    reexported_crate_path = "wit_bindgen_guest_rust"
    "contract.wit"
);

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
        impl $crate::contract::Contract for $application {
            type Initialize = Initialize;
            type ExecuteOperation = ExecuteOperation;
            type ExecuteEffect = ExecuteEffect;
            type HandleApplicationCall = HandleApplicationCall;
            type HandleSessionCall = HandleSessionCall;
        }

        impl $application {
            /// Calls another `application`.
            ///
            /// The application state is persisted before the call and restored after the call, in
            /// order to allow reentrant calls to use the most up-to-date state.
            pub async fn call_application(
                &mut self,
                authenticated: bool,
                application: $crate::base::ApplicationId,
                argument: &[u8],
                forwarded_sessions: Vec<$crate::base::SessionId>,
            ) -> (Vec<u8>, Vec<$crate::base::SessionId>) {
                use $crate::contract::exported_futures::ContractStateStorage as Storage;

                <Self as $crate::Contract>::Storage::execute_with_released_state(
                    self,
                    move || async move {
                        $crate::contract::system_api::call_application_without_persisting_state(
                            authenticated,
                            application,
                            argument,
                            forwarded_sessions,
                        )
                    },
                )
                .await
            }

            /// Calls a `session` from another application.
            ///
            /// The application state is persisted before the call and restored after the call, in
            /// order to allow reentrant calls to use the most up-to-date state.
            pub async fn call_session(
                &mut self,
                authenticated: bool,
                session: $crate::base::SessionId,
                argument: &[u8],
                forwarded_sessions: Vec<$crate::base::SessionId>,
            ) -> (Vec<u8>, Vec<$crate::base::SessionId>) {
                use $crate::contract::exported_futures::ContractStateStorage as Storage;

                <Self as $crate::Contract>::Storage::execute_with_released_state(
                    self,
                    move || async move {
                        $crate::contract::system_api::call_session_without_persisting_state(
                            authenticated,
                            session,
                            argument,
                            forwarded_sessions,
                        )
                    },
                )
                .await
            }
        }

        $crate::instance_exported_future! {
            contract::Initialize<$application>(
                context: $crate::contract::OperationContext,
                argument: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::ExecuteOperation<$application>(
                context: $crate::contract::OperationContext,
                operation: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::ExecuteEffect<$application>(
                context: $crate::contract::EffectContext,
                effect: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::HandleApplicationCall<$application>(
                context: $crate::contract::CalleeContext,
                argument: Vec<u8>,
                forwarded_sessions: Vec<$crate::contract::SessionId>,
            ) -> PollCallApplication
        }

        $crate::instance_exported_future! {
            contract::HandleSessionCall<$application>(
                context: $crate::contract::CalleeContext,
                session: $crate::contract::Session,
                argument: Vec<u8>,
                forwarded_sessions: Vec<$crate::contract::SessionId>,
            ) -> PollCallSession
        }

        /// Stub of a `main` entrypoint so that the binary doesn't fail to compile on targets other
        /// than WebAssembly.
        #[cfg(not(target_arch = "wasm32"))]
        fn main() {}
    };
}
