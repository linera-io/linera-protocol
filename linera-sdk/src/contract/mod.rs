// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application contract.

mod conversions_from_wit;
mod conversions_to_wit;
pub mod exported_futures;
pub mod system_api;
pub mod wit_types;

use super::log::ContractLogger;
use crate::{contract::exported_futures::ContractStateStorage, util::BlockingWait, Contract};
use std::future::Future;

// Import the system interface.
wit_bindgen_guest_rust::import!("contract_system_api.wit");

/// Declares an implementation of the [`Contract`][`crate::Contract`] trait, exporting it from the
/// Wasm module.
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
            fn initialize(
                context: $crate::contract::wit_types::OperationContext,
                argument: Vec<u8>,
            ) -> Result<$crate::contract::wit_types::ExecutionResult, String> {
                $crate::contract::run_async_entrypoint::<$application, _, _, _, _>(
                    move |mut application| async move {
                        let argument = serde_json::from_slice(&argument)?;

                        application
                            .initialize(&context.into(), argument)
                            .await
                            .map(|result| (application, result))
                    },
                )
            }

            fn execute_operation(
                context: $crate::contract::wit_types::OperationContext,
                operation: Vec<u8>,
            ) -> Result<$crate::contract::wit_types::ExecutionResult, String> {
                $crate::contract::run_async_entrypoint::<$application, _, _, _, _>(
                    move |mut application| async move {
                        let operation: <$application as $crate::abi::ContractAbi>::Operation =
                            bcs::from_bytes(&operation)?;

                        application
                            .execute_operation(&context.into(), operation)
                            .await
                            .map(|result| (application, result))
                    },
                )
            }

            fn execute_message(
                context: $crate::contract::wit_types::MessageContext,
                message: Vec<u8>,
            ) -> Result<$crate::contract::wit_types::ExecutionResult, String> {
                $crate::contract::run_async_entrypoint::<$application, _, _, _, _>(
                    move |mut application| async move {
                        let message: <$application as $crate::abi::ContractAbi>::Message =
                            bcs::from_bytes(&message)?;

                        application
                            .execute_message(&context.into(), message)
                            .await
                            .map(|result| (application, result))
                    },
                )
            }

            fn handle_application_call(
                context: $crate::contract::wit_types::CalleeContext,
                argument: Vec<u8>,
                forwarded_sessions: Vec<$crate::contract::wit_types::SessionId>,
            ) -> Result<$crate::contract::wit_types::ApplicationCallResult, String> {
                $crate::contract::run_async_entrypoint::<$application, _, _, _, _>(
                    move |mut application| async move {
                        let argument: <$application as $crate::abi::ContractAbi>::ApplicationCall =
                            bcs::from_bytes(&argument)?;
                        let forwarded_sessions = forwarded_sessions
                            .into_iter()
                            .map(SessionId::from)
                            .collect();

                        application
                            .handle_application_call(&context.into(), argument, forwarded_sessions)
                            .await
                            .map(|result| (application, result))
                    },
                )
            }

            fn handle_session_call(
                context: $crate::contract::wit_types::CalleeContext,
                session_state: Vec<u8>,
                argument: Vec<u8>,
                forwarded_sessions: Vec<$crate::contract::wit_types::SessionId>,
            ) -> Result<$crate::contract::wit_types::SessionCallResult, String> {
                $crate::contract::run_async_entrypoint::<$application, _, _, _, _>(
                    move |mut application| async move {
                        let session_state: <$application as $crate::abi::ContractAbi>::SessionState =
                            bcs::from_bytes(&session_state)?;
                        let argument: <$application as $crate::abi::ContractAbi>::SessionCall =
                            bcs::from_bytes(&argument)?;
                        let forwarded_sessions = forwarded_sessions
                            .into_iter()
                            .map(SessionId::from)
                            .collect();

                        application
                            .handle_session_call(
                                &context.into(),
                                session_state,
                                argument,
                                forwarded_sessions,
                            )
                            .await
                            .map(|result| (application, result))
                    },
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
pub fn run_async_entrypoint<Application, Entrypoint, Output, Error, RawOutput>(
    entrypoint: impl FnOnce(Application) -> Entrypoint + Send,
) -> Result<RawOutput, String>
where
    Application: Contract,
    Entrypoint: Future<Output = Result<(Application, Output), Error>> + Send,
    Output: Into<RawOutput> + Send + 'static,
    Error: ToString + 'static,
{
    ContractLogger::install();

    <Application as Contract>::Storage::execute_with_state(entrypoint)
        .blocking_wait()
        .map(|output| output.into())
        .map_err(|error| error.to_string())
}
