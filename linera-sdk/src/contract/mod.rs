// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application contract.

mod conversions_from_wit;
mod conversions_to_wit;
mod runtime;
mod storage;
pub mod system_api;
mod wit_system_api;
pub mod wit_types;

pub use self::{runtime::ContractRuntime, storage::ContractStateStorage};
use crate::{
    log::ContractLogger, util::BlockingWait, ApplicationCallOutcome, Contract, ExecutionOutcome,
    SessionCallOutcome, SessionId,
};

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
        fn __contract_initialize(
            argument: Vec<u8>,
        ) -> Result<$crate::ExecutionOutcome<Vec<u8>>, String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
                    let argument = serde_json::from_slice(&argument)?;

                    application
                        .initialize(&mut $crate::ContractRuntime::default(), argument)
                        .blocking_wait()
                        .map(|outcome| outcome.into_raw())
                },
            )
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_execute_operation(
            operation: Vec<u8>,
        ) -> Result<$crate::ExecutionOutcome<Vec<u8>>, String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
                    let operation: <$application as $crate::abi::ContractAbi>::Operation =
                        bcs::from_bytes(&operation)?;

                    application
                        .execute_operation(&mut $crate::ContractRuntime::default(), operation)
                        .blocking_wait()
                        .map(|outcome| outcome.into_raw())
                },
            )
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_execute_message(
            message: Vec<u8>,
        ) -> Result<$crate::ExecutionOutcome<Vec<u8>>, String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
                    let message: <$application as $crate::abi::ContractAbi>::Message =
                        bcs::from_bytes(&message)?;

                    application
                        .execute_message(&mut $crate::ContractRuntime::default(), message)
                        .blocking_wait()
                        .map(|outcome| outcome.into_raw())
                },
            )
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_handle_application_call(
            argument: Vec<u8>,
            forwarded_sessions: Vec<$crate::SessionId>,
        ) -> Result<$crate::ApplicationCallOutcome<Vec<u8>, Vec<u8>>, String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
                    let argument: <$application as $crate::abi::ContractAbi>::ApplicationCall =
                        bcs::from_bytes(&argument)?;
                    let forwarded_sessions = forwarded_sessions
                        .into_iter()
                        .map(SessionId::from)
                        .collect();

                    application
                        .handle_application_call(
                            &mut $crate::ContractRuntime::default(),
                            argument,
                            forwarded_sessions,
                        )
                        .blocking_wait()
                        .map(|outcome| outcome.into_raw())
                },
            )
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_handle_session_call(
            session_state: Vec<u8>,
            argument: Vec<u8>,
            forwarded_sessions: Vec<$crate::SessionId>,
        ) -> Result<$crate::SessionCallOutcome<Vec<u8>, Vec<u8>, Vec<u8>>, String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
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
                            &mut $crate::ContractRuntime::default(),
                            session_state,
                            argument,
                            forwarded_sessions,
                        )
                        .blocking_wait()
                        .map(|outcome| outcome.into_raw())
                },
            )
        }

        #[doc(hidden)]
        #[no_mangle]
        fn __contract_finalize() -> Result<$crate::ExecutionOutcome<Vec<u8>>, String> {
            use $crate::util::BlockingWait;
            $crate::contract::run_async_entrypoint::<$application, _, _, _>(
                unsafe { &mut APPLICATION },
                move |application| {
                    application
                        .finalize(&mut $crate::ContractRuntime::default())
                        .blocking_wait()
                        .map(|outcome| outcome.into_raw())
                },
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

    let application =
        application.get_or_insert_with(|| Application::Storage::load().blocking_wait());

    let output = entrypoint(application).map_err(|error| error.to_string())?;

    Application::Storage::store(application).blocking_wait();

    Ok(output.into())
}

// Import entrypoint proxy functions that applications implement with the `contract!` macro.
extern "Rust" {
    fn __contract_initialize(argument: Vec<u8>) -> Result<ExecutionOutcome<Vec<u8>>, String>;

    fn __contract_execute_operation(argument: Vec<u8>)
        -> Result<ExecutionOutcome<Vec<u8>>, String>;

    fn __contract_execute_message(message: Vec<u8>) -> Result<ExecutionOutcome<Vec<u8>>, String>;

    fn __contract_handle_application_call(
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallOutcome<Vec<u8>, Vec<u8>>, String>;

    fn __contract_handle_session_call(
        session_state: Vec<u8>,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Vec<u8>, Vec<u8>, Vec<u8>>, String>;

    fn __contract_finalize() -> Result<ExecutionOutcome<Vec<u8>>, String>;
}
