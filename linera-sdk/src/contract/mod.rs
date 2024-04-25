// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application contract.

mod conversions_from_wit;
mod conversions_to_wit;
mod runtime;
#[doc(hidden)]
pub mod wit;

pub use self::runtime::ContractRuntime;
#[doc(hidden)]
pub use self::wit::export_contract;
use crate::{log::ContractLogger, util::BlockingWait, State};

/// Declares an implementation of the [`Contract`][`crate::Contract`] trait, exporting it from the
/// Wasm module.
///
/// Generates the necessary boilerplate for implementing the contract WIT interface, exporting the
/// necessary resource types and functions so that the host can call the application contract.
#[macro_export]
macro_rules! contract {
    ($contract:ident) => {
        #[doc(hidden)]
        static mut CONTRACT: Option<$contract> = None;

        /// Export the contract interface.
        $crate::export_contract!($contract with_types_in $crate::contract::wit);

        /// Mark the contract type to be exported.
        impl $crate::contract::wit::exports::linera::app::contract_entrypoints::Guest
            for $contract
        {
            fn instantiate(argument: Vec<u8>) -> Result<(), String> {
                use $crate::util::BlockingWait;
                $crate::contract::run_async_entrypoint::<$contract, _, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let argument = serde_json::from_slice(&argument)?;

                        contract.instantiate(argument).blocking_wait()
                    },
                )
            }

            fn execute_operation(operation: Vec<u8>) -> Result<Vec<u8>, String> {
                use $crate::util::BlockingWait;
                $crate::contract::run_async_entrypoint::<$contract, _, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let operation: <$contract as $crate::abi::ContractAbi>::Operation =
                            bcs::from_bytes(&operation)?;

                        contract
                            .execute_operation(operation)
                            .blocking_wait()
                            .map(|response| {
                                bcs::to_bytes(&response)
                                    .expect("Failed to serialize contract's `Response`")
                            })
                    },
                )
            }

            fn execute_message(message: Vec<u8>) -> Result<(), String> {
                use $crate::util::BlockingWait;
                $crate::contract::run_async_entrypoint::<$contract, _, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let message: <$contract as $crate::Contract>::Message =
                            bcs::from_bytes(&message)?;

                        contract.execute_message(message).blocking_wait()
                    },
                )
            }

            fn finalize() -> Result<(), String> {
                use $crate::util::BlockingWait;
                $crate::contract::run_async_entrypoint::<$contract, _, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| contract.finalize().blocking_wait(),
                )?;

                unsafe { CONTRACT.take(); }
                Ok(())
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
pub fn run_async_entrypoint<Contract, Output, Error, RawOutput>(
    contract: &mut Option<Contract>,
    entrypoint: impl FnOnce(&mut Contract) -> Result<Output, Error> + Send,
) -> Result<RawOutput, String>
where
    Contract: crate::Contract,
    Output: Into<RawOutput> + Send + 'static,
    Error: ToString + 'static,
{
    ContractLogger::install();

    let contract = contract.get_or_insert_with(|| {
        let state = Contract::State::load().blocking_wait();
        Contract::new(state, ContractRuntime::new())
            .blocking_wait()
            .expect("Failed to create application contract handler instance")
    });

    entrypoint(contract)
        .map_err(|error| error.to_string())
        .map(Output::into)
}
