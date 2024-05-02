// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types and macros useful for writing an application contract.

mod conversions_from_wit;
mod conversions_to_wit;
#[cfg(not(with_testing))]
mod runtime;
#[cfg(with_testing)]
mod test_runtime;
#[doc(hidden)]
pub mod wit;

#[cfg(not(with_testing))]
pub use self::runtime::ContractRuntime;
#[cfg(with_testing)]
pub use self::test_runtime::MockContractRuntime;
#[doc(hidden)]
pub use self::wit::export_contract;
use crate::{log::ContractLogger, util::BlockingWait, State};

/// Inside tests, use the [`MockContractRuntime`] instead of the real [`ContractRuntime`].
#[cfg(with_testing)]
pub type ContractRuntime<Application> = MockContractRuntime<Application>;

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
            fn instantiate(argument: Vec<u8>) {
                use $crate::util::BlockingWait;
                $crate::contract::run_async_entrypoint::<$contract, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let argument = $crate::serde_json::from_slice(&argument)
                            .expect("Failed to deserialize instantiation argument");

                        contract.instantiate(argument).blocking_wait()
                    },
                )
            }

            fn execute_operation(operation: Vec<u8>) -> Vec<u8> {
                use $crate::util::BlockingWait;
                $crate::contract::run_async_entrypoint::<$contract, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let operation: <$contract as $crate::abi::ContractAbi>::Operation =
                            $crate::bcs::from_bytes(&operation)
                                .expect("Failed to deserialize operation");

                        let response = contract.execute_operation(operation).blocking_wait();

                        $crate::bcs::to_bytes(&response)
                            .expect("Failed to serialize contract's `Response`")
                    },
                )
            }

            fn execute_message(message: Vec<u8>) {
                use $crate::util::BlockingWait;
                $crate::contract::run_async_entrypoint::<$contract, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let message: <$contract as $crate::Contract>::Message =
                            $crate::bcs::from_bytes(&message)
                                .expect("Failed to deserialize message");

                        contract.execute_message(message).blocking_wait()
                    },
                )
            }

            fn finalize() {
                use $crate::util::BlockingWait;
                $crate::contract::run_async_entrypoint::<$contract, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| contract.finalize().blocking_wait(),
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
pub fn run_async_entrypoint<Contract, Output, RawOutput>(
    contract: &mut Option<Contract>,
    entrypoint: impl FnOnce(&mut Contract) -> Output + Send,
) -> RawOutput
where
    Contract: crate::Contract,
    Output: Into<RawOutput> + Send + 'static,
{
    ContractLogger::install();

    let contract = contract.get_or_insert_with(|| {
        let runtime = ContractRuntime::new();
        let state = Contract::State::load(runtime.key_value_store()).blocking_wait();
        Contract::new(state, runtime).blocking_wait()
    });

    entrypoint(contract).into()
}
