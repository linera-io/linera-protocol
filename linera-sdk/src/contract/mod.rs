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
use crate::log::ContractLogger;

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
                $crate::contract::run_entrypoint::<$contract, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let argument = $crate::serde_json::from_slice(&argument)
                            .unwrap_or_else(|_| panic!("Failed to deserialize instantiation argument {argument:?}"));

                        contract.instantiate(argument)
                    },
                )
            }

            fn execute_operation(operation: Vec<u8>) -> Vec<u8> {
                $crate::contract::run_entrypoint::<$contract, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let operation = <$contract as $crate::abi::ContractAbi>::deserialize_operation(operation)
                            .expect("Failed to deserialize `Operation` in execute_operation");

                        let response = contract.execute_operation(operation);

                        <$contract as $crate::abi::ContractAbi>::serialize_response(response)
                            .expect("Failed to serialize `Response` in execute_operation")
                    },
                )
            }

            fn execute_message(message: Vec<u8>) {
                $crate::contract::run_entrypoint::<$contract, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let message: <$contract as $crate::Contract>::Message =
                            $crate::bcs::from_bytes(&message)
                                .expect("Failed to deserialize message");

                        contract.execute_message(message)
                    },
                )
            }

            fn process_streams(updates: Vec<
                $crate::contract::wit::exports::linera::app::contract_entrypoints::StreamUpdate,
            >) {
                $crate::contract::run_entrypoint::<$contract, _, _>(
                    unsafe { &mut CONTRACT },
                    move |contract| {
                        let updates = updates.into_iter().map(Into::into).collect();
                        contract.process_streams(updates)
                    },
                )
            }

            fn finalize() {
                let Some(contract) = (unsafe { CONTRACT.take() }) else {
                    $crate::ContractLogger::install();
                    panic!("Calling `store` on a `Contract` instance that wasn't loaded");
                };

                contract.store();
            }
        }

        /// Stub of a `main` entrypoint so that the binary doesn't fail to compile on targets other
        /// than WebAssembly.
        #[cfg(not(target_arch = "wasm32"))]
        fn main() {}
    };
}

/// Runs an entrypoint, ensuring the contract is loaded first.
pub fn run_entrypoint<Contract, Output, RawOutput>(
    contract: &mut Option<Contract>,
    entrypoint: impl FnOnce(&mut Contract) -> Output + Send,
) -> RawOutput
where
    Contract: crate::Contract,
    Output: Into<RawOutput> + Send + 'static,
{
    ContractLogger::install();

    let contract = contract.get_or_insert_with(|| Contract::load(ContractRuntime::new()));

    entrypoint(contract).into()
}
