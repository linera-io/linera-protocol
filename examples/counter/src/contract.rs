// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use counter::CounterAbi;
use linera_sdk::{base::WithContractAbi, Contract, ContractRuntime, StoreOnDrop};
use thiserror::Error;

use self::state::Counter;

pub struct CounterContract {
    state: StoreOnDrop<Counter>,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(CounterContract);

impl WithContractAbi for CounterContract {
    type Abi = CounterAbi;
}

impl Contract for CounterContract {
    type Error = Error;
    type State = Counter;
    type Message = ();
    type InstantiationArgument = u64;
    type Parameters = ();

    async fn new(state: Counter, runtime: ContractRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(CounterContract {
            state: StoreOnDrop(state),
            runtime,
        })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn instantiate(&mut self, value: u64) -> Result<(), Self::Error> {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();

        self.state.value.set(value);

        Ok(())
    }

    async fn execute_operation(&mut self, operation: u64) -> Result<u64, Self::Error> {
        let new_value = self.state.value.get() + operation;
        self.state.value.set(new_value);
        Ok(new_value)
    }

    async fn execute_message(&mut self, _message: ()) -> Result<(), Self::Error> {
        Err(Error::MessagesNotSupported)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Counter application doesn't support any cross-chain messages.
    #[error("Counter application doesn't support any cross-chain messages")]
    MessagesNotSupported,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::FutureExt;
    use linera_sdk::{
        test::{mock_application_parameters, mock_key_value_store, test_contract_runtime},
        util::BlockingWait,
        views::{View, ViewStorageContext},
        Contract,
    };
    use webassembly_test::webassembly_test;

    use super::{Counter, CounterContract, Error};

    #[webassembly_test]
    fn operation() {
        let initial_value = 72_u64;
        let mut counter = create_and_instantiate_counter(initial_value);

        let increment = 42_308_u64;

        let result = counter
            .execute_operation(increment)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert!(result.is_ok());
        assert_eq!(*counter.state.value.get(), initial_value + increment);
    }

    #[webassembly_test]
    fn message() {
        let initial_value = 72_u64;
        let mut counter = create_and_instantiate_counter(initial_value);

        let result = counter
            .execute_message(())
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert_matches!(result, Err(Error::MessagesNotSupported));
        assert_eq!(*counter.state.value.get(), initial_value);
    }

    #[webassembly_test]
    fn cross_application_call() {
        let initial_value = 2_845_u64;
        let mut counter = create_and_instantiate_counter(initial_value);

        let increment = 8_u64;

        let result = counter
            .execute_operation(increment)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        let expected_value = initial_value + increment;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_value);
        assert_eq!(*counter.state.value.get(), expected_value);
    }

    fn create_and_instantiate_counter(initial_value: u64) -> CounterContract {
        mock_key_value_store();
        mock_application_parameters(&());

        let mut contract = CounterContract {
            state: Counter::load(ViewStorageContext::default())
                .blocking_wait()
                .expect("Failed to read from mock key value store")
                .into(),
            runtime: test_contract_runtime(),
        };

        let result = contract
            .instantiate(initial_value)
            .now_or_never()
            .expect("Initialization of counter state should not await anything");

        assert!(result.is_ok());
        assert_eq!(*contract.state.value.get(), initial_value);

        contract
    }
}
