// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_trait::async_trait;
use counter::CounterAbi;
use linera_sdk::{base::WithContractAbi, Contract, ContractRuntime, SimpleStateStorage};
use thiserror::Error;

use self::state::Counter;

pub struct CounterContract {
    state: Counter,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(CounterContract);

impl WithContractAbi for CounterContract {
    type Abi = CounterAbi;
}

#[async_trait]
impl Contract for CounterContract {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;
    type State = Counter;
    type Message = ();

    async fn new(state: Counter, runtime: ContractRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(CounterContract { state, runtime })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn initialize(&mut self, value: u64) -> Result<(), Self::Error> {
        // Validate that the application parameters were configured correctly.
        let _ = self.runtime.application_parameters();

        self.state.value = value;

        Ok(())
    }

    async fn execute_operation(&mut self, operation: u64) -> Result<(), Self::Error> {
        self.state.value += operation;
        Ok(())
    }

    async fn execute_message(&mut self, _message: ()) -> Result<(), Self::Error> {
        Err(Error::MessagesNotSupported)
    }

    async fn handle_application_call(
        &mut self,
        increment: u64,
    ) -> Result<Self::Response, Self::Error> {
        self.state.value += increment;
        Ok(self.state.value)
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
        test::{mock_application_parameters, test_contract_runtime},
        Contract,
    };
    use webassembly_test::webassembly_test;

    use super::{Counter, CounterContract, Error};

    #[webassembly_test]
    fn operation() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let increment = 42_308_u64;

        let result = counter
            .execute_operation(increment)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert!(result.is_ok());
        assert_eq!(counter.state.value, initial_value + increment);
    }

    #[webassembly_test]
    fn message() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let result = counter
            .execute_message(())
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert_matches!(result, Err(Error::MessagesNotSupported));
        assert_eq!(counter.state.value, initial_value);
    }

    #[webassembly_test]
    fn cross_application_call() {
        let initial_value = 2_845_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let increment = 8_u64;

        let result = counter
            .handle_application_call(increment)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        let expected_value = initial_value + increment;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_value);
        assert_eq!(counter.state.value, expected_value);
    }

    fn create_and_initialize_counter(initial_value: u64) -> CounterContract {
        let counter = Counter::default();
        let mut contract = CounterContract {
            state: counter,
            runtime: test_contract_runtime(),
        };

        mock_application_parameters(&());

        let result = contract
            .initialize(initial_value)
            .now_or_never()
            .expect("Initialization of counter state should not await anything");

        assert!(result.is_ok());
        assert_eq!(contract.state.value, initial_value);

        contract
    }
}
