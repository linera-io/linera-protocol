// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Counter;
use async_trait::async_trait;
use linera_sdk::{
    base::WithContractAbi, ApplicationCallOutcome, Contract, ContractRuntime, ExecutionOutcome,
    SimpleStateStorage,
};
use thiserror::Error;

pub struct CounterContract {
    state: Counter,
}

linera_sdk::contract!(CounterContract);

impl WithContractAbi for CounterContract {
    type Abi = counter::CounterAbi;
}

#[async_trait]
impl Contract for CounterContract {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;
    type State = Counter;

    async fn new(state: Counter, _runtime: ContractRuntime) -> Result<Self, Self::Error> {
        Ok(CounterContract { state })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn initialize(
        &mut self,
        value: u64,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());

        self.state.value = value;

        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        operation: u64,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        self.state.value += operation;
        Ok(ExecutionOutcome::default())
    }

    async fn execute_message(
        &mut self,
        _message: (),
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        Err(Error::MessagesNotSupported)
    }

    async fn handle_application_call(
        &mut self,
        increment: u64,
    ) -> Result<ApplicationCallOutcome<Self::Message, Self::Response>, Self::Error> {
        self.state.value += increment;
        Ok(ApplicationCallOutcome {
            value: self.state.value,
            ..ApplicationCallOutcome::default()
        })
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
    use super::{Counter, CounterContract, Error};
    use assert_matches::assert_matches;
    use futures::FutureExt;
    use linera_sdk::{
        test::mock_application_parameters, ApplicationCallOutcome, Contract, ExecutionOutcome,
    };
    use webassembly_test::webassembly_test;

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
        assert_eq!(result.unwrap(), ExecutionOutcome::default());
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
        let expected_outcome = ApplicationCallOutcome {
            value: expected_value,
            execution_outcome: ExecutionOutcome::default(),
        };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_outcome);
        assert_eq!(counter.state.value, expected_value);
    }

    fn create_and_initialize_counter(initial_value: u64) -> CounterContract {
        let counter = Counter::default();
        let mut contract = CounterContract { state: counter };

        mock_application_parameters(&());

        let result = contract
            .initialize(initial_value)
            .now_or_never()
            .expect("Initialization of counter state should not await anything");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionOutcome::default());
        assert_eq!(contract.state.value, initial_value);

        contract
    }
}
