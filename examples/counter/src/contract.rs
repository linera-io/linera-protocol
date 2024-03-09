// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Counter;
use async_trait::async_trait;
use linera_sdk::{
    base::{SessionId, WithContractAbi},
    ApplicationCallOutcome, Contract, ContractRuntime, ExecutionOutcome, SimpleStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(Counter);

impl WithContractAbi for Counter {
    type Abi = counter::CounterAbi;
}

#[async_trait]
impl Contract for Counter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn initialize(
        &mut self,
        _runtime: &mut ContractRuntime,
        value: u64,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());

        self.value = value;

        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        _runtime: &mut ContractRuntime,
        operation: u64,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        self.value += operation;
        Ok(ExecutionOutcome::default())
    }

    async fn execute_message(
        &mut self,
        _runtime: &mut ContractRuntime,
        _message: (),
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        Err(Error::MessagesNotSupported)
    }

    async fn handle_application_call(
        &mut self,
        _runtime: &mut ContractRuntime,
        increment: u64,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallOutcome<Self::Message, Self::Response>, Self::Error> {
        self.value += increment;
        Ok(ApplicationCallOutcome {
            value: self.value,
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
    use super::{Counter, Error};
    use assert_matches::assert_matches;
    use futures::FutureExt;
    use linera_sdk::{
        test::mock_application_parameters, ApplicationCallOutcome, Contract, ContractRuntime,
        ExecutionOutcome,
    };
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn operation() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let increment = 42_308_u64;

        let result = counter
            .execute_operation(&mut ContractRuntime::default(), increment)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionOutcome::default());
        assert_eq!(counter.value, initial_value + increment);
    }

    #[webassembly_test]
    fn message() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let result = counter
            .execute_message(&mut ContractRuntime::default(), ())
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert_matches!(result, Err(Error::MessagesNotSupported));
        assert_eq!(counter.value, initial_value);
    }

    #[webassembly_test]
    fn cross_application_call() {
        let initial_value = 2_845_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let increment = 8_u64;

        let result = counter
            .handle_application_call(&mut ContractRuntime::default(), increment, vec![])
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        let expected_value = initial_value + increment;
        let expected_outcome = ApplicationCallOutcome {
            value: expected_value,
            execution_outcome: ExecutionOutcome::default(),
        };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_outcome);
        assert_eq!(counter.value, expected_value);
    }

    fn create_and_initialize_counter(initial_value: u64) -> Counter {
        let mut counter = Counter::default();

        mock_application_parameters(&());

        let result = counter
            .initialize(&mut ContractRuntime::default(), initial_value)
            .now_or_never()
            .expect("Initialization of counter state should not await anything");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionOutcome::default());
        assert_eq!(counter.value, initial_value);

        counter
    }
}
