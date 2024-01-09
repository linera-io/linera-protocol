// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Counter;
use async_trait::async_trait;
use linera_sdk::{
    base::{SessionId, WithContractAbi},
    ApplicationCallOutcome, CalleeContext, Contract, ExecutionOutcome, MessageContext,
    OperationContext, SessionCallOutcome, SimpleStateStorage,
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
        _context: &OperationContext,
        value: u64,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());

        self.value = value;

        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation: u64,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        self.value += operation;
        Ok(ExecutionOutcome::default())
    }

    async fn execute_message(
        &mut self,
        _context: &MessageContext,
        _message: (),
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        Err(Error::MessagesNotSupported)
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        increment: u64,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<
        ApplicationCallOutcome<Self::Message, Self::Response, Self::SessionState>,
        Self::Error,
    > {
        self.value += increment;
        Ok(ApplicationCallOutcome {
            value: self.value,
            ..ApplicationCallOutcome::default()
        })
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _state: Self::SessionState,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        Err(Error::SessionsNotSupported)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Counter application doesn't support any cross-chain messages.
    #[error("Counter application doesn't support any cross-chain messages")]
    MessagesNotSupported,

    /// Counter application doesn't support any cross-application sessions.
    #[error("Counter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

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
    use futures::FutureExt;
    use linera_sdk::{
        base::{BlockHeight, ChainId, MessageId},
        test::mock_application_parameters,
        ApplicationCallOutcome, CalleeContext, Contract, ExecutionOutcome, MessageContext,
        OperationContext,
    };
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn operation() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let increment = 42_308_u64;

        let result = counter
            .execute_operation(&dummy_operation_context(), increment)
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
            .execute_message(&dummy_message_context(), ())
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert!(matches!(result, Err(Error::MessagesNotSupported)));
        assert_eq!(counter.value, initial_value);
    }

    #[webassembly_test]
    fn cross_application_call() {
        let initial_value = 2_845_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let increment = 8_u64;

        let result = counter
            .handle_application_call(&dummy_callee_context(), increment, vec![])
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        let expected_value = initial_value + increment;
        let expected_outcome = ApplicationCallOutcome {
            value: expected_value,
            create_sessions: vec![],
            execution_outcome: ExecutionOutcome::default(),
        };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_outcome);
        assert_eq!(counter.value, expected_value);
    }

    #[webassembly_test]
    fn sessions() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let result = counter
            .handle_session_call(&dummy_callee_context(), Default::default(), (), vec![])
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert!(matches!(result, Err(Error::SessionsNotSupported)));
        assert_eq!(counter.value, initial_value);
    }

    fn create_and_initialize_counter(initial_value: u64) -> Counter {
        let mut counter = Counter::default();

        mock_application_parameters(&());

        let result = counter
            .initialize(&dummy_operation_context(), initial_value)
            .now_or_never()
            .expect("Initialization of counter state should not await anything");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionOutcome::default());
        assert_eq!(counter.value, initial_value);

        counter
    }

    fn dummy_operation_context() -> OperationContext {
        OperationContext {
            chain_id: ChainId([0; 4].into()),
            authenticated_signer: None,
            height: BlockHeight(0),
            index: 0,
        }
    }

    fn dummy_message_context() -> MessageContext {
        MessageContext {
            chain_id: ChainId([0; 4].into()),
            is_bouncing: false,
            authenticated_signer: None,
            height: BlockHeight(0),
            message_id: MessageId {
                chain_id: ChainId([1; 4].into()),
                height: BlockHeight(1),
                index: 1,
            },
        }
    }

    fn dummy_callee_context() -> CalleeContext {
        CalleeContext {
            chain_id: ChainId([0; 4].into()),
            authenticated_signer: None,
            authenticated_caller_id: None,
        }
    }
}
