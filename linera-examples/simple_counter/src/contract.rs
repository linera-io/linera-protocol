// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::SimpleCounter;
use async_trait::async_trait;
use linera_sdk::{
    ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
    OperationContext, Session, SessionCallResult, SessionId, SimpleStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(SimpleCounter);

#[async_trait]
impl Contract for SimpleCounter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        self.value = bcs::from_bytes(argument)?;
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let increment: u128 = bcs::from_bytes(operation)?;
        self.value += increment;
        Ok(ExecutionResult::default())
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        _effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        Err(Error::EffectsNotSupported)
    }

    async fn call_application(
        &mut self,
        _context: &CalleeContext,
        argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        let increment: u128 = bcs::from_bytes(argument)?;
        self.value += increment;
        Ok(ApplicationCallResult {
            value: bcs::to_bytes(&self.value).expect("Serialization should not fail"),
            ..ApplicationCallResult::default()
        })
    }

    async fn call_session(
        &mut self,
        _context: &CalleeContext,
        _session: Session,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error> {
        Err(Error::SessionsNotSupported)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// SimpleCounter application doesn't support any cross-chain effects.
    #[error("SimpleCounter application doesn't support any cross-chain effects")]
    EffectsNotSupported,

    /// SimpleCounter application doesn't support any cross-application sessions.
    #[error("SimpleCounter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Invalid serialized increment value.
    #[error("Invalid serialized increment value")]
    InvalidIncrement(#[from] bcs::Error),
}

#[cfg(test)]
mod tests {
    use super::{Error, SimpleCounter};
    use futures::FutureExt;
    use linera_sdk::{
        ApplicationCallResult, BlockHeight, CalleeContext, ChainId, Contract, EffectContext,
        EffectId, ExecutionResult, OperationContext, Session,
    };
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn operation() {
        let initial_value = 72_u128;
        let mut simple_counter = create_and_initialize_simple_counter(initial_value);

        let increment = 42_308_u128;
        let operation = bcs::to_bytes(&increment).expect("Increment value is not serializable");

        let result = simple_counter
            .execute_operation(&dummy_operation_context(), &operation)
            .now_or_never()
            .expect("Execution of simple_counter operation should not await anything");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::default());
        assert_eq!(simple_counter.value, initial_value + increment);
    }

    #[webassembly_test]
    fn effect() {
        let initial_value = 72_u128;
        let mut simple_counter = create_and_initialize_simple_counter(initial_value);

        let result = simple_counter
            .execute_effect(&dummy_effect_context(), &[])
            .now_or_never()
            .expect("Execution of simple_counter operation should not await anything");

        assert!(matches!(result, Err(Error::EffectsNotSupported)));
        assert_eq!(simple_counter.value, initial_value);
    }

    #[webassembly_test]
    fn cross_application_call() {
        let initial_value = 2_845_u128;
        let mut simple_counter = create_and_initialize_simple_counter(initial_value);

        let increment = 8_u128;
        let argument = bcs::to_bytes(&increment).expect("Increment value is not serializable");

        let result = simple_counter
            .call_application(&dummy_callee_context(), &argument, vec![])
            .now_or_never()
            .expect("Execution of simple_counter operation should not await anything");

        let expected_value = initial_value + increment;
        let expected_result = ApplicationCallResult {
            value: bcs::to_bytes(&expected_value).expect("Expected value is not serializable"),
            create_sessions: vec![],
            execution_result: ExecutionResult::default(),
        };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_result);
        assert_eq!(simple_counter.value, expected_value);
    }

    #[webassembly_test]
    fn sessions() {
        let initial_value = 72_u128;
        let mut simple_counter = create_and_initialize_simple_counter(initial_value);

        let result = simple_counter
            .call_session(&dummy_callee_context(), Session::default(), &[], vec![])
            .now_or_never()
            .expect("Execution of simple_counter operation should not await anything");

        assert!(matches!(result, Err(Error::SessionsNotSupported)));
        assert_eq!(simple_counter.value, initial_value);
    }

    fn create_and_initialize_simple_counter(initial_value: u128) -> SimpleCounter {
        let mut simple_counter = SimpleCounter::default();
        let initial_argument =
            bcs::to_bytes(&initial_value).expect("Initial value is not serializable");

        let result = simple_counter
            .initialize(&dummy_operation_context(), &initial_argument)
            .now_or_never()
            .expect("Initialization of simple_counter state should not await anything");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::default());
        assert_eq!(simple_counter.value, initial_value);

        simple_counter
    }

    fn dummy_operation_context() -> OperationContext {
        OperationContext {
            chain_id: ChainId([0; 8].into()),
            height: BlockHeight(0),
            index: 0,
        }
    }

    fn dummy_effect_context() -> EffectContext {
        EffectContext {
            chain_id: ChainId([0; 8].into()),
            height: BlockHeight(0),
            effect_id: EffectId {
                chain_id: ChainId([1; 8].into()),
                height: BlockHeight(1),
                index: 1,
            },
        }
    }

    fn dummy_callee_context() -> CalleeContext {
        CalleeContext {
            chain_id: ChainId([0; 8].into()),
            authenticated_caller_id: None,
        }
    }
}
