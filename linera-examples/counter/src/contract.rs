// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Counter;
use async_trait::async_trait;
use linera_sdk::{
    base::SessionId, ApplicationCallResult, CalleeContext, Contract, EffectContext,
    ExecutionResult, OperationContext, Session, SessionCallResult, SimpleStateStorage,
};
use thiserror::Error;
use crate::state::CounterOperation;

linera_sdk::contract!(Counter);

#[async_trait]
impl Contract for Counter {
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
        let counter_operation: CounterOperation = bcs::from_bytes(operation)?;
        self.value += counter_operation.increment;
        Ok(ExecutionResult::default())
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        _effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        Err(Error::EffectsNotSupported)
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        log::error!("received {:?}", argument);
        let counter_operation: CounterOperation = bcs::from_bytes(argument)?;
        log::error!("incrementing by {:?}", counter_operation);
        self.value += counter_operation.increment;
        Ok(ApplicationCallResult {
            value: bcs::to_bytes(&self.value).expect("Serialization should not fail"),
            ..ApplicationCallResult::default()
        })
    }

    async fn handle_session_call(
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
    /// Counter application doesn't support any cross-chain effects.
    #[error("Counter application doesn't support any cross-chain effects")]
    EffectsNotSupported,

    /// Counter application doesn't support any cross-application sessions.
    #[error("Counter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Invalid serialized increment value.
    #[error("Invalid serialized increment value")]
    InvalidIncrement(#[from] bcs::Error),
}

#[cfg(test)]
mod tests {
    use super::{Counter, Error};
    use futures::FutureExt;
    use linera_sdk::{
        base::{BlockHeight, ChainId, EffectId},
        ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
        OperationContext, Session,
    };
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn operation() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let increment = 42_308_u64;
        let operation = bcs::to_bytes(&increment).expect("Increment value is not serializable");

        let result = counter
            .execute_operation(&dummy_operation_context(), &operation)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::default());
        assert_eq!(counter.value, initial_value + increment);
    }

    #[webassembly_test]
    fn effect() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let result = counter
            .execute_effect(&dummy_effect_context(), &[])
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert!(matches!(result, Err(Error::EffectsNotSupported)));
        assert_eq!(counter.value, initial_value);
    }

    #[webassembly_test]
    fn cross_application_call() {
        let initial_value = 2_845_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let increment = 8_u64;
        let argument = bcs::to_bytes(&increment).expect("Increment value is not serializable");

        let result = counter
            .handle_application_call(&dummy_callee_context(), &argument, vec![])
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        let expected_value = initial_value + increment;
        let expected_result = ApplicationCallResult {
            value: bcs::to_bytes(&expected_value).expect("Expected value is not serializable"),
            create_sessions: vec![],
            execution_result: ExecutionResult::default(),
        };

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_result);
        assert_eq!(counter.value, expected_value);
    }

    #[webassembly_test]
    fn sessions() {
        let initial_value = 72_u64;
        let mut counter = create_and_initialize_counter(initial_value);

        let result = counter
            .handle_session_call(&dummy_callee_context(), Session::default(), &[], vec![])
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        assert!(matches!(result, Err(Error::SessionsNotSupported)));
        assert_eq!(counter.value, initial_value);
    }

    fn create_and_initialize_counter(initial_value: u64) -> Counter {
        let mut counter = Counter::default();
        let initial_argument =
            bcs::to_bytes(&initial_value).expect("Initial value is not serializable");

        let result = counter
            .initialize(&dummy_operation_context(), &initial_argument)
            .now_or_never()
            .expect("Initialization of counter state should not await anything");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ExecutionResult::default());
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

    fn dummy_effect_context() -> EffectContext {
        EffectContext {
            chain_id: ChainId([0; 4].into()),
            authenticated_signer: None,
            height: BlockHeight(0),
            effect_id: EffectId {
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
