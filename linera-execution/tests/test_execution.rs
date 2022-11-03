// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

extern crate core;

use async_trait::async_trait;
use linera_base::messages::{ApplicationId, BlockHeight, ChainDescription, ChainId};
use linera_execution::*;
use linera_views::{
    memory::MemoryContext,
    views::{Context, View},
};
use std::sync::Arc;

#[tokio::test]
async fn test_missing_user_application() {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;

    let app_id = ApplicationId(1);

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
    };

    let result = view
        .execute_operation(app_id, &context, &Operation::User(vec![]))
        .await;

    assert!(matches!(result, Err(ExecutionError::UnknownApplication)))
}

struct TestApplication;

#[async_trait]
impl UserApplication for TestApplication {
    /// Extend the application state with the `operation` bytes.
    ///
    /// Calls itself during the operation, opening a session. The session is intentionally
    /// leaked if the operation is empty.
    async fn execute_operation(
        &self,
        _context: &OperationContext,
        storage: &dyn WritableStorage,
        operation: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        // Who we are.
        let app_id = storage.application_id();
        // Modify our state.
        let mut state = storage.try_read_and_lock_my_state().await?;
        state.extend(operation);
        storage
            .save_and_unlock_my_state(state)
            .expect("State is locked at the start of the operation");
        // Call ourselves after the state => ok.
        let call_result = storage
            .try_call_application(/* authenticate */ true, app_id, &[], vec![])
            .await?;
        assert_eq!(call_result.value, vec![]);
        assert_eq!(call_result.sessions.len(), 1);
        if !operation.is_empty() {
            // Call the session to close it.
            let session_id = call_result.sessions[0];
            storage
                .try_call_session(/* authenticate */ true, session_id, &[], vec![])
                .await?;
        }
        Ok(RawExecutionResult::default())
    }

    /// Attempt to call ourself while the state is locked.
    async fn execute_effect(
        &self,
        _context: &EffectContext,
        storage: &dyn WritableStorage,
        _effect: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        // Who we are.
        let app_id = storage.application_id();
        storage.try_read_and_lock_my_state().await?;
        // Call ourselves while the state is locked => not ok.
        storage
            .try_call_application(/* authenticate */ true, app_id, &[], vec![])
            .await?;
        storage.unlock_my_state();
        Ok(RawExecutionResult::default())
    }

    /// Create a session.
    async fn call_application(
        &self,
        _context: &CalleeContext,
        _storage: &dyn WritableStorage,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        Ok(ApplicationCallResult {
            create_sessions: vec![NewSession {
                kind: 0,
                data: vec![1],
            }],
            ..ApplicationCallResult::default()
        })
    }

    /// Close the session.
    async fn call_session(
        &self,
        _context: &CalleeContext,
        _storage: &dyn WritableStorage,
        _session_kind: u64,
        _session_data: &mut Vec<u8>,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, ExecutionError> {
        Ok(SessionCallResult {
            inner: ApplicationCallResult::default(),
            close_session: true,
        })
    }

    /// Return the application state.
    async fn query_application(
        &self,
        _context: &QueryContext,
        storage: &dyn QueryableStorage,
        _argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError> {
        let state = storage.try_read_my_state().await?;
        Ok(state)
    }
}

#[tokio::test]
async fn test_simple_user_operation() {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let app_id = ApplicationId(1);
    view.context()
        .extra()
        .user_applications()
        .insert(app_id, Arc::new(TestApplication));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
    };
    let result = view
        .execute_operation(app_id, &context, &Operation::User(vec![1]))
        .await
        .unwrap();
    assert_eq!(
        result,
        vec![
            ExecutionResult::User(app_id, RawExecutionResult::default()),
            ExecutionResult::User(app_id, RawExecutionResult::default()),
            ExecutionResult::User(app_id, RawExecutionResult::default())
        ]
    );

    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    assert_eq!(
        view.query_application(app_id, &context, &Query::User(vec![]))
            .await
            .unwrap(),
        Response::User(vec![1])
    );
}

#[tokio::test]
async fn test_simple_user_operation_with_leaking_session() {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let app_id = ApplicationId(1);
    view.context()
        .extra()
        .user_applications()
        .insert(app_id, Arc::new(TestApplication));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
    };

    let result = view
        .execute_operation(app_id, &context, &Operation::User(vec![]))
        .await;

    assert!(matches!(result, Err(ExecutionError::SessionWasNotClosed)))
}
