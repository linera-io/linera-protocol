// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

extern crate core;

mod utils;

use self::utils::create_dummy_user_application_description;
use async_trait::async_trait;
use linera_base::{
    crypto::PublicKey,
    data_types::BlockHeight,
    identifiers::{ChainDescription, ChainId, Owner, SessionId},
};
use linera_execution::*;
use linera_views::{batch::Batch, common::Context, memory::MemoryContext, views::View};
use std::sync::Arc;

#[tokio::test]
async fn test_missing_bytecode_for_user_application() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;

    let app_desc = create_dummy_user_application_description();
    let app_id = view
        .system
        .registry
        .register_application(app_desc.clone())
        .await?;
    assert_eq!(
        view.system.registry.describe_application(app_id).await?,
        app_desc
    );

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: None,
    };

    let result = view
        .execute_operation(
            ApplicationId::User(app_id),
            &context,
            &Operation::User(vec![]),
        )
        .await;

    assert!(matches!(
        result,
        Err(ExecutionError::ApplicationBytecodeNotFound(desc)) if *desc == app_desc
    ));
    Ok(())
}

struct TestApplication {
    owner: Owner,
}

#[async_trait]
impl UserApplication for TestApplication {
    /// Nothing needs to be done during initialization.
    async fn initialize(
        &self,
        context: &OperationContext,
        _storage: &dyn WritableStorage,
        _argument: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        assert_eq!(context.authenticated_signer, Some(self.owner));
        Ok(RawExecutionResult::default())
    }

    /// Extend the application state with the `operation` bytes.
    ///
    /// Calls itself during the operation, opening a session. The session is intentionally
    /// leaked if the operation is empty.
    async fn execute_operation(
        &self,
        context: &OperationContext,
        storage: &dyn WritableStorage,
        operation: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        // Who we are.
        assert_eq!(context.authenticated_signer, Some(self.owner));
        let app_id = storage.application_id();
        // Modify our state.
        let chosen_key = vec![0];
        storage.lock_view_user_state().await?;
        let state = storage.read_key_bytes(chosen_key.clone()).await?;
        let mut state = state.unwrap_or_default();
        state.extend(operation);
        let mut batch = Batch::new();
        batch.put_key_value_bytes(chosen_key, state);
        storage
            .write_batch_and_unlock(batch)
            .await
            .expect("State is locked at the start of the operation");
        // Call ourselves after the state => ok.
        let call_result = storage
            .try_call_application(/* authenticate */ true, app_id, &[], vec![])
            .await?;
        assert_eq!(call_result.value, Vec::<u8>::new());
        assert_eq!(call_result.sessions.len(), 1);
        if !operation.is_empty() {
            // Call the session to close it.
            let session_id = call_result.sessions[0];
            storage
                .try_call_session(/* authenticate */ false, session_id, &[], vec![])
                .await?;
        }
        Ok(RawExecutionResult::default())
    }

    /// Attempt to call ourself while the state is locked.
    async fn execute_effect(
        &self,
        context: &EffectContext,
        storage: &dyn WritableStorage,
        _effect: &[u8],
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        // Who we are.
        assert_eq!(context.authenticated_signer, Some(self.owner));
        let app_id = storage.application_id();
        storage.lock_view_user_state().await?;
        // Call ourselves while the state is locked => not ok.
        storage
            .try_call_application(/* authenticate */ true, app_id, &[], vec![])
            .await?;
        storage.unlock_view_user_state().await?;
        Ok(RawExecutionResult::default())
    }

    /// Create a session.
    async fn handle_application_call(
        &self,
        context: &CalleeContext,
        _storage: &dyn WritableStorage,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        assert_eq!(context.authenticated_signer, Some(self.owner));
        Ok(ApplicationCallResult {
            create_sessions: vec![NewSession {
                kind: 0,
                data: vec![1],
            }],
            ..ApplicationCallResult::default()
        })
    }

    /// Close the session.
    async fn handle_session_call(
        &self,
        context: &CalleeContext,
        _storage: &dyn WritableStorage,
        _session_kind: u64,
        _session_data: &mut Vec<u8>,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, ExecutionError> {
        assert_eq!(context.authenticated_signer, None);
        Ok(SessionCallResult {
            inner: ApplicationCallResult::default(),
            close_session: true,
        })
    }

    /// Return the application state.
    async fn query_application(
        &self,
        _context: &QueryContext,
        storage: &dyn ServiceRuntime,
        _argument: &[u8],
    ) -> Result<Vec<u8>, ExecutionError> {
        let chosen_key = vec![0];
        storage.lock_view_user_state().await?;
        let state = storage.read_key_bytes(chosen_key).await?;
        let state = state.unwrap_or_default();
        storage.unlock_view_user_state().await?;
        Ok(state)
    }
}

#[tokio::test]
async fn test_simple_user_operation() -> anyhow::Result<()> {
    let owner = Owner::from(PublicKey::debug(0));
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let app_desc = create_dummy_user_application_description();
    let app_id = view
        .system
        .registry
        .register_application(app_desc.clone())
        .await?;
    view.context()
        .extra()
        .user_applications()
        .insert(app_id, Arc::new(TestApplication { owner }));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: Some(owner),
    };
    let result = view
        .execute_operation(
            ApplicationId::User(app_id),
            &context,
            &Operation::User(vec![1]),
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        vec![
            ExecutionResult::User(
                app_id,
                RawExecutionResult::default().with_authenticated_signer(Some(owner))
            ),
            ExecutionResult::User(app_id, RawExecutionResult::default()),
            ExecutionResult::User(
                app_id,
                RawExecutionResult::default().with_authenticated_signer(Some(owner))
            )
        ]
    );

    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    assert_eq!(
        view.query_application(ApplicationId::User(app_id), &context, &Query::User(vec![]),)
            .await
            .unwrap(),
        Response::User(vec![1])
    );
    Ok(())
}

#[tokio::test]
async fn test_simple_user_operation_with_leaking_session() -> anyhow::Result<()> {
    let owner = Owner::from(PublicKey::debug(0));
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let app_desc = create_dummy_user_application_description();
    let app_id = view
        .system
        .registry
        .register_application(app_desc.clone())
        .await?;
    view.context()
        .extra()
        .user_applications()
        .insert(app_id, Arc::new(TestApplication { owner }));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: Some(owner),
    };

    let result = view
        .execute_operation(
            ApplicationId::User(app_id),
            &context,
            &Operation::User(vec![]),
        )
        .await;

    assert!(matches!(result, Err(ExecutionError::SessionWasNotClosed)));
    Ok(())
}
