// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

mod utils;

use self::utils::create_dummy_user_application_description;
use linera_base::{
    crypto::PublicKey,
    data_types::BlockHeight,
    identifiers::{ChainDescription, ChainId, Owner, SessionId},
};
use linera_execution::{
    policy::ResourceControlPolicy,
    runtime_actor::{ContractRuntimeSender, ServiceRuntimeSender},
    *,
};
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
        next_message_index: 0,
    };
    let mut tracker = ResourceTracker::default();
    let policy = ResourceControlPolicy::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: app_id,
                bytes: vec![],
            },
            &policy,
            &mut tracker,
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

impl UserContract for TestApplication {
    /// Nothing needs to be done during initialization.
    fn initialize(
        &self,
        context: OperationContext,
        _runtime_sender: ContractRuntimeSender,
        _argument: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        assert_eq!(context.authenticated_signer, Some(self.owner));
        Ok(RawExecutionResult::default())
    }

    /// Extend the application state with the `operation` bytes.
    ///
    /// Calls itself during the operation, opening a session. The session is intentionally
    /// leaked if the operation is empty.
    fn execute_operation(
        &self,
        context: OperationContext,
        mut runtime_sender: ContractRuntimeSender,
        operation: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        // Who we are.
        assert_eq!(context.authenticated_signer, Some(self.owner));
        let app_id = runtime_sender.application_id()?;

        // Modify our state.
        let chosen_key = vec![0];
        runtime_sender.lock()?;
        let mut state = runtime_sender
            .read_value_bytes(chosen_key.clone())?
            .unwrap_or_default();
        state.extend(operation.clone());
        let mut batch = Batch::new();
        batch.put_key_value_bytes(chosen_key, state);
        runtime_sender.write_batch_and_unlock(batch)?;

        // Call ourselves after unlocking the state => ok.
        let call_result = runtime_sender.try_call_application(
            /* authenticated */ true,
            app_id,
            vec![],
            vec![],
        )?;
        assert_eq!(call_result.value, Vec::<u8>::new());
        assert_eq!(call_result.sessions.len(), 1);
        if !operation.is_empty() {
            // Call the session to close it.
            let session_id = call_result.sessions[0];
            runtime_sender.try_call_session(
                /* authenticated */ false,
                session_id,
                vec![],
                vec![],
            )?;
        }
        Ok(RawExecutionResult::default())
    }

    /// Attempts to call ourself while the state is locked.
    fn execute_message(
        &self,
        context: MessageContext,
        mut runtime_sender: ContractRuntimeSender,
        _message: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        // Who we are.
        assert_eq!(context.authenticated_signer, Some(self.owner));
        let app_id = runtime_sender.application_id()?;
        runtime_sender.lock()?;

        // Call ourselves while the state is locked => not ok.
        runtime_sender.try_call_application(
            /* authenticated */ true,
            app_id,
            vec![],
            vec![],
        )?;
        runtime_sender.unlock()?;

        Ok(RawExecutionResult::default())
    }

    /// Creates a session.
    fn handle_application_call(
        &self,
        context: CalleeContext,
        _runtime_sender: ContractRuntimeSender,
        _argument: Vec<u8>,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        assert_eq!(context.authenticated_signer, Some(self.owner));
        Ok(ApplicationCallResult {
            create_sessions: vec![vec![1]],
            ..ApplicationCallResult::default()
        })
    }

    /// Closes the session.
    fn handle_session_call(
        &self,
        context: CalleeContext,
        _runtime_sender: ContractRuntimeSender,
        session_state: Vec<u8>,
        _argument: Vec<u8>,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<(SessionCallResult, Vec<u8>), ExecutionError> {
        assert_eq!(context.authenticated_signer, None);
        Ok((
            SessionCallResult {
                inner: ApplicationCallResult::default(),
                close_session: true,
            },
            session_state,
        ))
    }
}

impl UserService for TestApplication {
    /// Returns the application state.
    fn handle_query(
        &self,
        _context: QueryContext,
        mut runtime_sender: ServiceRuntimeSender,
        _argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let chosen_key = vec![0];
        runtime_sender.lock()?;

        let state = runtime_sender
            .read_value_bytes(chosen_key)?
            .unwrap_or_default();

        runtime_sender.unlock()?;

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
        .user_contracts()
        .insert(app_id, Arc::new(TestApplication { owner }));
    view.context()
        .extra()
        .user_services()
        .insert(app_id, Arc::new(TestApplication { owner }));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: Some(owner),
        next_message_index: 0,
    };
    let mut tracker = ResourceTracker::default();
    let policy = ResourceControlPolicy::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: app_id,
                bytes: vec![1],
            },
            &policy,
            &mut tracker,
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
        view.query_application(
            context,
            Query::User {
                application_id: app_id,
                bytes: vec![]
            }
        )
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
        .user_contracts()
        .insert(app_id, Arc::new(TestApplication { owner }));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: Some(owner),
        next_message_index: 0,
    };
    let mut tracker = ResourceTracker::default();
    let policy = ResourceControlPolicy::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: app_id,
                bytes: vec![],
            },
            &policy,
            &mut tracker,
        )
        .await;

    assert!(matches!(result, Err(ExecutionError::SessionWasNotClosed)));
    Ok(())
}
