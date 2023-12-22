// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

mod utils;

use self::utils::create_dummy_user_application_description;
use linera_base::{
    crypto::PublicKey,
    data_types::BlockHeight,
    ensure,
    identifiers::{ChainDescription, ChainId, Owner, SessionId},
};
use linera_execution::{
    policy::ResourceControlPolicy, ContractActorRuntime, ContractSyncRuntime, ServiceActorRuntime,
    ServiceSyncRuntime, *,
};
use linera_views::{batch::Batch, common::Context, memory::MemoryContext, views::View};
use std::sync::Arc;
use test_case::test_case;

#[tokio::test]
async fn test_missing_bytecode_for_user_application() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            Default::default(),
        )
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

#[derive(Clone)]
struct TestModule {
    owner: Owner,
}

struct TestApplication<Runtime> {
    owner: Owner,
    runtime: Runtime,
}

impl TestModule {
    fn new(owner: Owner) -> Self {
        Self { owner }
    }
}

/// A fake operation for the [`TestApplication`] which can be easily "serialized" into a single
/// byte.
#[repr(u8)]
enum TestOperation {
    Completely,
    LeakingSession,
    FailingCrossApplicationCall,
}

impl UserContractModule for TestModule {
    fn instantiate_with_actor_runtime(
        &self,
        runtime: ContractActorRuntime,
    ) -> Result<Box<dyn UserContract + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(TestApplication {
            owner: self.owner,
            runtime,
        }))
    }

    fn instantiate_with_sync_runtime(
        &self,
        runtime: ContractSyncRuntime,
    ) -> Result<Box<dyn UserContract + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(TestApplication {
            owner: self.owner,
            runtime,
        }))
    }
}

impl<Runtime> UserContract for TestApplication<Runtime>
where
    Runtime: ContractRuntime,
{
    /// Nothing needs to be done during initialization.
    fn initialize(
        &mut self,
        context: OperationContext,
        _argument: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        assert_eq!(context.authenticated_signer, Some(self.owner));
        Ok(RawExecutionResult::default())
    }

    /// Extend the application state with the `operation` bytes.
    ///
    /// Calls itself during the operation, opening a session. The session is intentionally
    /// leaked if the test operation is [`TestOperation::LeakingSession`].
    fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        assert_eq!(operation.len(), 1);
        // Who we are.
        assert_eq!(context.authenticated_signer, Some(self.owner));
        let app_id = self.runtime.application_id()?;

        // Modify our state.
        let chosen_key = vec![0];
        self.runtime.lock()?;
        let mut state = self
            .runtime
            .read_value_bytes(chosen_key.clone())?
            .unwrap_or_default();
        state.extend(operation.clone());
        let mut batch = Batch::new();
        batch.put_key_value_bytes(chosen_key, state);
        self.runtime.write_batch_and_unlock(batch)?;

        // Call ourselves after unlocking the state => ok.
        let call_result = self.runtime.try_call_application(
            /* authenticated */ true,
            app_id,
            operation.clone(),
            vec![],
        )?;
        assert_eq!(call_result.value, Vec::<u8>::new());
        assert_eq!(call_result.sessions.len(), 1);
        if operation[0] != TestOperation::LeakingSession as u8 {
            // Call the session to close it.
            let session_id = call_result.sessions[0];
            self.runtime.try_call_session(
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
        &mut self,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<RawExecutionResult<Vec<u8>>, ExecutionError> {
        assert_eq!(message.len(), 1);
        // Who we are.
        assert_eq!(context.authenticated_signer, Some(self.owner));
        let app_id = self.runtime.application_id()?;
        self.runtime.lock()?;

        // Call ourselves while the state is locked => not ok.
        self.runtime.try_call_application(
            /* authenticated */ true,
            app_id,
            message,
            vec![],
        )?;
        self.runtime.unlock()?;

        Ok(RawExecutionResult::default())
    }

    /// Creates a session.
    fn handle_application_call(
        &mut self,
        context: CalleeContext,
        argument: Vec<u8>,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, ExecutionError> {
        assert_eq!(argument.len(), 1);
        assert_eq!(context.authenticated_signer, Some(self.owner));
        ensure!(
            argument[0] != TestOperation::FailingCrossApplicationCall as u8,
            ExecutionError::UserError("Cross-application call failed".to_owned())
        );
        Ok(ApplicationCallResult {
            create_sessions: vec![vec![1]],
            ..ApplicationCallResult::default()
        })
    }

    /// Closes the session.
    fn handle_session_call(
        &mut self,
        context: CalleeContext,
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

impl UserServiceModule for TestModule {
    fn instantiate_with_actor_runtime(
        &self,
        runtime: ServiceActorRuntime,
    ) -> Result<Box<dyn UserService + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(TestApplication {
            owner: self.owner,
            runtime,
        }))
    }

    fn instantiate_with_sync_runtime(
        &self,
        runtime: ServiceSyncRuntime,
    ) -> Result<Box<dyn UserService + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(TestApplication {
            owner: self.owner,
            runtime,
        }))
    }
}

impl<Runtime> UserService for TestApplication<Runtime>
where
    Runtime: ServiceRuntime,
{
    /// Returns the application state.
    fn handle_query(
        &mut self,
        _context: QueryContext,
        _argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let chosen_key = vec![0];
        self.runtime.lock()?;

        let state = self
            .runtime
            .read_value_bytes(chosen_key)?
            .unwrap_or_default();

        self.runtime.unlock()?;

        Ok(state)
    }
}

#[test_case(ExecutionRuntimeConfig::Actor ; "actor")]
#[test_case(ExecutionRuntimeConfig::Synchronous ; "synchronous")]
#[tokio::test]
async fn test_simple_user_operation(
    execution_runtime_config: ExecutionRuntimeConfig,
) -> anyhow::Result<()> {
    let owner = Owner::from(PublicKey::debug(0));
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            execution_runtime_config,
        )
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
        .insert(app_id, Arc::new(TestModule::new(owner)));
    view.context()
        .extra()
        .user_services()
        .insert(app_id, Arc::new(TestModule::new(owner)));

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
                bytes: vec![TestOperation::Completely as u8],
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
        Response::User(vec![TestOperation::Completely as u8])
    );
    Ok(())
}

#[test_case(ExecutionRuntimeConfig::Actor ; "actor")]
#[test_case(ExecutionRuntimeConfig::Synchronous ; "synchronous")]
#[tokio::test]
async fn test_simple_user_operation_with_leaking_session(
    execution_runtime_config: ExecutionRuntimeConfig,
) -> anyhow::Result<()> {
    let owner = Owner::from(PublicKey::debug(0));
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            execution_runtime_config,
        )
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
        .insert(app_id, Arc::new(TestModule::new(owner)));

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
                bytes: vec![TestOperation::LeakingSession as u8],
            },
            &policy,
            &mut tracker,
        )
        .await;

    assert!(matches!(
        result,
        Err(ExecutionError::SessionWasNotClosed(_))
    ));
    Ok(())
}

/// Tests if user application errors when handling cross-application calls are handled correctly.
///
/// Sends an operation to the [`TestApplication`] requesting it to fail a cross-application call.
/// It is then forwarded to the reentrant call, where the cross-application call handler fails and
/// the execution error should be handled correctly (without panicking).
#[test_case(ExecutionRuntimeConfig::Actor ; "actor")]
#[test_case(ExecutionRuntimeConfig::Synchronous ; "synchronous")]
#[tokio::test]
async fn test_cross_application_error(
    execution_runtime_config: ExecutionRuntimeConfig,
) -> anyhow::Result<()> {
    let owner = Owner::from(PublicKey::debug(0));
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            execution_runtime_config,
        )
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
        .insert(app_id, Arc::new(TestModule::new(owner)));
    view.context()
        .extra()
        .user_services()
        .insert(app_id, Arc::new(TestModule::new(owner)));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: Some(owner),
        next_message_index: 0,
    };
    let mut tracker = ResourceTracker::default();
    let policy = ResourceControlPolicy::default();
    assert!(matches!(
        view.execute_operation(
            context,
            Operation::User {
                application_id: app_id,
                bytes: vec![TestOperation::FailingCrossApplicationCall as u8],
            },
            &policy,
            &mut tracker,
        )
        .await,
        Err(ExecutionError::UserError(_))
    ));

    Ok(())
}
