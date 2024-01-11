// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

mod utils;

use self::utils::{create_dummy_user_application_description, ExpectedCall, MockApplication};
use linera_base::{
    crypto::PublicKey,
    data_types::BlockHeight,
    ensure,
    identifiers::{ChainDescription, ChainId, Owner, SessionId},
};
use linera_execution::{policy::ResourceControlPolicy, ContractSyncRuntime, ServiceSyncRuntime, *};
use linera_views::{
    batch::Batch,
    common::Context,
    memory::MemoryContext,
    views::{View, ViewError},
};
use std::{sync::Arc, vec};

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

    let (app_id, app_desc) =
        &create_dummy_user_application_registrations(&mut view.system.registry, 1).await?[0];

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
                application_id: *app_id,
                bytes: vec![],
            },
            &policy,
            &mut tracker,
        )
        .await;

    assert!(matches!(
        result,
        Err(ExecutionError::ApplicationBytecodeNotFound(desc)) if &*desc == app_desc
    ));
    Ok(())
}

#[derive(Clone)]
struct TestModule<const IS_CALLER: bool> {
    owner: Owner,
}

struct TestApplication<const IS_CALLER: bool, Runtime> {
    owner: Owner,
    runtime: Runtime,
}

impl<const IS_CALLER: bool> TestModule<IS_CALLER> {
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

impl UserContractModule for TestModule<true> {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntime,
    ) -> Result<Box<dyn UserContract + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(TestApplication::<true, ContractSyncRuntime> {
            owner: self.owner,
            runtime,
        }))
    }
}

impl UserContractModule for TestModule<false> {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntime,
    ) -> Result<Box<dyn UserContract + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(TestApplication::<false, ContractSyncRuntime> {
            owner: self.owner,
            runtime,
        }))
    }
}

/// Key to store the application ID to call.
static CALLEE_ID_KEY: [u8; 1] = [0];
/// Key to store some dummy data in the application state.
static DUMMY_STATE_KEY: [u8; 1] = [1];

impl<Runtime> UserContract for TestApplication<true, Runtime>
where
    Runtime: ContractRuntime,
{
    /// Store the application ID to be called.
    fn initialize(
        &mut self,
        context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        assert_eq!(context.authenticated_signer, Some(self.owner));
        assert!(bcs::from_bytes::<UserApplicationId>(&argument).is_ok());

        let mut batch = Batch::new();
        batch.put_key_value_bytes(CALLEE_ID_KEY.to_vec(), argument);
        self.runtime.write_batch(batch)?;

        Ok(RawExecutionOutcome::default())
    }

    /// Extend the application state with the `operation` bytes.
    ///
    /// Calls the callee test application during the operation, opening a session. The session is
    /// intentionally leaked if the test operation is [`TestOperation::LeakingSession`].
    fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        assert_eq!(operation.len(), 1);
        // Who we are.
        assert_eq!(context.authenticated_signer, Some(self.owner));

        // Read the application ID to call
        let callee_id_bytes = self
            .runtime
            .read_value_bytes(CALLEE_ID_KEY.to_vec())?
            .expect("Missing application ID to call");
        let callee_id = bcs::from_bytes(&callee_id_bytes)
            .expect("Failed to deserialize application ID to call");

        // Modify our state.
        let mut state = self
            .runtime
            .read_value_bytes(DUMMY_STATE_KEY.to_vec())?
            .unwrap_or_default();
        state.extend(operation.clone());
        let mut batch = Batch::new();
        batch.put_key_value_bytes(DUMMY_STATE_KEY.to_vec(), state);
        self.runtime.write_batch(batch)?;

        // Call ourselves after unlocking the state => ok.
        let call_outcome = self.runtime.try_call_application(
            /* authenticated */ true,
            callee_id,
            operation.clone(),
            vec![],
        )?;
        assert_eq!(call_outcome.value, Vec::<u8>::new());
        assert_eq!(call_outcome.sessions.len(), 1);
        if operation[0] != TestOperation::LeakingSession as u8 {
            // Call the session to close it.
            let session_id = call_outcome.sessions[0];
            self.runtime.try_call_session(
                /* authenticated */ false,
                session_id,
                vec![],
                vec![],
            )?;
        }
        Ok(RawExecutionOutcome::default())
    }

    /// Attempts to call ourself.
    fn execute_message(
        &mut self,
        context: MessageContext,
        message: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        assert_eq!(message.len(), 1);
        // Who we are.
        assert_eq!(context.authenticated_signer, Some(self.owner));
        let app_id = self.runtime.application_id()?;

        // Call ourselves => not ok.
        self.runtime.try_call_application(
            /* authenticated */ true,
            app_id,
            message,
            vec![],
        )?;

        Ok(RawExecutionOutcome::default())
    }

    fn handle_application_call(
        &mut self,
        _context: CalleeContext,
        _argument: Vec<u8>,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallOutcome, ExecutionError> {
        unreachable!("Caller test application does not support being called");
    }

    fn handle_session_call(
        &mut self,
        _context: CalleeContext,
        _session_state: Vec<u8>,
        _argument: Vec<u8>,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<(SessionCallOutcome, Vec<u8>), ExecutionError> {
        unreachable!("Caller test application does not support being called");
    }
}

impl<Runtime> UserContract for TestApplication<false, Runtime>
where
    Runtime: ContractRuntime,
{
    /// Nothing needs to be done during initialization.
    fn initialize(
        &mut self,
        context: OperationContext,
        _argument: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        assert_eq!(context.authenticated_signer, Some(self.owner));
        Ok(RawExecutionOutcome::default())
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        _operation: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        unreachable!("Callee test application does not support starting transactions");
    }

    fn execute_message(
        &mut self,
        _context: MessageContext,
        _message: Vec<u8>,
    ) -> Result<RawExecutionOutcome<Vec<u8>>, ExecutionError> {
        unreachable!("Callee test application does not support starting transactions");
    }

    /// Creates a session.
    fn handle_application_call(
        &mut self,
        context: CalleeContext,
        argument: Vec<u8>,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallOutcome, ExecutionError> {
        assert_eq!(argument.len(), 1);
        assert_eq!(context.authenticated_signer, Some(self.owner));
        ensure!(
            argument[0] != TestOperation::FailingCrossApplicationCall as u8,
            ExecutionError::UserError("Cross-application call failed".to_owned())
        );
        Ok(ApplicationCallOutcome {
            create_sessions: vec![vec![1]],
            ..ApplicationCallOutcome::default()
        })
    }

    /// Closes the session.
    fn handle_session_call(
        &mut self,
        context: CalleeContext,
        session_state: Vec<u8>,
        _argument: Vec<u8>,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<(SessionCallOutcome, Vec<u8>), ExecutionError> {
        assert_eq!(context.authenticated_signer, None);
        Ok((
            SessionCallOutcome {
                inner: ApplicationCallOutcome::default(),
                close_session: true,
            },
            session_state,
        ))
    }
}

impl<const IS_CALLER: bool> UserServiceModule for TestModule<IS_CALLER> {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntime,
    ) -> Result<Box<dyn UserService + Send + Sync + 'static>, ExecutionError> {
        Ok(Box::new(TestApplication::<IS_CALLER, ServiceSyncRuntime> {
            owner: self.owner,
            runtime,
        }))
    }
}

impl<const IS_CALLER: bool, Runtime> UserService for TestApplication<IS_CALLER, Runtime>
where
    Runtime: ServiceRuntime,
{
    /// Returns the application state.
    fn handle_query(
        &mut self,
        _context: QueryContext,
        _argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let state = self
            .runtime
            .read_value_bytes(DUMMY_STATE_KEY.to_vec())?
            .unwrap_or_default();

        Ok(state)
    }
}

#[tokio::test]
async fn test_simple_user_operation() -> anyhow::Result<()> {
    let owner = Owner::from(PublicKey::debug(0));
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            ExecutionRuntimeConfig::Synchronous,
        )
        .await;
    let application_ids = register_test_applications(owner, &mut view).await?;

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: Some(owner),
        next_message_index: 0,
    };
    let mut tracker = ResourceTracker::default();
    let policy = ResourceControlPolicy::default();
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id: application_ids[0],
                bytes: vec![TestOperation::Completely as u8],
            },
            &policy,
            &mut tracker,
        )
        .await
        .unwrap();
    assert_eq!(
        outcomes,
        vec![
            ExecutionOutcome::User(
                application_ids[1],
                RawExecutionOutcome::default().with_authenticated_signer(Some(owner))
            ),
            ExecutionOutcome::User(application_ids[1], RawExecutionOutcome::default()),
            ExecutionOutcome::User(
                application_ids[0],
                RawExecutionOutcome::default().with_authenticated_signer(Some(owner))
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
                application_id: application_ids[0],
                bytes: vec![]
            }
        )
        .await
        .unwrap(),
        Response::User(vec![TestOperation::Completely as u8])
    );
    Ok(())
}

#[tokio::test]
async fn test_simple_user_operation_with_leaking_session() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            ExecutionRuntimeConfig::Synchronous,
        )
        .await;

    let mut applications = register_mock_applications(&mut view, 2).await?;
    let (caller_id, caller_application) = applications
        .next()
        .expect("Missing caller mock application");
    let (target_id, target_application) = applications
        .next()
        .expect("Missing target mock application");

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.try_call_application(false, target_id, vec![], vec![])?;
            Ok(RawExecutionOutcome::default())
        },
    ));

    target_application.expect_call(ExpectedCall::handle_application_call(
        |_runtime, _context, _argument, _forwarded_sessions| {
            Ok(ApplicationCallOutcome {
                create_sessions: vec![vec![]],
                ..ApplicationCallOutcome::default()
            })
        },
    ));

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
                application_id: caller_id,
                bytes: vec![],
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
#[tokio::test]
async fn test_cross_application_error() -> anyhow::Result<()> {
    let owner = Owner::from(PublicKey::debug(0));
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            ExecutionRuntimeConfig::Synchronous,
        )
        .await;
    let application_ids = register_test_applications(owner, &mut view).await?;

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
                application_id: application_ids[0],
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

/// Creates `count` [`MockApplication`]s and registers them in the provided [`ExecutionStateView`].
///
/// Returns an iterator over pairs of [`UserApplicationId`]s and their respective
/// [`MockApplication`]s.
pub async fn register_mock_applications<C>(
    state: &mut ExecutionStateView<C>,
    count: u64,
) -> anyhow::Result<vec::IntoIter<(UserApplicationId, MockApplication<()>)>>
where
    C: Context<Extra = TestExecutionRuntimeContext> + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    let mock_applications: Vec<_> =
        create_dummy_user_application_registrations(&mut state.system.registry, count)
            .await?
            .into_iter()
            .map(|(id, _description)| (id, MockApplication::default()))
            .collect();
    let extra = state.context().extra();

    for (id, mock_application) in &mock_applications {
        extra
            .user_contracts()
            .insert(*id, Arc::new(mock_application.clone()));
        extra
            .user_services()
            .insert(*id, Arc::new(mock_application.clone()));
    }

    Ok(mock_applications.into_iter())
}

pub async fn register_test_applications<C>(
    owner: Owner,
    state: &mut ExecutionStateView<C>,
) -> anyhow::Result<Vec<UserApplicationId>>
where
    C: Context<Extra = TestExecutionRuntimeContext> + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    let (application_ids, _application_descriptions): (Vec<_>, Vec<_>) =
        create_dummy_user_application_registrations(&mut state.system.registry, 2)
            .await?
            .into_iter()
            .unzip();
    let extra = state.context().extra();

    extra
        .user_contracts()
        .insert(application_ids[0], Arc::new(TestModule::<true>::new(owner)));
    extra
        .user_services()
        .insert(application_ids[0], Arc::new(TestModule::<true>::new(owner)));
    extra.user_contracts().insert(
        application_ids[1],
        Arc::new(TestModule::<false>::new(owner)),
    );
    extra.user_services().insert(
        application_ids[1],
        Arc::new(TestModule::<false>::new(owner)),
    );

    let mut caller_application_state = state.users.try_load_entry_mut(&application_ids[0]).await?;
    let callee_id =
        bcs::to_bytes(&application_ids[1]).expect("Failed to serialize application ID to call");

    let mut batch = Batch::new();
    batch.put_key_value_bytes(CALLEE_ID_KEY.to_vec(), callee_id);
    caller_application_state.write_batch(batch).await?;

    Ok(application_ids)
}

pub async fn create_dummy_user_application_registrations<C>(
    registry: &mut ApplicationRegistryView<C>,
    count: u64,
) -> anyhow::Result<Vec<(UserApplicationId, UserApplicationDescription)>>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    let mut ids = Vec::with_capacity(count as usize);

    for index in 0..count {
        let description = create_dummy_user_application_description(index);
        let id = registry.register_application(description.clone()).await?;

        assert_eq!(registry.describe_application(id).await?, description);

        ids.push((id, description));
    }

    Ok(ids)
}
