// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

mod utils;

use self::utils::{create_dummy_user_application_description, ExpectedCall, MockApplication};
use linera_base::{
    crypto::PublicKey,
    data_types::BlockHeight,
    identifiers::{ChainDescription, ChainId, Destination, Owner},
};
use linera_execution::{
    policy::ResourceControlPolicy, system::SystemMessage, ApplicationCallOutcome,
    ApplicationRegistryView, BaseRuntime, ContractRuntime, ExecutionError, ExecutionOutcome,
    ExecutionRuntimeConfig, ExecutionRuntimeContext, ExecutionStateView, MessageKind, Operation,
    OperationContext, Query, QueryContext, RawExecutionOutcome, RawOutgoingMessage,
    ResourceTracker, Response, SessionCallOutcome, SystemExecutionState,
    TestExecutionRuntimeContext, UserApplicationDescription, UserApplicationId,
};
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

#[tokio::test]
// TODO(#1484): Split this test into multiple more specialized tests.
async fn test_simple_user_operation() -> anyhow::Result<()> {
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
        .expect("Caller mock application should be registered");
    let (target_id, target_application) = applications
        .next()
        .expect("Target mock application should be registered");

    let owner = Owner::from(PublicKey::debug(0));
    let state_key = vec![];
    let dummy_operation = vec![1];

    caller_application.expect_call({
        let state_key = state_key.clone();
        let dummy_operation = dummy_operation.clone();
        ExpectedCall::execute_operation(move |runtime, _context, operation| {
            assert_eq!(operation, dummy_operation);
            // Modify our state.
            let mut state = runtime
                .read_value_bytes(state_key.clone())?
                .unwrap_or_default();
            state.extend(operation.clone());
            let mut batch = Batch::new();
            batch.put_key_value_bytes(state_key, state);
            runtime.write_batch(batch)?;

            // Call the target application to create a session
            let call_outcome = runtime.try_call_application(
                /* authenticated */ true,
                target_id,
                vec![],
                vec![],
            )?;
            assert!(call_outcome.value.is_empty());
            assert_eq!(call_outcome.sessions.len(), 1);

            // Call the session to close it.
            let session_id = call_outcome.sessions[0];
            runtime.try_call_session(/* authenticated */ false, session_id, vec![], vec![])?;

            Ok(RawExecutionOutcome::default())
        })
    });

    let dummy_session_state = vec![1];

    target_application.expect_call({
        let dummy_session_state = dummy_session_state.clone();
        ExpectedCall::handle_application_call(
            move |_runtime, context, argument, forwarded_sessions| {
                assert_eq!(context.authenticated_signer, Some(owner));
                assert!(argument.is_empty());
                assert!(forwarded_sessions.is_empty());
                Ok(ApplicationCallOutcome {
                    create_sessions: vec![dummy_session_state],
                    ..ApplicationCallOutcome::default()
                })
            },
        )
    });
    target_application.expect_call(ExpectedCall::handle_session_call(
        move |_runtime, context, session_state, argument, forwarded_sessions| {
            assert_eq!(context.authenticated_signer, None);
            assert_eq!(session_state, dummy_session_state);
            assert!(argument.is_empty());
            assert!(forwarded_sessions.is_empty());
            Ok((
                SessionCallOutcome {
                    inner: ApplicationCallOutcome::default(),
                    close_session: true,
                },
                session_state,
            ))
        },
    ));

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
                application_id: caller_id,
                bytes: dummy_operation.clone(),
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
                target_id,
                RawExecutionOutcome::default().with_authenticated_signer(Some(owner))
            ),
            ExecutionOutcome::User(target_id, RawExecutionOutcome::default()),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default().with_authenticated_signer(Some(owner))
            )
        ]
    );

    caller_application.expect_call(ExpectedCall::handle_query(|runtime, _context, _query| {
        let state = runtime.read_value_bytes(state_key)?.unwrap_or_default();
        Ok(state)
    }));

    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    assert_eq!(
        view.query_application(
            context,
            Query::User {
                application_id: caller_id,
                bytes: vec![]
            }
        )
        .await
        .unwrap(),
        Response::User(dummy_operation)
    );
    Ok(())
}

/// Tests if execution fails if a session is created and leaked by its owner.
#[tokio::test]
async fn test_leaking_session() -> anyhow::Result<()> {
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
        .expect("Caller mock application should be registered");
    let (target_id, target_application) = applications
        .next()
        .expect("Target mock application should be registered");

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

/// Tests if a session is called correctly during execution.
#[tokio::test]
async fn test_simple_session() -> anyhow::Result<()> {
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
        .expect("Caller mock application should be registered");
    let (target_id, target_application) = applications
        .next()
        .expect("Target mock application should be registered");

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            let outcome = runtime.try_call_application(false, target_id, vec![], vec![])?;
            runtime.try_call_session(false, outcome.sessions[0], vec![], vec![])?;
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

    target_application.expect_call(ExpectedCall::handle_session_call(
        |_runtime, _context, _session_state, _argument, _forwarded_sessions| {
            Ok((
                SessionCallOutcome {
                    close_session: true,
                    ..SessionCallOutcome::default()
                },
                vec![],
            ))
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
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &policy,
            &mut tracker,
        )
        .await?;

    assert_eq!(
        outcomes,
        vec![
            ExecutionOutcome::User(target_id, RawExecutionOutcome::default()),
            ExecutionOutcome::User(target_id, RawExecutionOutcome::default()),
            ExecutionOutcome::User(caller_id, RawExecutionOutcome::default()),
        ]
    );
    Ok(())
}

/// Tests if user application errors when handling cross-application calls are handled correctly.
///
/// Errors in [`UserContract::handle_application_call`] should be handled correctly without
/// panicking.
#[tokio::test]
async fn test_cross_application_error() -> anyhow::Result<()> {
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
        .expect("Caller mock application should be registered");
    let (target_id, target_application) = applications
        .next()
        .expect("Target mock application should be registered");

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.try_call_application(
                /* authenticated */ false,
                target_id,
                vec![],
                vec![],
            )?;
            Ok(RawExecutionOutcome::default())
        },
    ));

    let error_message = "Cross-application call failed";

    target_application.expect_call(ExpectedCall::handle_application_call(
        |_runtime, _context, _argument, _forwarded_sessions| {
            Err(ExecutionError::UserError(error_message.to_owned()))
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
    assert!(matches!(
        view.execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &policy,
            &mut tracker,
        )
        .await,
        Err(ExecutionError::UserError(message)) if message == error_message
    ));

    Ok(())
}

/// Tests if an application is scheduled to be registered together with any messages it sends to
/// other chains.
#[tokio::test]
async fn test_simple_message() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(
            state,
            ExecutionRuntimeConfig::Synchronous,
        )
        .await;

    let mut applications = register_mock_applications(&mut view, 1).await?;
    let (application_id, application) = applications
        .next()
        .expect("Caller mock application should be registered");

    let destination_chain = ChainId::from(ChainDescription::Root(1));
    let dummy_message = RawOutgoingMessage {
        destination: Destination::from(destination_chain),
        authenticated: false,
        kind: MessageKind::Simple,
        message: b"msg".to_vec(),
    };

    application.expect_call(ExpectedCall::execute_operation({
        let dummy_message = dummy_message.clone();
        move |_runtime, _context, _operation| {
            Ok(RawExecutionOutcome::default().with_message(dummy_message))
        }
    }));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: None,
        next_message_index: 0,
    };
    let mut tracker = ResourceTracker::default();
    let policy = ResourceControlPolicy::default();
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id,
                bytes: vec![],
            },
            &policy,
            &mut tracker,
        )
        .await?;

    let application_description = view
        .system
        .registry
        .describe_application(application_id)
        .await?;
    let registration_message = RawOutgoingMessage {
        destination: Destination::from(destination_chain),
        authenticated: false,
        kind: MessageKind::Simple,
        message: SystemMessage::RegisterApplications {
            applications: vec![application_description],
        },
    };

    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::System(
                RawExecutionOutcome::default().with_message(registration_message)
            ),
            ExecutionOutcome::User(
                application_id,
                RawExecutionOutcome::default().with_message(dummy_message)
            )
        ]
    );

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
