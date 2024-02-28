// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use assert_matches::assert_matches;
use linera_base::{
    crypto::PublicKey,
    data_types::{Amount, BlockHeight, Resources, Timestamp},
    identifiers::{Account, ChainDescription, ChainId, Destination, MessageId, Owner},
    ownership::ChainOwnership,
};
use linera_execution::{
    committee::{Committee, Epoch},
    system::SystemMessage,
    test_utils::{
        create_dummy_user_application_registrations, register_mock_applications, ExpectedCall,
        SystemExecutionState,
    },
    ApplicationCallOutcome, BaseRuntime, ContractRuntime, ExecutionError, ExecutionOutcome,
    IntoPriced, MessageKind, Operation, OperationContext, Query, QueryContext, RawExecutionOutcome,
    RawOutgoingMessage, ResourceController, Response, SessionCallOutcome,
};
use linera_views::batch::Batch;
use std::{
    collections::{BTreeMap, BTreeSet},
    vec,
};

fn make_operation_context() -> OperationContext {
    OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: None,
        next_message_index: 0,
    }
}

#[tokio::test]
async fn test_missing_bytecode_for_user_application() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let (app_id, app_desc) =
        &create_dummy_user_application_registrations(&mut view.system.registry, 1).await?[0];

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: *app_id,
                bytes: vec![],
            },
            &mut controller,
        )
        .await;

    assert_matches!(
        result,
        Err(ExecutionError::ApplicationBytecodeNotFound(desc)) if &*desc == app_desc
    );
    Ok(())
}

#[tokio::test]
// TODO(#1484): Split this test into multiple more specialized tests.
async fn test_simple_user_operation() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let mut applications = register_mock_applications(&mut view, 2).await?;
    let (caller_id, caller_application) = applications
        .next()
        .expect("Caller mock application should be registered");
    let (target_id, target_application) = applications
        .next()
        .expect("Target mock application should be registered");

    let owner = Owner::from(PublicKey::test_key(0));
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
        authenticated_signer: Some(owner),
        ..make_operation_context()
    };
    let mut controller = ResourceController::default();
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: dummy_operation.clone(),
            },
            &mut controller,
        )
        .await
        .unwrap();
    let account = Account {
        chain_id: ChainId::root(0),
        owner: Some(owner),
    };
    assert_eq!(
        outcomes,
        vec![
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default()
                    .with_authenticated_signer(Some(owner))
                    .with_refund_grant_to(Some(account)),
            ),
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default()
                    .with_authenticated_signer(Some(owner))
                    .with_refund_grant_to(Some(account))
            )
        ]
    );

    caller_application.expect_call(ExpectedCall::handle_query(|runtime, _context, _query| {
        let state = runtime.read_value_bytes(state_key)?.unwrap_or_default();
        Ok(state)
    }));

    let context = QueryContext {
        chain_id: ChainId::root(0),
        next_block_height: BlockHeight(0),
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
    let mut view = state.into_view().await;

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

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::SessionWasNotClosed(_)));
    Ok(())
}

/// Tests if a session is called correctly during execution.
#[tokio::test]
async fn test_simple_session() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

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

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut controller,
        )
        .await?;
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };
    assert_eq!(
        outcomes,
        vec![
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
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
    let mut view = state.into_view().await;

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

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    assert_matches!(
        view.execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut controller,
        )
        .await,
        Err(ExecutionError::UserError(message)) if message == error_message
    );

    Ok(())
}

/// Tests if an application is scheduled to be registered together with any messages it sends to
/// other chains.
#[tokio::test]
async fn test_simple_message() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let mut applications = register_mock_applications(&mut view, 1).await?;
    let (application_id, application) = applications
        .next()
        .expect("Caller mock application should be registered");

    let destination_chain = ChainId::from(ChainDescription::Root(1));
    let dummy_message = RawOutgoingMessage {
        destination: Destination::from(destination_chain),
        authenticated: false,
        grant: Resources::default(),
        kind: MessageKind::Simple,
        message: b"msg".to_vec(),
    };

    application.expect_call(ExpectedCall::execute_operation({
        let dummy_message = dummy_message.clone();
        move |_runtime, _context, _operation| {
            Ok(RawExecutionOutcome::default().with_raw_message(dummy_message))
        }
    }));

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id,
                bytes: vec![],
            },
            &mut controller,
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
        grant: Amount::ZERO,
        kind: MessageKind::Simple,
        message: SystemMessage::RegisterApplications {
            applications: vec![application_description],
        },
    };
    let dummy_message = dummy_message.into_priced(&Default::default())?;
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };

    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::System(
                RawExecutionOutcome::default().with_raw_message(registration_message)
            ),
            ExecutionOutcome::User(
                application_id,
                RawExecutionOutcome::default()
                    .with_raw_message(dummy_message)
                    .with_refund_grant_to(Some(account))
            )
        ]
    );

    Ok(())
}

/// Tests if a message is scheduled to be sent while an application is handling a cross-application
/// call.
#[tokio::test]
async fn test_message_from_cross_application_call() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

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

    let destination_chain = ChainId::from(ChainDescription::Root(1));
    let dummy_message = RawOutgoingMessage {
        destination: Destination::from(destination_chain),
        authenticated: false,
        grant: Resources::default(),
        kind: MessageKind::Simple,
        message: b"msg".to_vec(),
    };

    target_application.expect_call(ExpectedCall::handle_application_call({
        let dummy_message = dummy_message.clone();
        |_runtime, _context, _argument, _forwarded_sessions| {
            Ok(ApplicationCallOutcome {
                value: vec![],
                execution_outcome: RawExecutionOutcome::default().with_raw_message(dummy_message),
                create_sessions: vec![],
            })
        }
    }));

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut controller,
        )
        .await?;

    let target_description = view.system.registry.describe_application(target_id).await?;
    let registration_message = RawOutgoingMessage {
        destination: Destination::from(destination_chain),
        authenticated: false,
        grant: Amount::ZERO,
        kind: MessageKind::Simple,
        message: SystemMessage::RegisterApplications {
            applications: vec![target_description],
        },
    };
    let dummy_message = dummy_message.into_priced(&Default::default())?;
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };

    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::System(
                RawExecutionOutcome::default().with_raw_message(registration_message)
            ),
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default()
                    .with_raw_message(dummy_message)
                    .with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
        ]
    );

    Ok(())
}

/// Tests if a message is scheduled to be sent while an application is handling a session call.
#[tokio::test]
async fn test_message_from_session_call() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let mut applications = register_mock_applications(&mut view, 3).await?;
    let (caller_id, caller_application) = applications
        .next()
        .expect("Caller mock application should be registered");
    let (middle_id, middle_application) = applications
        .next()
        .expect("Middle mock application should be registered");
    let (target_id, target_application) = applications
        .next()
        .expect("Target mock application should be registered");

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.try_call_application(
                /* authenticated */ false,
                middle_id,
                vec![],
                vec![],
            )?;
            Ok(RawExecutionOutcome::default())
        },
    ));

    middle_application.expect_call(ExpectedCall::handle_application_call(
        move |runtime, _context, _argument, _forwarded_sessions| {
            let outcome = runtime.try_call_application(
                /* authenticated */ false,
                target_id,
                vec![],
                vec![],
            )?;
            assert_eq!(outcome.sessions.len(), 1);
            runtime.try_call_session(
                /* authenticated */ false,
                outcome.sessions[0],
                vec![],
                vec![],
            )?;
            Ok(ApplicationCallOutcome::default())
        },
    ));

    let dummy_session = b"session".to_vec();

    let destination_chain = ChainId::from(ChainDescription::Root(1));
    let dummy_message = RawOutgoingMessage {
        destination: Destination::from(destination_chain),
        authenticated: false,
        grant: Resources::default(),
        kind: MessageKind::Simple,
        message: b"msg".to_vec(),
    };

    target_application.expect_call(ExpectedCall::handle_application_call({
        let dummy_session = dummy_session.clone();
        move |_runtime, _context, _argument, _forwarded_sessions| {
            Ok(ApplicationCallOutcome::default().with_new_session(dummy_session))
        }
    }));
    target_application.expect_call(ExpectedCall::handle_session_call({
        let dummy_message = dummy_message.clone();
        move |_runtime, _context, session_state, _argument, _forwarded_sessions| {
            assert_eq!(session_state, dummy_session);
            Ok((
                SessionCallOutcome {
                    inner: ApplicationCallOutcome::default().with_message(dummy_message),
                    close_session: true,
                },
                session_state,
            ))
        }
    }));

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut controller,
        )
        .await?;

    let target_description = view.system.registry.describe_application(target_id).await?;
    let registration_message = RawOutgoingMessage {
        destination: Destination::from(destination_chain),
        authenticated: false,
        grant: Amount::ZERO,
        kind: MessageKind::Simple,
        message: SystemMessage::RegisterApplications {
            applications: vec![target_description],
        },
    };
    let dummy_message = dummy_message.into_priced(&Default::default())?;
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };
    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::System(
                RawExecutionOutcome::default().with_raw_message(registration_message)
            ),
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default()
                    .with_raw_message(dummy_message)
                    .with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                middle_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
        ]
    );

    Ok(())
}

/// Tests if multiple messages are scheduled to be sent by different applications to different
/// chains.
///
/// Ensures that in a more complex scenario, chains receive application registration messages only
/// for the applications that will receive messages on them.
#[tokio::test]
async fn test_multiple_messages_from_different_applications() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let mut applications = register_mock_applications(&mut view, 3).await?;
    // The entrypoint application, which sends a message and calls other applications
    let (caller_id, caller_application) = applications
        .next()
        .expect("Caller mock application should be registered");
    // An application that does not send any messages
    let (silent_target_id, silent_target_application) = applications
        .next()
        .expect("Target mock application that doesn't send messages should be registered");
    // An application that sends a message when handling a cross-application call
    let (sending_target_id, sending_target_application) = applications
        .next()
        .expect("Target mock application that sends a message should be registered");

    // The first destination chain receives messages from the caller and the sending applications
    let first_destination_chain = ChainId::from(ChainDescription::Root(1));
    // The second destination chain only receives a message from the sending application
    let second_destination_chain = ChainId::from(ChainDescription::Root(2));

    // The message sent to the first destination chain by the caller and the sending applications
    let first_message = RawOutgoingMessage {
        destination: Destination::from(first_destination_chain),
        authenticated: false,
        grant: Resources::default(),
        kind: MessageKind::Simple,
        message: b"first".to_vec(),
    };

    // The entrypoint sends a message to the first chain and calls the silent and the sending
    // applications
    caller_application.expect_call(ExpectedCall::execute_operation({
        let first_message = first_message.clone();
        move |runtime, _context, _operation| {
            runtime.try_call_application(
                /* authenticated */ false,
                silent_target_id,
                vec![],
                vec![],
            )?;
            runtime.try_call_application(
                /* authenticated */ false,
                sending_target_id,
                vec![],
                vec![],
            )?;
            Ok(RawExecutionOutcome::default().with_raw_message(first_message))
        }
    }));

    // The silent application does nothing
    silent_target_application.expect_call(ExpectedCall::handle_application_call(
        |_runtime, _context, _argument, _forwarded_sessions| Ok(ApplicationCallOutcome::default()),
    ));

    // The message sent to the second destination chain by the sending application
    let second_message = RawOutgoingMessage {
        destination: Destination::from(second_destination_chain),
        authenticated: false,
        grant: Resources::default(),
        kind: MessageKind::Simple,
        message: b"second".to_vec(),
    };

    // The sending application sends two messages, one to each of the destination chains
    sending_target_application.expect_call(ExpectedCall::handle_application_call({
        let first_message = first_message.clone();
        let second_message = second_message.clone();
        |_runtime, _context, _argument, _forwarded_sessions| {
            Ok(ApplicationCallOutcome {
                value: vec![],
                execution_outcome: RawExecutionOutcome::default()
                    .with_raw_message(first_message)
                    .with_raw_message(second_message),
                create_sessions: vec![],
            })
        }
    }));

    // Execute the operation, starting the test scenario
    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let mut outcomes = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut controller,
        )
        .await?;

    // Describe the two applications that sent messages, and will therefore handle them in the
    // other chains
    let caller_description = view.system.registry.describe_application(caller_id).await?;
    let sending_target_description = view
        .system
        .registry
        .describe_application(sending_target_id)
        .await?;

    // The registration message for the first destination chain
    let first_registration_message = RawOutgoingMessage {
        destination: Destination::from(first_destination_chain),
        authenticated: false,
        grant: Amount::ZERO,
        kind: MessageKind::Simple,
        message: SystemMessage::RegisterApplications {
            applications: vec![sending_target_description.clone(), caller_description],
        },
    };
    // The registration message for the second destination chain
    let second_registration_message = RawOutgoingMessage {
        destination: Destination::from(second_destination_chain),
        authenticated: false,
        grant: Amount::ZERO,
        kind: MessageKind::Simple,
        message: SystemMessage::RegisterApplications {
            applications: vec![sending_target_description],
        },
    };

    // Need to deconstruct the outcome and verify each field individually because the messages are
    // built from a `HashMap`, which may produce a different order every run
    let ExecutionOutcome::System(system_outcome) = outcomes.remove(0) else {
        panic!(
            "First execution outcome is not the system outcome with messages to \
            register applications"
        );
    };
    assert!(system_outcome.authenticated_signer.is_none());
    assert!(system_outcome.subscribe.is_empty());
    assert!(system_outcome.unsubscribe.is_empty());
    // Check for the two registration messages
    assert_eq!(system_outcome.messages.len(), 2);
    assert!(system_outcome
        .messages
        .contains(&first_registration_message));
    assert!(system_outcome
        .messages
        .contains(&second_registration_message));
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };

    let first_message = first_message.into_priced(&Default::default())?;
    let second_message = second_message.into_priced(&Default::default())?;
    // Return to checking the user application outcomes
    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::User(
                silent_target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account)),
            ),
            ExecutionOutcome::User(
                sending_target_id,
                RawExecutionOutcome::default()
                    .with_raw_message(first_message.clone())
                    .with_raw_message(second_message)
                    .with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default()
                    .with_raw_message(first_message)
                    .with_refund_grant_to(Some(account))
            ),
        ]
    );

    Ok(())
}

/// Tests the system API calls `open_chain` and `chain_ownership`.
#[tokio::test]
async fn test_open_chain() {
    let committee = Committee::make_simple(vec![PublicKey::test_key(0).into()]);
    let committees = BTreeMap::from([(Epoch::ZERO, committee)]);
    let ownership = ChainOwnership::single(PublicKey::test_key(1));
    let child_ownership = ChainOwnership::single(PublicKey::test_key(2));
    let state = SystemExecutionState {
        committees: committees.clone(),
        ownership: ownership.clone(),
        balance: Amount::from_tokens(5),
        ..SystemExecutionState::new(Epoch::ZERO, ChainDescription::Root(0), ChainId::root(0))
    };
    let mut view = state.into_view().await;
    let mut applications = register_mock_applications(&mut view, 1).await.unwrap();
    let (application_id, application) = applications.next().unwrap();

    let context = OperationContext {
        height: BlockHeight(1),
        next_message_index: 5,
        ..make_operation_context()
    };
    let message_id = MessageId {
        chain_id: context.chain_id,
        height: context.height,
        index: context.next_message_index,
    };

    application.expect_call(ExpectedCall::execute_operation({
        let child_ownership = child_ownership.clone();
        move |runtime, _context, _operation| {
            assert_eq!(runtime.chain_ownership().unwrap(), ownership);
            let chain_id = runtime.open_chain(child_ownership, Amount::ONE).unwrap();
            assert_eq!(chain_id, ChainId::child(message_id));
            Ok(RawExecutionOutcome::default())
        }
    }));
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let outcomes = view
        .execute_operation(context, operation, &mut controller)
        .await
        .unwrap();

    assert_eq!(*view.system.balance.get(), Amount::from_tokens(4));
    let ExecutionOutcome::System(outcome) = &outcomes[0] else {
        panic!("Unexpected outcomes: {:?}", outcomes);
    };
    let RawOutgoingMessage {
        message: SystemMessage::OpenChain(config),
        destination: Destination::Recipient(recipient_id),
        ..
    } = &outcome.messages[0]
    else {
        panic!("Unexpected first message: {:?}", outcome.messages[0]);
    };
    assert_eq!(*recipient_id, ChainId::child(message_id));
    assert_eq!(config.balance, Amount::ONE);
    assert_eq!(config.ownership, child_ownership);
    assert_eq!(config.committees, committees);

    // Initialize the child chain using the config from the message.
    let mut child_view = SystemExecutionState::default()
        .into_view_with(ChainId::child(message_id), Default::default())
        .await;
    child_view
        .system
        .initialize_chain(message_id, Timestamp::from(0), config.clone());
    assert_eq!(*child_view.system.balance.get(), Amount::ONE);
    assert_eq!(*child_view.system.ownership.get(), child_ownership);
    assert_eq!(*child_view.system.committees.get(), committees);
    assert_eq!(
        *child_view.system.authorized_applications.get(),
        Some(BTreeSet::from([application_id]))
    );
}
