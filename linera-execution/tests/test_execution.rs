// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use std::{collections::BTreeMap, vec};

use assert_matches::assert_matches;
use futures::{stream, StreamExt, TryStreamExt};
use linera_base::{
    crypto::PublicKey,
    data_types::{
        Amount, ApplicationPermissions, BlockHeight, Resources, SendMessageRequest, Timestamp,
    },
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
    BaseRuntime, ContractRuntime, ExecutionError, ExecutionOutcome, MessageKind, Operation,
    OperationContext, Query, QueryContext, RawExecutionOutcome, RawOutgoingMessage,
    ResourceControlPolicy, ResourceController, Response, SystemOperation,
};
use linera_views::batch::Batch;

fn make_operation_context() -> OperationContext {
    OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: Some(0),
        authenticated_signer: None,
        authenticated_caller_id: None,
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
            Timestamp::from(0),
            Operation::User {
                application_id: *app_id,
                bytes: vec![],
            },
            Some(Vec::new()),
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
            let response = runtime.try_call_application(
                /* authenticated */ true,
                target_id,
                vec![SessionCall::StartSession as u8],
            )?;
            assert!(response.is_empty());

            // Call the target application to end the session
            let response = runtime.try_call_application(
                /* authenticated */ false,
                target_id,
                vec![SessionCall::EndSession as u8],
            )?;
            assert!(response.is_empty());

            Ok(vec![])
        })
    });

    target_application.expect_call(ExpectedCall::execute_operation(
        move |_runtime, context, argument| {
            assert_eq!(context.authenticated_signer, Some(owner));
            assert_eq!(&argument, &[SessionCall::StartSession as u8]);
            Ok(vec![])
        },
    ));
    target_application.expect_call(ExpectedCall::execute_operation(
        move |_runtime, context, argument| {
            assert_eq!(context.authenticated_signer, None);
            assert_eq!(&argument, &[SessionCall::EndSession as u8]);
            Ok(vec![])
        },
    ));

    target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: Some(owner),
        ..make_operation_context()
    };
    let mut controller = ResourceController::default();
    let (outcomes, _) = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: dummy_operation.clone(),
            },
            Some(Vec::new()),
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
            ),
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default()
                    .with_refund_grant_to(Some(account))
                    .with_authenticated_signer(Some(owner))
            ),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default()
                    .with_authenticated_signer(Some(owner))
                    .with_refund_grant_to(Some(account))
            ),
        ]
    );

    caller_application.expect_call(ExpectedCall::handle_query(|runtime, _context, _query| {
        let state = runtime.read_value_bytes(state_key)?.unwrap_or_default();
        Ok(state)
    }));

    let context = QueryContext {
        chain_id: ChainId::root(0),
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };
    let (execution_request_receiver, runtime_request_sender) =
        context.spawn_service_runtime_actor();
    assert_eq!(
        view.query_application(
            context,
            Query::User {
                application_id: caller_id,
                bytes: vec![]
            },
            execution_request_receiver,
            runtime_request_sender,
        )
        .await
        .unwrap(),
        Response::User(dummy_operation)
    );
    Ok(())
}

/// A cross-application call to start or end a session.
///
/// Here a session is a test scenario where the transaction is prevented from succeeding while
/// there in an open session.
#[repr(u8)]
enum SessionCall {
    StartSession,
    EndSession,
}

/// Tests a simulated session.
#[tokio::test]
async fn test_simulated_session() -> anyhow::Result<()> {
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
                false,
                target_id,
                vec![SessionCall::StartSession as u8],
            )?;
            runtime.try_call_application(false, target_id, vec![SessionCall::EndSession as u8])?;
            Ok(vec![])
        },
    ));

    let state_key = vec![];

    target_application.expect_call(ExpectedCall::execute_operation({
        let state_key = state_key.clone();
        move |runtime, _context, argument| {
            assert_eq!(&argument, &[SessionCall::StartSession as u8]);

            let mut batch = Batch::new();
            batch.put_key_value_bytes(state_key, vec![true as u8]);
            runtime.write_batch(batch)?;

            Ok(vec![])
        }
    }));

    target_application.expect_call(ExpectedCall::execute_operation({
        let state_key = state_key.clone();
        move |runtime, _context, argument| {
            assert_eq!(&argument, &[SessionCall::EndSession as u8]);

            let mut batch = Batch::new();
            batch.put_key_value_bytes(state_key, vec![false as u8]);
            runtime.write_batch(batch)?;

            Ok(vec![])
        }
    }));

    target_application.expect_call(ExpectedCall::finalize(|runtime, _context| {
        match runtime.read_value_bytes(state_key)? {
            Some(session_is_open) if session_is_open == vec![u8::from(false)] => Ok(()),
            Some(_) => Err(ExecutionError::UserError("Leaked session".to_owned())),
            _ => Err(ExecutionError::UserError(
                "Missing or invalid session state".to_owned(),
            )),
        }
    }));
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let (outcomes, _) = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
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

/// Tests if execution fails if a simulated session isn't properly closed.
#[tokio::test]
async fn test_simulated_session_leak() -> anyhow::Result<()> {
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
                false,
                target_id,
                vec![SessionCall::StartSession as u8],
            )?;
            Ok(vec![])
        },
    ));

    let state_key = vec![];

    target_application.expect_call(ExpectedCall::execute_operation({
        let state_key = state_key.clone();
        |runtime, _context, argument| {
            assert_eq!(argument, &[SessionCall::StartSession as u8]);

            let mut batch = Batch::new();
            batch.put_key_value_bytes(state_key, vec![true as u8]);
            runtime.write_batch(batch)?;

            Ok(Vec::new())
        }
    }));

    let error_message = "Session leaked";

    target_application.expect_call(ExpectedCall::finalize(|runtime, _context| {
        match runtime.read_value_bytes(state_key)? {
            Some(session_is_open) if session_is_open == vec![u8::from(false)] => Ok(()),
            Some(_) => Err(ExecutionError::UserError(error_message.to_owned())),
            _ => Err(ExecutionError::UserError(
                "Missing or invalid session state".to_owned(),
            )),
        }
    }));

    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::UserError(message)) if message == error_message);
    Ok(())
}

/// Tests if `finalize` can cause execution to fail.
#[tokio::test]
async fn test_rejecting_block_from_finalize() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let mut applications = register_mock_applications(&mut view, 1).await?;
    let (id, application) = applications
        .next()
        .expect("Mock application should be registered");

    application.expect_call(ExpectedCall::execute_operation(
        move |_runtime, _context, _operation| Ok(vec![]),
    ));

    let error_message = "Finalize aborted execution";

    application.expect_call(ExpectedCall::finalize(|_runtime, _context| {
        Err(ExecutionError::UserError(error_message.to_owned()))
    }));

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: id,
                bytes: vec![],
            },
            Some(Vec::new()),
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::UserError(message)) if message == error_message);
    Ok(())
}

/// Tests if `finalize` from a called application can cause execution to fail.
#[tokio::test]
async fn test_rejecting_block_from_called_applications_finalize() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let mut applications = register_mock_applications(&mut view, 4).await?;
    let (first_id, first_application) = applications
        .next()
        .expect("First mock application should be registered");
    let (second_id, second_application) = applications
        .next()
        .expect("Second mock application should be registered");
    let (third_id, third_application) = applications
        .next()
        .expect("Third mock application should be registered");
    let (fourth_id, fourth_application) = applications
        .next()
        .expect("Fourth mock application should be registered");

    first_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.try_call_application(false, second_id, vec![])?;
            Ok(vec![])
        },
    ));
    second_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _argument| {
            runtime.try_call_application(false, third_id, vec![])?;
            Ok(vec![])
        },
    ));
    third_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _argument| {
            runtime.try_call_application(false, fourth_id, vec![])?;
            Ok(vec![])
        },
    ));
    fourth_application.expect_call(ExpectedCall::execute_operation(
        |_runtime, _context, _argument| Ok(vec![]),
    ));

    let error_message = "Third application aborted execution";

    fourth_application.expect_call(ExpectedCall::default_finalize());
    third_application.expect_call(ExpectedCall::finalize(|_runtime, _context| {
        Err(ExecutionError::UserError(error_message.to_owned()))
    }));
    second_application.expect_call(ExpectedCall::default_finalize());
    first_application.expect_call(ExpectedCall::default_finalize());

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: first_id,
                bytes: vec![],
            },
            Some(Vec::new()),
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::UserError(message)) if message == error_message);
    Ok(())
}

/// Tests if `finalize` can send messages.
#[tokio::test]
async fn test_sending_message_from_finalize() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let mut applications = register_mock_applications(&mut view, 4).await?;
    let (first_id, first_application) = applications
        .next()
        .expect("First mock application should be registered");
    let (second_id, second_application) = applications
        .next()
        .expect("Second mock application should be registered");
    let (third_id, third_application) = applications
        .next()
        .expect("Third mock application should be registered");
    let (fourth_id, fourth_application) = applications
        .next()
        .expect("Fourth mock application should be registered");

    let destination_chain = ChainId::from(ChainDescription::Root(1));
    let first_message = SendMessageRequest {
        destination: Destination::from(destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"first".to_vec(),
    };

    let fee_policy = ResourceControlPolicy::default();
    let expected_first_message =
        RawOutgoingMessage::from(first_message.clone()).into_priced(&fee_policy)?;

    first_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.try_call_application(false, second_id, vec![])?;
            Ok(vec![])
        },
    ));
    second_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _argument| {
            runtime.try_call_application(false, third_id, vec![])?;
            Ok(vec![])
        },
    ));
    third_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _argument| {
            runtime.send_message(first_message)?;
            runtime.try_call_application(false, fourth_id, vec![])?;
            Ok(vec![])
        },
    ));
    fourth_application.expect_call(ExpectedCall::execute_operation(
        |_runtime, _context, _argument| Ok(vec![]),
    ));

    let second_message = SendMessageRequest {
        destination: Destination::from(destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"second".to_vec(),
    };
    let third_message = SendMessageRequest {
        destination: Destination::from(destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"third".to_vec(),
    };
    let fourth_message = SendMessageRequest {
        destination: Destination::from(destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"fourth".to_vec(),
    };

    let expected_second_message =
        RawOutgoingMessage::from(second_message.clone()).into_priced(&fee_policy)?;
    let expected_third_message =
        RawOutgoingMessage::from(third_message.clone()).into_priced(&fee_policy)?;
    let expected_fourth_message =
        RawOutgoingMessage::from(fourth_message.clone()).into_priced(&fee_policy)?;

    fourth_application.expect_call(ExpectedCall::default_finalize());
    third_application.expect_call(ExpectedCall::finalize(|runtime, _context| {
        runtime.send_message(second_message)?;
        runtime.send_message(third_message)?;
        Ok(())
    }));
    second_application.expect_call(ExpectedCall::default_finalize());
    first_application.expect_call(ExpectedCall::finalize(|runtime, _context| {
        runtime.send_message(fourth_message)?;
        Ok(())
    }));

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let (outcomes, _) = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: first_id,
                bytes: vec![],
            },
            Some(Vec::new()),
            &mut controller,
        )
        .await?;

    let applications = stream::iter([third_id, first_id])
        .then(|id| view.system.registry.describe_application(id))
        .try_collect()
        .await?;
    let registration_message = RawOutgoingMessage {
        destination: Destination::from(destination_chain),
        authenticated: false,
        grant: Amount::ZERO,
        kind: MessageKind::Simple,
        message: SystemMessage::RegisterApplications { applications },
    };
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };

    assert_eq!(
        outcomes,
        vec![
            ExecutionOutcome::System(
                RawExecutionOutcome::default().with_message(registration_message)
            ),
            ExecutionOutcome::User(
                fourth_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                third_id,
                RawExecutionOutcome::default()
                    .with_refund_grant_to(Some(account))
                    .with_message(expected_first_message)
            ),
            ExecutionOutcome::User(
                second_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                first_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                fourth_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                third_id,
                RawExecutionOutcome::default()
                    .with_refund_grant_to(Some(account))
                    .with_message(expected_second_message)
                    .with_message(expected_third_message)
            ),
            ExecutionOutcome::User(
                second_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                first_id,
                RawExecutionOutcome::default()
                    .with_refund_grant_to(Some(account))
                    .with_message(expected_fourth_message)
            ),
        ]
    );
    Ok(())
}

/// Tests if an application can't perform cross-application calls during `finalize`.
#[tokio::test]
async fn test_cross_application_call_from_finalize() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;

    let mut applications = register_mock_applications(&mut view, 2).await?;
    let (caller_id, caller_application) = applications
        .next()
        .expect("Caller mock application should be registered");
    let (target_id, _target_application) = applications
        .next()
        .expect("Target mock application should be registered");

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |_runtime, _context, _operation| Ok(vec![]),
    ));

    caller_application.expect_call(ExpectedCall::finalize({
        move |runtime, _context| {
            runtime.try_call_application(false, target_id, vec![])?;
            Ok(())
        }
    }));

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
            &mut controller,
        )
        .await;

    let expected_caller_id = caller_id;
    let expected_callee_id = target_id;
    assert_matches!(
        result,
        Err(ExecutionError::CrossApplicationCallInFinalize { caller_id, callee_id })
            if *caller_id == expected_caller_id && *callee_id == expected_callee_id
    );

    Ok(())
}

/// Tests if an application can't perform cross-application calls during `finalize`, even if they
/// have already called the same application.
#[tokio::test]
async fn test_cross_application_call_from_finalize_of_called_application() -> anyhow::Result<()> {
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
            runtime.try_call_application(false, target_id, vec![])?;
            Ok(vec![])
        },
    ));
    target_application.expect_call(ExpectedCall::execute_operation(
        |_runtime, _context, _argument| Ok(vec![]),
    ));

    target_application.expect_call(ExpectedCall::finalize({
        move |runtime, _context| {
            runtime.try_call_application(false, caller_id, vec![])?;
            Ok(())
        }
    }));
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
            &mut controller,
        )
        .await;

    let expected_caller_id = target_id;
    let expected_callee_id = caller_id;
    assert_matches!(
        result,
        Err(ExecutionError::CrossApplicationCallInFinalize { caller_id, callee_id })
            if *caller_id == expected_caller_id && *callee_id == expected_callee_id
    );

    Ok(())
}

/// Tests if a called application can't perform cross-application calls during `finalize`.
#[tokio::test]
async fn test_calling_application_again_from_finalize() -> anyhow::Result<()> {
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
            runtime.try_call_application(false, target_id, vec![])?;
            Ok(vec![])
        },
    ));
    target_application.expect_call(ExpectedCall::execute_operation(
        |_runtime, _context, _argument| Ok(vec![]),
    ));

    target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::finalize({
        move |runtime, _context| {
            runtime.try_call_application(false, target_id, vec![])?;
            Ok(())
        }
    }));

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
            &mut controller,
        )
        .await;

    let expected_caller_id = caller_id;
    let expected_callee_id = target_id;
    assert_matches!(
        result,
        Err(ExecutionError::CrossApplicationCallInFinalize { caller_id, callee_id })
            if *caller_id == expected_caller_id && *callee_id == expected_callee_id
    );

    Ok(())
}

/// Tests if user application errors when handling cross-application calls are handled correctly.
///
/// Errors in secondary [`UserContract::execute_operation`] executions should be handled correctly
/// without panicking.
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
            runtime.try_call_application(/* authenticated */ false, target_id, vec![])?;
            Ok(vec![])
        },
    ));

    let error_message = "Cross-application call failed";

    target_application.expect_call(ExpectedCall::execute_operation(
        |_runtime, _context, _argument| Err(ExecutionError::UserError(error_message.to_owned())),
    ));

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    assert_matches!(
        view.execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
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
    let dummy_message = SendMessageRequest {
        destination: Destination::from(destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"msg".to_vec(),
    };

    let fee_policy = ResourceControlPolicy::default();
    let expected_dummy_message =
        RawOutgoingMessage::from(dummy_message.clone()).into_priced(&fee_policy)?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.send_message(dummy_message)?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let (outcomes, _) = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id,
                bytes: vec![],
            },
            Some(Vec::new()),
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
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };

    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::System(
                RawExecutionOutcome::default().with_message(registration_message)
            ),
            ExecutionOutcome::User(
                application_id,
                RawExecutionOutcome::default()
                    .with_message(expected_dummy_message)
                    .with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                application_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
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
            runtime.try_call_application(/* authenticated */ false, target_id, vec![])?;
            Ok(vec![])
        },
    ));

    let destination_chain = ChainId::from(ChainDescription::Root(1));
    let dummy_message = SendMessageRequest {
        destination: Destination::from(destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"msg".to_vec(),
    };

    let fee_policy = ResourceControlPolicy::default();
    let expected_dummy_message =
        RawOutgoingMessage::from(dummy_message.clone()).into_priced(&fee_policy)?;

    target_application.expect_call(ExpectedCall::execute_operation(
        |runtime, _context, _argument| {
            runtime.send_message(dummy_message)?;
            Ok(vec![])
        },
    ));

    target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let (outcomes, _) = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
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
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };

    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::System(
                RawExecutionOutcome::default().with_message(registration_message)
            ),
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default()
                    .with_message(expected_dummy_message)
                    .with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                caller_id,
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

/// Tests if a message is scheduled to be sent by a deeper cross-application call.
#[tokio::test]
async fn test_message_from_deeper_call() -> anyhow::Result<()> {
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
            runtime.try_call_application(/* authenticated */ false, middle_id, vec![])?;
            Ok(vec![])
        },
    ));

    middle_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _argument| {
            runtime.try_call_application(/* authenticated */ false, target_id, vec![])?;
            Ok(vec![])
        },
    ));

    let destination_chain = ChainId::from(ChainDescription::Root(1));
    let dummy_message = SendMessageRequest {
        destination: Destination::from(destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"msg".to_vec(),
    };

    let fee_policy = ResourceControlPolicy::default();
    let expected_dummy_message =
        RawOutgoingMessage::from(dummy_message.clone()).into_priced(&fee_policy)?;

    target_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _argument| {
            runtime.send_message(dummy_message)?;
            Ok(vec![])
        },
    ));

    target_application.expect_call(ExpectedCall::default_finalize());
    middle_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let (outcomes, _) = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
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
    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };
    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::System(
                RawExecutionOutcome::default().with_message(registration_message)
            ),
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default()
                    .with_message(expected_dummy_message)
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
            ExecutionOutcome::User(
                target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
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
    let first_message = SendMessageRequest {
        destination: Destination::from(first_destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"first".to_vec(),
    };

    let fee_policy = ResourceControlPolicy::default();
    let expected_first_message =
        RawOutgoingMessage::from(first_message.clone()).into_priced(&fee_policy)?;

    // The entrypoint sends a message to the first chain and calls the silent and the sending
    // applications
    caller_application.expect_call(ExpectedCall::execute_operation({
        let first_message = first_message.clone();
        move |runtime, _context, _operation| {
            runtime.try_call_application(
                /* authenticated */ false,
                silent_target_id,
                vec![],
            )?;
            runtime.send_message(first_message)?;
            runtime.try_call_application(
                /* authenticated */ false,
                sending_target_id,
                vec![],
            )?;
            Ok(vec![])
        }
    }));

    // The silent application does nothing
    silent_target_application.expect_call(ExpectedCall::execute_operation(
        |_runtime, _context, _argument| Ok(vec![]),
    ));

    // The message sent to the second destination chain by the sending application
    let second_message = SendMessageRequest {
        destination: Destination::from(second_destination_chain),
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"second".to_vec(),
    };

    let expected_second_message =
        RawOutgoingMessage::from(second_message.clone()).into_priced(&fee_policy)?;

    // The sending application sends two messages, one to each of the destination chains
    sending_target_application.expect_call(ExpectedCall::execute_operation(
        |runtime, _context, _argument| {
            runtime.send_message(first_message)?;
            runtime.send_message(second_message)?;
            Ok(vec![])
        },
    ));

    sending_target_application.expect_call(ExpectedCall::default_finalize());
    silent_target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    // Execute the operation, starting the test scenario
    let context = make_operation_context();
    let mut controller = ResourceController::default();
    let (outcomes, _) = view
        .execute_operation(
            context,
            Timestamp::from(0),
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            Some(Vec::new()),
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

    let account = Account {
        chain_id: ChainId::root(0),
        owner: None,
    };

    // Return to checking the user application outcomes
    assert_eq!(
        outcomes,
        &[
            ExecutionOutcome::System(
                RawExecutionOutcome::default()
                    .with_message(first_registration_message)
                    .with_message(second_registration_message)
            ),
            ExecutionOutcome::User(
                silent_target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account)),
            ),
            ExecutionOutcome::User(
                sending_target_id,
                RawExecutionOutcome::default()
                    .with_message(expected_first_message.clone())
                    .with_message(expected_second_message)
                    .with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default()
                    .with_message(expected_first_message)
                    .with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                sending_target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            ),
            ExecutionOutcome::User(
                silent_target_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account)),
            ),
            ExecutionOutcome::User(
                caller_id,
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
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
    // We will send one additional message before calling open_chain.
    let index = context.next_message_index + 1;
    let message_id = MessageId {
        chain_id: context.chain_id,
        height: context.height,
        index,
    };

    application.expect_call(ExpectedCall::execute_operation({
        let child_ownership = child_ownership.clone();
        move |runtime, _context, _operation| {
            assert_eq!(runtime.chain_ownership().unwrap(), ownership);
            let destination = Account::chain(ChainId::root(2));
            runtime.transfer(None, destination, Amount::ONE).unwrap();
            let id = runtime.application_id().unwrap();
            let application_permissions = ApplicationPermissions::new_single(id);
            let chain_id = runtime
                .open_chain(child_ownership, application_permissions, Amount::ONE)
                .unwrap();
            assert_eq!(chain_id, ChainId::child(message_id));
            Ok(vec![])
        }
    }));
    application.expect_call(ExpectedCall::default_finalize());

    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let (outcomes, _) = view
        .execute_operation(
            context,
            Timestamp::from(0),
            operation,
            Some(Vec::new()),
            &mut controller,
        )
        .await
        .unwrap();

    assert_eq!(*view.system.balance.get(), Amount::from_tokens(3));
    let message = outcomes
        .iter()
        .flat_map(|outcome| match outcome {
            ExecutionOutcome::System(outcome) => &outcome.messages,
            ExecutionOutcome::User(_, _) => panic!("Unexpected message"),
        })
        .nth((index - context.next_message_index) as usize)
        .unwrap();
    let RawOutgoingMessage {
        message: SystemMessage::OpenChain(config),
        destination: Destination::Recipient(recipient_id),
        ..
    } = message
    else {
        panic!("Unexpected message at index {}: {:?}", index, message);
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
        *child_view.system.application_permissions.get(),
        ApplicationPermissions::new_single(application_id)
    );
}

/// Tests the system API call `close_chain``.
#[tokio::test]
async fn test_close_chain() {
    let committee = Committee::make_simple(vec![PublicKey::test_key(0).into()]);
    let committees = BTreeMap::from([(Epoch::ZERO, committee)]);
    let ownership = ChainOwnership::single(PublicKey::test_key(1));
    let state = SystemExecutionState {
        committees: committees.clone(),
        ownership: ownership.clone(),
        balance: Amount::from_tokens(5),
        ..SystemExecutionState::new(Epoch::ZERO, ChainDescription::Root(0), ChainId::root(0))
    };
    let mut view = state.into_view().await;
    let mut applications = register_mock_applications(&mut view, 1).await.unwrap();
    let (application_id, application) = applications.next().unwrap();

    // The application is not authorized to close the chain.
    let context = make_operation_context();
    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_matches!(
                runtime.close_chain(),
                Err(ExecutionError::UnauthorizedApplication(_))
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        Some(Vec::new()),
        &mut controller,
    )
    .await
    .unwrap();
    assert!(!view.system.closed.get());

    // Now we authorize the application and it can close the chain.
    let permissions = ApplicationPermissions::new_single(application_id);
    let operation = SystemOperation::ChangeApplicationPermissions(permissions);
    view.execute_operation(
        context,
        Timestamp::from(0),
        operation.into(),
        Some(Vec::new()),
        &mut controller,
    )
    .await
    .unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.close_chain().unwrap();
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        Some(Vec::new()),
        &mut controller,
    )
    .await
    .unwrap();
    assert!(view.system.closed.get());
}
