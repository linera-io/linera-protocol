// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use std::{collections::BTreeMap, vec};

use assert_matches::assert_matches;
use linera_base::{
    crypto::{AccountPublicKey, ValidatorPublicKey},
    data_types::{
        Amount, ApplicationPermissions, Blob, BlockHeight, ChainDescription, ChainOrigin, Epoch,
        InitialChainConfig, Resources, SendMessageRequest, Timestamp,
    },
    identifiers::{Account, AccountOwner, BlobType},
    ownership::ChainOwnership,
};
use linera_execution::{
    committee::Committee,
    test_utils::{
        blob_oracle_responses, create_dummy_message_context, create_dummy_operation_context,
        create_dummy_user_application_registrations, dummy_chain_description,
        dummy_chain_description_with_ownership_and_balance, ExpectedCall, RegisterMockApplication,
        SystemExecutionState,
    },
    BaseRuntime, ContractRuntime, ExecutionError, ExecutionRuntimeContext, Message, Operation,
    OperationContext, OutgoingMessage, Query, QueryContext, QueryOutcome, QueryResponse,
    ResourceController, SystemOperation, TransactionTracker,
};
use linera_views::{batch::Batch, context::Context, views::View};
use test_case::test_case;

#[tokio::test]
async fn test_missing_bytecode_for_user_application() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (app_id, app_desc, contract_blob, service_blob) =
        &create_dummy_user_application_registrations(1).await?[0];
    let app_desc_blob = Blob::new_application_description(app_desc);
    let app_desc_blob_id = app_desc_blob.id();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();
    view.context()
        .extra()
        .add_blobs([contract_blob.clone(), service_blob.clone(), app_desc_blob])
        .await?;

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: *app_id,
                bytes: vec![],
            },
            &mut TransactionTracker::new_replaying_blobs([
                app_desc_blob_id,
                contract_blob_id,
                service_blob_id,
            ]),
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
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(1).await?;

    let owner = AccountOwner::from(AccountPublicKey::test_key(0));
    let state_key = vec![];
    let dummy_operation = vec![1];

    caller_application.expect_call({
        let state_key = state_key.clone();
        let dummy_operation = dummy_operation.clone();
        ExpectedCall::execute_operation(move |runtime, operation| {
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
        move |_runtime, argument| {
            assert_eq!(&argument, &[SessionCall::StartSession as u8]);
            Ok(vec![])
        },
    ));
    target_application.expect_call(ExpectedCall::execute_operation(
        move |_runtime, argument| {
            assert_eq!(&argument, &[SessionCall::EndSession as u8]);
            Ok(vec![])
        },
    ));

    target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: Some(owner),
        ..create_dummy_operation_context(chain_id)
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker =
        TransactionTracker::new_replaying_blobs(caller_blobs.iter().chain(&target_blobs));
    view.execute_operation(
        context,
        Operation::User {
            application_id: caller_id,
            bytes: dummy_operation.clone(),
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await
    .unwrap();
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    assert!(txn_outcome.outgoing_messages.is_empty());

    {
        let state_key = state_key.clone();
        caller_application.expect_call(ExpectedCall::handle_query(|runtime, _query| {
            let state = runtime.read_value_bytes(state_key)?.unwrap_or_default();
            Ok(state)
        }));
    }

    caller_application.expect_call(ExpectedCall::handle_query(|runtime, _query| {
        let state = runtime.read_value_bytes(state_key)?.unwrap_or_default();
        Ok(state)
    }));

    let context = QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };
    let mut service_runtime_endpoint = context.spawn_service_runtime_actor();
    assert_eq!(
        view.query_application(
            context,
            Query::User {
                application_id: caller_id,
                bytes: vec![]
            },
            Some(&mut service_runtime_endpoint),
        )
        .await
        .unwrap(),
        QueryOutcome {
            response: QueryResponse::User(dummy_operation.clone()),
            operations: vec![],
        }
    );

    assert_eq!(
        view.query_application(
            context,
            Query::User {
                application_id: caller_id,
                bytes: vec![]
            },
            Some(&mut service_runtime_endpoint),
        )
        .await
        .unwrap(),
        QueryOutcome {
            response: QueryResponse::User(dummy_operation),
            operations: vec![],
        }
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
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(1).await?;

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
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
        move |runtime, argument| {
            assert_eq!(&argument, &[SessionCall::StartSession as u8]);

            let mut batch = Batch::new();
            batch.put_key_value_bytes(state_key, vec![true as u8]);
            runtime.write_batch(batch)?;

            Ok(vec![])
        }
    }));

    target_application.expect_call(ExpectedCall::execute_operation({
        let state_key = state_key.clone();
        move |runtime, argument| {
            assert_eq!(&argument, &[SessionCall::EndSession as u8]);

            let mut batch = Batch::new();
            batch.put_key_value_bytes(state_key, vec![false as u8]);
            runtime.write_batch(batch)?;

            Ok(vec![])
        }
    }));

    target_application.expect_call(ExpectedCall::finalize(|runtime| {
        match runtime.read_value_bytes(state_key)? {
            Some(session_is_open) if session_is_open == vec![u8::from(false)] => Ok(()),
            Some(_) => Err(ExecutionError::UserError("Leaked session".to_owned())),
            _ => Err(ExecutionError::UserError(
                "Missing or invalid session state".to_owned(),
            )),
        }
    }));
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let mut txn_tracker =
        TransactionTracker::new_replaying_blobs(caller_blobs.iter().chain(&target_blobs));
    view.execute_operation(
        context,
        Operation::User {
            application_id: caller_id,
            bytes: vec![],
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await?;
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    assert!(txn_outcome.outgoing_messages.is_empty());
    Ok(())
}

/// Tests if execution fails if a simulated session isn't properly closed.
#[tokio::test]
async fn test_simulated_session_leak() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(1).await?;

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
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
        |runtime, argument| {
            assert_eq!(argument, &[SessionCall::StartSession as u8]);

            let mut batch = Batch::new();
            batch.put_key_value_bytes(state_key, vec![true as u8]);
            runtime.write_batch(batch)?;

            Ok(Vec::new())
        }
    }));

    let error_message = "Session leaked";

    target_application.expect_call(ExpectedCall::finalize(|runtime| {
        match runtime.read_value_bytes(state_key)? {
            Some(session_is_open) if session_is_open == vec![u8::from(false)] => Ok(()),
            Some(_) => Err(ExecutionError::UserError(error_message.to_owned())),
            _ => Err(ExecutionError::UserError(
                "Missing or invalid session state".to_owned(),
            )),
        }
    }));

    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut TransactionTracker::new_replaying_blobs(caller_blobs.iter().chain(&target_blobs)),
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::UserError(message)) if message == error_message);
    Ok(())
}

/// Tests if `finalize` can cause execution to fail.
#[tokio::test]
async fn test_rejecting_block_from_finalize() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (id, application, blobs) = view.register_mock_application(0).await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |_runtime, _operation| Ok(vec![]),
    ));

    let error_message = "Finalize aborted execution";

    application.expect_call(ExpectedCall::finalize(|_runtime| {
        Err(ExecutionError::UserError(error_message.to_owned()))
    }));

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: id,
                bytes: vec![],
            },
            &mut TransactionTracker::new_replaying_blobs(blobs),
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::UserError(message)) if message == error_message);
    Ok(())
}

/// Tests if `finalize` from a called application can cause execution to fail.
#[tokio::test]
async fn test_rejecting_block_from_called_applications_finalize() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (first_id, first_application, first_app_blobs) = view.register_mock_application(0).await?;
    let (second_id, second_application, second_app_blobs) =
        view.register_mock_application(1).await?;
    let (third_id, third_application, third_app_blobs) = view.register_mock_application(2).await?;
    let (fourth_id, fourth_application, fourth_app_blobs) =
        view.register_mock_application(3).await?;

    first_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.try_call_application(false, second_id, vec![])?;
            Ok(vec![])
        },
    ));
    second_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _argument| {
            runtime.try_call_application(false, third_id, vec![])?;
            Ok(vec![])
        },
    ));
    third_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _argument| {
            runtime.try_call_application(false, fourth_id, vec![])?;
            Ok(vec![])
        },
    ));
    fourth_application.expect_call(ExpectedCall::execute_operation(|_runtime, _argument| {
        Ok(vec![])
    }));

    let error_message = "Third application aborted execution";

    fourth_application.expect_call(ExpectedCall::default_finalize());
    third_application.expect_call(ExpectedCall::finalize(|_runtime| {
        Err(ExecutionError::UserError(error_message.to_owned()))
    }));
    second_application.expect_call(ExpectedCall::default_finalize());
    first_application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: first_id,
                bytes: vec![],
            },
            &mut TransactionTracker::new_replaying_blobs(
                first_app_blobs
                    .iter()
                    .chain(&second_app_blobs)
                    .chain(&third_app_blobs)
                    .chain(&fourth_app_blobs),
            ),
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::UserError(message)) if message == error_message);
    Ok(())
}

/// Tests if `finalize` can send messages.
#[tokio::test]
async fn test_sending_message_from_finalize() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (first_id, first_application, first_app_blobs) = view.register_mock_application(0).await?;
    let (second_id, second_application, second_app_blobs) =
        view.register_mock_application(1).await?;
    let (third_id, third_application, third_app_blobs) = view.register_mock_application(2).await?;
    let (fourth_id, fourth_application, fourth_app_blobs) =
        view.register_mock_application(3).await?;

    let destination = dummy_chain_description(1).id();
    let first_message = SendMessageRequest {
        destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"first".to_vec(),
    };
    let expected_first_message = OutgoingMessage::new(
        destination,
        Message::User {
            application_id: third_id,
            bytes: b"first".to_vec(),
        },
    );

    first_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.try_call_application(false, second_id, vec![])?;
            Ok(vec![])
        },
    ));
    second_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _argument| {
            runtime.try_call_application(false, third_id, vec![])?;
            Ok(vec![])
        },
    ));
    third_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _argument| {
            runtime.send_message(first_message)?;
            runtime.try_call_application(false, fourth_id, vec![])?;
            Ok(vec![])
        },
    ));
    fourth_application.expect_call(ExpectedCall::execute_operation(|_runtime, _argument| {
        Ok(vec![])
    }));

    let second_message = SendMessageRequest {
        destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"second".to_vec(),
    };
    let third_message = SendMessageRequest {
        destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"third".to_vec(),
    };
    let fourth_message = SendMessageRequest {
        destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"fourth".to_vec(),
    };

    let expected_second_message = OutgoingMessage::new(
        destination,
        Message::User {
            application_id: third_id,
            bytes: b"second".to_vec(),
        },
    );
    let expected_third_message = OutgoingMessage::new(
        destination,
        Message::User {
            application_id: third_id,
            bytes: b"third".to_vec(),
        },
    );
    let expected_fourth_message = OutgoingMessage::new(
        destination,
        Message::User {
            application_id: first_id,
            bytes: b"fourth".to_vec(),
        },
    );

    fourth_application.expect_call(ExpectedCall::default_finalize());
    third_application.expect_call(ExpectedCall::finalize(|runtime| {
        runtime.send_message(second_message)?;
        runtime.send_message(third_message)?;
        Ok(())
    }));
    second_application.expect_call(ExpectedCall::default_finalize());
    first_application.expect_call(ExpectedCall::finalize(|runtime| {
        runtime.send_message(fourth_message)?;
        Ok(())
    }));

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying_blobs(
        first_app_blobs
            .iter()
            .chain(&second_app_blobs)
            .chain(&third_app_blobs)
            .chain(&fourth_app_blobs),
    );
    view.execute_operation(
        context,
        Operation::User {
            application_id: first_id,
            bytes: vec![],
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await?;

    let txn_outcome = txn_tracker.into_outcome().unwrap();
    let mut expected = TransactionTracker::default();
    expected.add_outgoing_messages(vec![
        expected_first_message,
        expected_second_message,
        expected_third_message,
        expected_fourth_message,
    ]);
    assert_eq!(
        txn_outcome.outgoing_messages,
        expected.into_outcome().unwrap().outgoing_messages
    );
    Ok(())
}

/// Tests if an application can't perform cross-application calls during `finalize`.
#[tokio::test]
async fn test_cross_application_call_from_finalize() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, _target_application, target_blobs) = view.register_mock_application(1).await?;

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |_runtime, _operation| Ok(vec![]),
    ));

    caller_application.expect_call(ExpectedCall::finalize({
        move |runtime| {
            runtime.try_call_application(false, target_id, vec![])?;
            Ok(())
        }
    }));

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut TransactionTracker::new_replaying_blobs(caller_blobs.iter().chain(&target_blobs)),
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
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(1).await?;

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.try_call_application(false, target_id, vec![])?;
            Ok(vec![])
        },
    ));
    target_application.expect_call(ExpectedCall::execute_operation(|_runtime, _argument| {
        Ok(vec![])
    }));

    target_application.expect_call(ExpectedCall::finalize({
        move |runtime| {
            runtime.try_call_application(false, caller_id, vec![])?;
            Ok(())
        }
    }));
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut TransactionTracker::new_replaying_blobs(caller_blobs.iter().chain(&target_blobs)),
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
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(1).await?;

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.try_call_application(false, target_id, vec![])?;
            Ok(vec![])
        },
    ));
    target_application.expect_call(ExpectedCall::execute_operation(|_runtime, _argument| {
        Ok(vec![])
    }));

    target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::finalize({
        move |runtime| {
            runtime.try_call_application(false, target_id, vec![])?;
            Ok(())
        }
    }));

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let result = view
        .execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut TransactionTracker::new_replaying_blobs(caller_blobs.iter().chain(&target_blobs)),
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
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(1).await?;

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.try_call_application(/* authenticated */ false, target_id, vec![])?;
            Ok(vec![])
        },
    ));

    let error_message = "Cross-application call failed";

    target_application.expect_call(ExpectedCall::execute_operation(|_runtime, _argument| {
        Err(ExecutionError::UserError(error_message.to_owned()))
    }));

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    assert_matches!(
        view.execute_operation(
            context,
            Operation::User {
                application_id: caller_id,
                bytes: vec![],
            },
            &mut TransactionTracker::new_replaying_blobs(
                caller_blobs.iter().chain(&target_blobs)
            ),
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
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (application_id, application, blobs) = view.register_mock_application(0).await?;

    let destination = dummy_chain_description(1).id();
    let dummy_message = SendMessageRequest {
        destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"msg".to_vec(),
    };

    let expected_dummy_message = OutgoingMessage::new(
        destination,
        Message::User {
            application_id,
            bytes: b"msg".to_vec(),
        },
    );

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.send_message(dummy_message)?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying_blobs(blobs);
    view.execute_operation(
        context,
        Operation::User {
            application_id,
            bytes: vec![],
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await?;

    let txn_outcome = txn_tracker.into_outcome().unwrap();
    let mut expected = TransactionTracker::default();
    expected.add_outgoing_message(expected_dummy_message);
    assert_eq!(
        txn_outcome.outgoing_messages,
        expected.into_outcome().unwrap().outgoing_messages
    );

    Ok(())
}

/// Tests if a message is scheduled to be sent while an application is handling a cross-application
/// call.
#[tokio::test]
async fn test_message_from_cross_application_call() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(1).await?;

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.try_call_application(/* authenticated */ false, target_id, vec![])?;
            Ok(vec![])
        },
    ));

    let destination = dummy_chain_description(1).id();
    let dummy_message = SendMessageRequest {
        destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"msg".to_vec(),
    };

    let expected_dummy_message = OutgoingMessage::new(
        destination,
        Message::User {
            application_id: target_id,
            bytes: b"msg".to_vec(),
        },
    );

    target_application.expect_call(ExpectedCall::execute_operation(|runtime, _argument| {
        runtime.send_message(dummy_message)?;
        Ok(vec![])
    }));

    target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let mut txn_tracker =
        TransactionTracker::new_replaying_blobs(caller_blobs.iter().chain(&target_blobs));
    view.execute_operation(
        context,
        Operation::User {
            application_id: caller_id,
            bytes: vec![],
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await?;

    let txn_outcome = txn_tracker.into_outcome().unwrap();
    let mut expected = TransactionTracker::default();
    expected.add_outgoing_message(expected_dummy_message);
    assert_eq!(
        txn_outcome.outgoing_messages,
        expected.into_outcome().unwrap().outgoing_messages
    );

    Ok(())
}

/// Tests if a message is scheduled to be sent by a deeper cross-application call.
#[tokio::test]
async fn test_message_from_deeper_call() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (middle_id, middle_application, middle_blobs) = view.register_mock_application(1).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(2).await?;

    caller_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.try_call_application(/* authenticated */ false, middle_id, vec![])?;
            Ok(vec![])
        },
    ));

    middle_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _argument| {
            runtime.try_call_application(/* authenticated */ false, target_id, vec![])?;
            Ok(vec![])
        },
    ));

    let destination = dummy_chain_description(1).id();
    let dummy_message = SendMessageRequest {
        destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"msg".to_vec(),
    };

    let expected_dummy_message = OutgoingMessage::new(
        destination,
        Message::User {
            application_id: target_id,
            bytes: b"msg".to_vec(),
        },
    );

    target_application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _argument| {
            runtime.send_message(dummy_message)?;
            Ok(vec![])
        },
    ));

    target_application.expect_call(ExpectedCall::default_finalize());
    middle_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying_blobs(
        caller_blobs
            .iter()
            .chain(&middle_blobs)
            .chain(&target_blobs),
    );
    view.execute_operation(
        context,
        Operation::User {
            application_id: caller_id,
            bytes: vec![],
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await?;

    let txn_outcome = txn_tracker.into_outcome().unwrap();
    let mut expected = TransactionTracker::default();
    expected.add_outgoing_message(expected_dummy_message);
    assert_eq!(
        txn_outcome.outgoing_messages,
        expected.into_outcome().unwrap().outgoing_messages
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
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    // The entrypoint application, which sends a message and calls other applications
    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    // An application that does not send any messages
    let (silent_target_id, silent_target_application, silent_blobs) =
        view.register_mock_application(1).await?;
    // An application that sends a message when handling a cross-application call
    let (sending_target_id, sending_target_application, sending_blobs) =
        view.register_mock_application(2).await?;

    // The first destination chain receives messages from the caller and the sending applications
    let first_destination = dummy_chain_description(1).id();
    // The second destination chain only receives a message from the sending application
    let second_destination = dummy_chain_description(2).id();

    // The message sent to the first destination chain by the caller and the sending applications
    let first_message = SendMessageRequest {
        destination: first_destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"first".to_vec(),
    };

    // The entrypoint sends a message to the first chain and calls the silent and the sending
    // applications
    caller_application.expect_call(ExpectedCall::execute_operation({
        let first_message = first_message.clone();
        move |runtime, _operation| {
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
        |_runtime, _argument| Ok(vec![]),
    ));

    // The message sent to the second destination chain by the sending application
    let second_message = SendMessageRequest {
        destination: second_destination,
        authenticated: false,
        is_tracked: false,
        grant: Resources::default(),
        message: b"second".to_vec(),
    };

    // The sending application sends two messages, one to each of the destination chains
    sending_target_application.expect_call(ExpectedCall::execute_operation(
        |runtime, _argument| {
            runtime.send_message(first_message)?;
            runtime.send_message(second_message)?;
            Ok(vec![])
        },
    ));

    sending_target_application.expect_call(ExpectedCall::default_finalize());
    silent_target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    // Execute the operation, starting the test scenario
    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying_blobs(
        caller_blobs
            .iter()
            .chain(&silent_blobs)
            .chain(&sending_blobs),
    );
    view.execute_operation(
        context,
        Operation::User {
            application_id: caller_id,
            bytes: vec![],
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await?;

    // Return to checking the user application outcomes
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    let mut expected = TransactionTracker::default();
    expected.add_outgoing_messages(vec![
        OutgoingMessage::new(
            first_destination,
            Message::User {
                application_id: caller_id,
                bytes: b"first".to_vec(),
            },
        ),
        OutgoingMessage::new(
            first_destination,
            Message::User {
                application_id: sending_target_id,
                bytes: b"first".to_vec(),
            },
        ),
        OutgoingMessage::new(
            second_destination,
            Message::User {
                application_id: sending_target_id,
                bytes: b"second".to_vec(),
            },
        ),
    ]);
    assert_eq!(
        txn_outcome.outgoing_messages,
        expected.into_outcome().unwrap().outgoing_messages
    );

    Ok(())
}

/// Tests the system API calls `open_chain` and `chain_ownership`.
#[tokio::test]
async fn test_open_chain() -> anyhow::Result<()> {
    let committee = Committee::make_simple(vec![(
        ValidatorPublicKey::test_key(0),
        AccountPublicKey::test_key(0),
    )]);
    let committee_blob = Blob::new_committee(
        bcs::to_bytes(&committee).expect("serializing a committee should succeed"),
    );
    let chain_key = AccountPublicKey::test_key(1);
    let ownership = ChainOwnership::single(chain_key.into());
    let child_ownership = ChainOwnership::single(AccountPublicKey::test_key(2).into());
    let balance = Amount::from_tokens(5);
    let root_description =
        dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);
    let state = SystemExecutionState::new(root_description.clone());
    let mut view = state.into_view().await;
    view.context()
        .extra()
        .add_blobs([
            committee_blob.clone(),
            Blob::new_chain_description(&root_description),
        ])
        .await?;
    let (application_id, application, blobs) = view.register_mock_application(0).await?;

    let context = OperationContext {
        height: BlockHeight(1),
        authenticated_signer: Some(chain_key.into()),
        ..create_dummy_operation_context(root_description.id())
    };
    let first_message_index = 5;

    let child_origin = ChainOrigin::Child {
        parent: root_description.id(),
        block_height: BlockHeight(1),
        chain_index: 0,
    };
    let child_application_permissions = ApplicationPermissions::new_single(application_id);
    let child_config = InitialChainConfig {
        balance: Amount::ONE,
        ownership: child_ownership.clone(),
        application_permissions: child_application_permissions.clone(),
        ..root_description.config().clone()
    };
    let child_description = ChainDescription::new(child_origin, child_config, Timestamp::default());
    let child_id = child_description.id();

    application.expect_call(ExpectedCall::execute_operation({
        let child_ownership = child_ownership.clone();
        move |runtime, _operation| {
            assert_eq!(runtime.chain_ownership()?, ownership);
            let destination = Account::chain(dummy_chain_description(2).id());
            runtime.transfer(AccountOwner::CHAIN, destination, Amount::ONE)?;
            let application_permissions = child_application_permissions.clone();
            let chain_id =
                runtime.open_chain(child_ownership, application_permissions, Amount::ONE)?;
            assert_eq!(chain_id, child_id);
            Ok(vec![])
        }
    }));
    application.expect_call(ExpectedCall::default_finalize());

    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut txn_tracker = TransactionTracker::new(
        Timestamp::from(0),
        1,
        first_message_index,
        0,
        Some(blob_oracle_responses(blobs.iter())),
        &[],
    );
    view.execute_operation(context, operation, &mut txn_tracker, &mut controller)
        .await?;

    assert_eq!(*view.system.balance.get(), Amount::from_tokens(3));
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    let new_blob = &txn_outcome.blobs[0];
    assert_eq!(new_blob.id().hash, child_id.0);
    assert_eq!(new_blob.id().blob_type, BlobType::ChainDescription);
    let created_description: ChainDescription =
        bcs::from_bytes(new_blob.bytes()).expect("should deserialize a chain description");
    assert_eq!(created_description.config().balance, Amount::ONE);
    assert_eq!(created_description.config().ownership, child_ownership);

    // Initialize the child chain using the new blob.
    let mut child_view = SystemExecutionState::default()
        .into_view_with(child_id, Default::default())
        .await;
    child_view
        .context()
        .extra()
        .add_blobs([
            committee_blob.clone(),
            Blob::new_chain_description(&child_description),
        ])
        .await?;
    child_view
        .system
        .initialize_chain(child_description.id())
        .await
        .expect("should initialize chain correctly");
    assert_eq!(*child_view.system.balance.get(), Amount::ONE);
    assert_eq!(*child_view.system.ownership.get(), child_ownership);
    assert_eq!(
        *child_view.system.committees.get(),
        [(Epoch::ZERO, committee)]
            .into_iter()
            .collect::<BTreeMap<_, _>>()
    );
    assert_eq!(
        *child_view.system.application_permissions.get(),
        ApplicationPermissions::new_single(application_id)
    );

    Ok(())
}

/// Tests the system API call `close_chain`.
#[tokio::test]
async fn test_close_chain() -> anyhow::Result<()> {
    let ownership = ChainOwnership::single(AccountPublicKey::test_key(1).into());
    let description = dummy_chain_description_with_ownership_and_balance(
        0,
        ownership.clone(),
        Amount::from_tokens(5),
    );
    let chain_id = description.id();
    let state = SystemExecutionState::new(description);
    let mut view = state.into_view().await;
    let (application_id, application, blobs) = view.register_mock_application(0).await?;

    // The application is not authorized to close the chain.
    let context = create_dummy_operation_context(chain_id);
    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
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
        operation,
        &mut TransactionTracker::new_replaying_blobs(blobs),
        &mut controller,
    )
    .await?;
    assert!(!view.system.closed.get());

    // Now we authorize the application and it can close the chain.
    let permissions = ApplicationPermissions::new_single(application_id);
    let operation = SystemOperation::ChangeApplicationPermissions(permissions);
    view.execute_operation(
        context,
        operation.into(),
        &mut TransactionTracker::new_replaying(Vec::new()),
        &mut controller,
    )
    .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            runtime.close_chain()?;
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
        operation,
        &mut TransactionTracker::new_replaying(Vec::new()),
        &mut controller,
    )
    .await?;
    assert!(view.system.closed.get());

    Ok(())
}

/// Tests an application attempting to transfer the tokens in the chain's balance while executing
/// messages.
#[test_case(
    Some(AccountPublicKey::test_key(1).into()), Some(AccountPublicKey::test_key(1).into())
    => matches Ok(Ok(()));
    "works if sender is a receiving chain owner"
)]
#[test_case(
    Some(AccountPublicKey::test_key(1).into()), Some(AccountPublicKey::test_key(2).into())
    => matches Ok(Err(ExecutionError::UnauthenticatedTransferOwner));
    "fails if sender is not a receiving chain owner"
)]
#[test_case(
    Some(AccountPublicKey::test_key(1).into()), None
    => matches Ok(Err(ExecutionError::UnauthenticatedTransferOwner));
    "fails if unauthenticated"
)]
#[test_case(
    None, None
    => matches Ok(Err(ExecutionError::UnauthenticatedTransferOwner));
    "fails if unauthenticated and receiving chain has no owners"
)]
#[test_case(
    None, Some(AccountPublicKey::test_key(1).into())
    => matches Ok(Err(ExecutionError::UnauthenticatedTransferOwner));
    "fails if receiving chain has no owners"
)]
#[tokio::test]
async fn test_message_receipt_spending_chain_balance(
    receiving_chain_owner: Option<AccountOwner>,
    authenticated_signer: Option<AccountOwner>,
) -> anyhow::Result<Result<(), ExecutionError>> {
    let amount = Amount::ONE;
    let super_owners = receiving_chain_owner.into_iter().collect();

    let description = dummy_chain_description_with_ownership_and_balance(
        0,
        ChainOwnership {
            super_owners,
            ..ChainOwnership::default()
        },
        amount,
    );
    let chain_id = description.id();

    let mut view = SystemExecutionState::new(description).into_view().await;

    let (application_id, application, blobs) = view.register_mock_application(0).await?;

    let receiver_chain_account = AccountOwner::CHAIN;
    let sender_chain_id = dummy_chain_description(2).id();
    let recipient = Account {
        chain_id: sender_chain_id,
        owner: AccountOwner::CHAIN,
    };

    application.expect_call(ExpectedCall::execute_message(move |runtime, _operation| {
        runtime.transfer(receiver_chain_account, recipient, amount)?;
        Ok(())
    }));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_message_context(chain_id, authenticated_signer);
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying_blobs(blobs);

    let execution_result = view
        .execute_message(
            context,
            Message::User {
                application_id,
                bytes: vec![],
            },
            None,
            &mut txn_tracker,
            &mut controller,
        )
        .await;

    Ok(execution_result)
}
