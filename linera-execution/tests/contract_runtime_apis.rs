// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use std::collections::BTreeMap;

use anyhow::bail;
use assert_matches::assert_matches;
use linera_base::{
    data_types::{Amount, Blob, OracleResponse, Timestamp},
    identifiers::{Account, AccountOwner, ChainDescription, ChainId},
};
use linera_execution::{
    test_utils::{
        blob_oracle_responses, create_dummy_message_context, create_dummy_operation_context,
        test_accounts_strategy, ExpectedCall, RegisterMockApplication, SystemExecutionState,
        TransferTestEndpoint,
    },
    BaseRuntime, ContractRuntime, ExecutionError, ExecutionOutcome, Message, MessageContext,
    Operation, OperationContext, ResourceController, SystemExecutionError, TransactionOutcome,
    TransactionTracker,
};
use test_case::test_matrix;
use test_strategy::proptest;

/// Tests the contract system API to transfer tokens between accounts.
#[test_matrix(
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application],
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application]
)]
#[test_log::test(tokio::test)]
async fn test_transfer_system_api(
    sender: TransferTestEndpoint,
    recipient: TransferTestEndpoint,
) -> anyhow::Result<()> {
    let amount = Amount::ONE;

    let mut view = sender.create_system_state(amount).into_view().await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, context, _operation| {
            runtime.transfer(
                sender.sender_account_owner(),
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: context.chain_id,
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.signer(),
        ..create_dummy_operation_context()
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new(
        0,
        0,
        Some(vec![
            OracleResponse::Blob(app_desc_blob_id),
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
        ]),
    );
    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut tracker,
        &mut controller,
    )
    .await?;

    let TransactionOutcome {
        outcomes,
        oracle_responses,
        next_message_index,
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outcomes.len(), 1);
    assert_eq!(oracle_responses.len(), 3);
    assert_eq!(next_message_index, 1);

    let ExecutionOutcome::System(ref outcome) = outcomes[0] else {
        bail!("Missing system outcome with expected credit message");
    };

    assert_eq!(outcome.messages.len(), 1);

    view.execute_message(
        create_dummy_message_context(None),
        Timestamp::from(0),
        Message::System(outcome.messages[0].message.clone()),
        None,
        &mut TransactionTracker::new(0, 0, Some(Vec::new())),
        &mut controller,
    )
    .await?;

    recipient.verify_recipient(&view.system, amount).await?;

    Ok(())
}

/// Tests the contract system API to transfer tokens between accounts.
#[test_matrix(
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application],
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application]
)]
#[test_log::test(tokio::test)]
async fn test_unauthorized_transfer_system_api(
    sender: TransferTestEndpoint,
    recipient: TransferTestEndpoint,
) -> anyhow::Result<()> {
    let amount = Amount::ONE;

    let mut view = sender.create_system_state(amount).into_view().await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, context, _operation| {
            runtime.transfer(
                sender.unauthorized_sender_account_owner(),
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: context.chain_id,
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.unauthorized_signer(),
        ..create_dummy_operation_context()
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let result = view
        .execute_operation(
            context,
            Timestamp::from(0),
            operation,
            &mut TransactionTracker::new(
                0,
                0,
                Some(vec![
                    OracleResponse::Blob(app_desc_blob_id),
                    OracleResponse::Blob(contract_blob_id),
                    OracleResponse::Blob(service_blob_id),
                ]),
            ),
            &mut controller,
        )
        .await;

    assert_matches!(
        result,
        Err(ExecutionError::SystemError(
            SystemExecutionError::UnauthenticatedTransferOwner
        ))
    );

    Ok(())
}

/// Tests the contract system API to claim tokens from a remote account.
#[test_matrix(
    [TransferTestEndpoint::User, TransferTestEndpoint::Application],
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application]
)]
#[test_log::test(tokio::test)]
async fn test_claim_system_api(
    sender: TransferTestEndpoint,
    recipient: TransferTestEndpoint,
) -> anyhow::Result<()> {
    let amount = Amount::ONE;

    let claimer_chain_description = ChainDescription::Root(1);

    let source_state = sender.create_system_state(amount);
    let claimer_state = SystemExecutionState {
        description: Some(claimer_chain_description),
        ..SystemExecutionState::default()
    };

    let source_chain_id = ChainId::from(
        source_state
            .description
            .expect("System state created by sender should have a `ChainDescription`"),
    );
    let claimer_chain_id = ChainId::from(claimer_chain_description);

    let mut source_view = source_state.into_view().await;
    let mut claimer_view = claimer_state.into_view().await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = claimer_view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, context, _operation| {
            runtime.claim(
                Account {
                    owner: sender.sender_account_owner(),
                    chain_id: source_chain_id,
                },
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: context.chain_id,
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.signer(),
        chain_id: claimer_chain_id,
        ..create_dummy_operation_context()
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new(
        0,
        0,
        Some(vec![
            OracleResponse::Blob(app_desc_blob_id),
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
        ]),
    );
    claimer_view
        .execute_operation(
            context,
            Timestamp::from(0),
            operation,
            &mut tracker,
            &mut controller,
        )
        .await?;

    let TransactionOutcome {
        outcomes,
        oracle_responses,
        next_message_index,
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outcomes.len(), 1);
    assert_eq!(oracle_responses.len(), 3);
    assert_eq!(next_message_index, 1);

    let ExecutionOutcome::System(ref outcome) = outcomes[0] else {
        bail!("Missing system outcome with expected withdraw message");
    };

    assert_eq!(outcome.messages.len(), 1);

    let mut tracker = TransactionTracker::new(0, 0, Some(Vec::new()));
    source_view
        .execute_message(
            create_dummy_message_context(None),
            Timestamp::from(0),
            Message::System(outcome.messages[0].message.clone()),
            None,
            &mut tracker,
            &mut controller,
        )
        .await?;

    assert_eq!(*source_view.system.balance.get(), Amount::ZERO);
    source_view
        .system
        .balances
        .for_each_index_value(|owner, balance| {
            panic!(
                "No accounts should have tokens after the claim message has been handled, \
                but {owner} has {balance} tokens"
            );
        })
        .await?;

    let TransactionOutcome {
        outcomes,
        oracle_responses,
        next_message_index,
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outcomes.len(), 1);
    assert!(oracle_responses.is_empty());
    assert_eq!(next_message_index, 1);

    let ExecutionOutcome::System(ref outcome) = outcomes[0] else {
        bail!("Missing system outcome with expected credit message");
    };

    assert_eq!(outcome.messages.len(), 1);

    let mut tracker = TransactionTracker::new(0, 0, Some(Vec::new()));
    let context = MessageContext {
        chain_id: claimer_chain_id,
        ..create_dummy_message_context(None)
    };
    claimer_view
        .execute_message(
            context,
            Timestamp::from(0),
            Message::System(outcome.messages[0].message.clone()),
            None,
            &mut tracker,
            &mut controller,
        )
        .await?;

    recipient
        .verify_recipient(&claimer_view.system, amount)
        .await?;

    Ok(())
}

/// Tests the contract system API to claim tokens from an unauthorized remote account.
#[test_matrix(
    [TransferTestEndpoint::User, TransferTestEndpoint::Application],
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application]
)]
#[test_log::test(tokio::test)]
async fn test_unauthorized_claims(
    sender: TransferTestEndpoint,
    recipient: TransferTestEndpoint,
) -> anyhow::Result<()> {
    let amount = Amount::ONE;

    let claimer_chain_description = ChainDescription::Root(1);

    let claimer_state = SystemExecutionState {
        description: Some(claimer_chain_description),
        ..SystemExecutionState::default()
    };

    let source_chain_id = ChainId::root(0);
    let claimer_chain_id = ChainId::from(claimer_chain_description);

    let mut claimer_view = claimer_state.into_view().await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = claimer_view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, context, _operation| {
            runtime.claim(
                Account {
                    owner: sender.unauthorized_sender_account_owner(),
                    chain_id: source_chain_id,
                },
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: context.chain_id,
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.unauthorized_signer(),
        chain_id: claimer_chain_id,
        ..create_dummy_operation_context()
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new(
        0,
        0,
        Some(vec![
            OracleResponse::Blob(app_desc_blob_id),
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
        ]),
    );
    let result = claimer_view
        .execute_operation(
            context,
            Timestamp::from(0),
            operation,
            &mut tracker,
            &mut controller,
        )
        .await;

    assert_matches!(
        result,
        Err(ExecutionError::SystemError(
            SystemExecutionError::UnauthenticatedClaimOwner
        ))
    );

    Ok(())
}

/// Tests the contract system API to read the chain balance.
#[proptest(async = "tokio")]
async fn test_read_chain_balance_system_api(chain_balance: Amount) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balance: chain_balance,
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await
        .unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_eq!(runtime.read_chain_balance().unwrap(), chain_balance);
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(
            0,
            0,
            Some(vec![
                OracleResponse::Blob(app_desc_blob_id),
                OracleResponse::Blob(contract_blob_id),
                OracleResponse::Blob(service_blob_id),
            ]),
        ),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests the contract system API to read a single account balance.
#[proptest(async = "tokio")]
async fn test_read_owner_balance_system_api(
    #[strategy(test_accounts_strategy())] accounts: BTreeMap<AccountOwner, Amount>,
) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balances: accounts.clone(),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            for (owner, balance) in accounts {
                assert_eq!(runtime.read_owner_balance(owner).unwrap(), balance);
            }
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(0, 0, Some(blob_oracle_responses(blobs.iter()))),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests if reading the balance of a missing account returns zero.
#[proptest(async = "tokio")]
async fn test_read_owner_balance_returns_zero_for_missing_accounts(missing_account: AccountOwner) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_eq!(
                runtime.read_owner_balance(missing_account).unwrap(),
                Amount::ZERO
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(0, 0, Some(blob_oracle_responses(blobs.iter()))),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests the contract system API to read all account balances.
#[proptest(async = "tokio")]
async fn test_read_owner_balances_system_api(
    #[strategy(test_accounts_strategy())] accounts: BTreeMap<AccountOwner, Amount>,
) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balances: accounts.clone(),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_eq!(
                runtime.read_owner_balances().unwrap(),
                accounts.into_iter().collect::<Vec<_>>()
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(0, 0, Some(blob_oracle_responses(blobs.iter()))),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests the contract system API to read all account owners.
#[proptest(async = "tokio")]
async fn test_read_balance_owners_system_api(
    #[strategy(test_accounts_strategy())] accounts: BTreeMap<AccountOwner, Amount>,
) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balances: accounts.clone(),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_eq!(
                runtime.read_balance_owners().unwrap(),
                accounts.keys().copied().collect::<Vec<_>>()
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(0, 0, Some(blob_oracle_responses(blobs.iter()))),
        &mut controller,
    )
    .await
    .unwrap();
}
