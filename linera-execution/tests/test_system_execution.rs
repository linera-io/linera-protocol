// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use linera_base::{
    crypto::AccountSecretKey,
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{Account, AccountOwner},
    ownership::ChainOwnership,
};
use linera_execution::{
    test_utils::{
        dummy_chain_description, dummy_chain_description_with_ownership_and_balance,
        SystemExecutionState,
    },
    Message, MessageContext, Operation, OperationContext, Query, QueryContext, QueryOutcome,
    QueryResponse, ResourceController, SystemMessage, SystemOperation, SystemQuery, SystemResponse,
    TransactionTracker,
};

#[tokio::test]
async fn test_simple_system_operation() -> anyhow::Result<()> {
    let owner_key_pair = AccountSecretKey::generate();
    let owner = AccountOwner::from(owner_key_pair.public());
    let ownership = ChainOwnership {
        super_owners: [owner].into_iter().collect(),
        ..ChainOwnership::default()
    };
    let balance = Amount::from_tokens(4);
    let description =
        dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);
    let chain_id = description.id();
    let state = SystemExecutionState {
        description: Some(description),
        balance,
        ownership,
        ..SystemExecutionState::default()
    };
    let mut view = state.into_view().await;
    let recipient = Account::burn_address(chain_id);
    let operation = SystemOperation::Transfer {
        owner: AccountOwner::CHAIN,
        amount: Amount::from_tokens(4),
        recipient,
    };
    let context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_signer: Some(owner),
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    view.execute_operation(
        context,
        Operation::system(operation),
        &mut txn_tracker,
        &mut controller,
    )
    .await
    .unwrap();
    assert_eq!(view.system.balance.get(), &Amount::ZERO);
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    assert!(txn_outcome.outgoing_messages.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_simple_system_message() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    state.description = Some(description);
    let mut view = state.into_view().await;
    let message = SystemMessage::Credit {
        amount: Amount::from_tokens(4),
        target: AccountOwner::CHAIN,
        source: AccountOwner::CHAIN,
    };
    let context = MessageContext {
        chain_id,
        origin: chain_id,
        is_bouncing: false,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_signer: None,
        refund_grant_to: None,
        timestamp: Default::default(),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new_replaying(Vec::new());
    view.execute_message(
        context,
        Message::System(message),
        None,
        &mut txn_tracker,
        &mut controller,
    )
    .await
    .unwrap();
    assert_eq!(view.system.balance.get(), &Amount::from_tokens(4));
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    assert!(txn_outcome.outgoing_messages.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_simple_system_query() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    state.description = Some(description);
    state.balance = Amount::from_tokens(4);
    let mut view = state.into_view().await;
    let context = QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };
    let QueryOutcome {
        response,
        operations,
    } = view
        .query_application(context, Query::System(SystemQuery), None)
        .await
        .unwrap();
    assert_eq!(
        response,
        QueryResponse::System(SystemResponse {
            chain_id,
            balance: Amount::from_tokens(4)
        })
    );
    assert!(operations.is_empty());
    Ok(())
}
