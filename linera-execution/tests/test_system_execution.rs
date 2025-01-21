// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use linera_base::{
    crypto::{CryptoHash, KeyPair},
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{Account, AccountOwner, ChainDescription, ChainId, MessageId, Owner},
    ownership::ChainOwnership,
};
use linera_execution::{
    system::Recipient, test_utils::SystemExecutionState, ExecutionOutcome, Message, MessageContext,
    Operation, OperationContext, Query, QueryContext, RawExecutionOutcome, ResourceController,
    Response, SystemMessage, SystemOperation, SystemQuery, SystemResponse, TransactionTracker,
};

#[tokio::test]
async fn test_simple_system_operation() -> anyhow::Result<()> {
    let owner_key_pair = KeyPair::generate();
    let owner = Owner::from(owner_key_pair.public());
    let state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balance: Amount::from_tokens(4),
        ownership: ChainOwnership {
            super_owners: [owner].into_iter().collect(),
            ..ChainOwnership::default()
        },
        ..SystemExecutionState::default()
    };
    let mut view = state.into_view().await;
    let operation = SystemOperation::Transfer {
        owner: None,
        amount: Amount::from_tokens(4),
        recipient: Recipient::Burn,
    };
    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: Some(0),
        authenticated_signer: Some(owner),
        authenticated_caller_id: None,
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new(0, Some(Vec::new()));
    view.execute_operation(
        context,
        Timestamp::from(0),
        Operation::System(operation),
        &mut txn_tracker,
        &mut controller,
    )
    .await
    .unwrap();
    assert_eq!(view.system.balance.get(), &Amount::ZERO);
    let account = Account {
        chain_id: ChainId::root(0),
        owner: Some(AccountOwner::User(owner)),
    };
    let (outcomes, _, _) = txn_tracker.destructure().unwrap();
    assert_eq!(
        outcomes,
        vec![ExecutionOutcome::System(
            RawExecutionOutcome::default()
                .with_authenticated_signer(Some(owner))
                .with_refund_grant_to(Some(account))
        )]
    );
    Ok(())
}

#[tokio::test]
async fn test_simple_system_message() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view = state.into_view().await;
    let message = SystemMessage::Credit {
        amount: Amount::from_tokens(4),
        target: None,
        source: None,
    };
    let context = MessageContext {
        chain_id: ChainId::root(0),
        is_bouncing: false,
        height: BlockHeight(0),
        certificate_hash: CryptoHash::test_hash("certificate"),
        message_id: MessageId {
            chain_id: ChainId::root(1),
            height: BlockHeight(0),
            index: 0,
        },
        authenticated_signer: None,
        refund_grant_to: None,
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new(0, Some(Vec::new()));
    view.execute_message(
        context,
        Timestamp::from(0),
        Message::System(message),
        None,
        &mut txn_tracker,
        &mut controller,
    )
    .await
    .unwrap();
    assert_eq!(view.system.balance.get(), &Amount::from_tokens(4));
    let (outcomes, _, _) = txn_tracker.destructure().unwrap();
    assert_eq!(
        outcomes,
        vec![ExecutionOutcome::System(RawExecutionOutcome::default())]
    );
    Ok(())
}

#[tokio::test]
async fn test_simple_system_query() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    state.balance = Amount::from_tokens(4);
    let mut view = state.into_view().await;
    let context = QueryContext {
        chain_id: ChainId::root(0),
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };
    let response = view
        .query_application(context, Query::System(SystemQuery), None)
        .await
        .unwrap();
    assert_eq!(
        response,
        Response::System(SystemResponse {
            chain_id: ChainId::root(0),
            balance: Amount::from_tokens(4)
        })
    );
    Ok(())
}
