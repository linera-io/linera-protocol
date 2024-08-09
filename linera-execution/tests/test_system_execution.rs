// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{Account, ChainDescription, ChainId, MessageId},
};
use linera_execution::{
    system::{Recipient, UserData},
    test_utils::SystemExecutionState,
    ExecutionOutcome, Message, MessageContext, Operation, OperationContext, Query, QueryContext,
    RawExecutionOutcome, ResourceController, Response, SystemMessage, SystemOperation, SystemQuery,
    SystemResponse, TransactionTracker,
};

#[tokio::test]
async fn test_simple_system_operation() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    state.balance = Amount::from_tokens(4);
    let mut view = state.into_view().await;
    let operation = SystemOperation::Transfer {
        owner: None,
        amount: Amount::from_tokens(4),
        recipient: Recipient::Burn,
        user_data: UserData::default(),
    };
    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: Some(0),
        authenticated_signer: None,
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
        owner: None,
    };
    let (outcomes, _, _) = txn_tracker.destructure().unwrap();
    assert_eq!(
        outcomes,
        vec![ExecutionOutcome::System(
            RawExecutionOutcome::default().with_refund_grant_to(Some(account))
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
        .query_application(
            context,
            Query::System(SystemQuery),
            &mut futures::channel::mpsc::unbounded().1,
            &mut std::sync::mpsc::channel().0,
        )
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
