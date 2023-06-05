// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::{Amount, BlockHeight},
    identifiers::{ChainDescription, ChainId, MessageId},
};
use linera_execution::{
    system::{Account, Recipient, UserData},
    ExecutionResult, ExecutionStateView, Message, MessageContext, Operation, OperationContext,
    Query, QueryContext, RawExecutionResult, Response, SystemExecutionState, SystemMessage,
    SystemOperation, SystemQuery, SystemResponse, TestExecutionRuntimeContext,
};
use linera_views::memory::MemoryContext;
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn test_simple_system_operation() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    state.balance = Amount::from_tokens(4);
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let operation = SystemOperation::Transfer {
        owner: None,
        amount: Amount::from_tokens(4),
        recipient: Recipient::Burn,
        user_data: UserData::default(),
    };
    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: None,
        next_message_index: 0,
    };
    let results = view
        .execute_operation(&context, &Operation::System(operation), &mut 10_000_000)
        .await
        .unwrap();
    assert_eq!(view.system.balance.get(), &Amount::ZERO);
    assert_eq!(
        results,
        vec![ExecutionResult::System(RawExecutionResult::default())]
    );
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Dummy;

impl BcsSignable for Dummy {}

#[tokio::test]
async fn test_simple_system_message() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let message = SystemMessage::Credit {
        amount: Amount::from_tokens(4),
        account: Account::chain(ChainId::root(0)),
    };
    let context = MessageContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        certificate_hash: CryptoHash::new(&Dummy),
        message_id: MessageId {
            chain_id: ChainId::root(1),
            height: BlockHeight(0),
            index: 0,
        },
        authenticated_signer: None,
    };
    let results = view
        .execute_message(&context, &Message::System(message), &mut 10_000_000)
        .await
        .unwrap();
    assert_eq!(view.system.balance.get(), &Amount::from_tokens(4));
    assert_eq!(
        results,
        vec![ExecutionResult::System(RawExecutionResult::default())]
    );
    Ok(())
}

#[tokio::test]
async fn test_simple_system_query() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    state.balance = Amount::from_tokens(4);
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    let response = view
        .query_application(&context, &Query::System(SystemQuery))
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
