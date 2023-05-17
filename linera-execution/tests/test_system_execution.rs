// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::{Amount, BlockHeight},
    identifiers::{ChainDescription, ChainId, EffectId},
};
use linera_execution::{
    system::{Account, Recipient, UserData},
    Effect, EffectContext, ExecutionResult, ExecutionStateView, Operation, OperationContext, Query,
    QueryContext, RawExecutionResult, Response, SystemEffect, SystemExecutionState,
    SystemOperation, SystemQuery, SystemResponse, TestExecutionRuntimeContext,
};
use linera_views::memory::MemoryContext;
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn test_simple_system_operation() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    state.balance = Amount::from(4);
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let operation = SystemOperation::Transfer {
        owner: None,
        amount: Amount::from(4),
        recipient: Recipient::Burn,
        user_data: UserData::default(),
    };
    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: None,
        next_effect_index: 0,
    };
    let results = view
        .execute_operation(&context, &Operation::System(operation))
        .await
        .unwrap();
    assert_eq!(view.system.balance.get(), &Amount::from(0));
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
async fn test_simple_system_effect() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let effect = SystemEffect::Credit {
        amount: Amount::from(4),
        account: Account::chain(ChainId::root(0)),
    };
    let context = EffectContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        certificate_hash: CryptoHash::new(&Dummy),
        effect_id: EffectId {
            chain_id: ChainId::root(1),
            height: BlockHeight(0),
            index: 0,
        },
        authenticated_signer: None,
    };
    let results = view
        .execute_effect(&context, &Effect::System(effect))
        .await
        .unwrap();
    assert_eq!(view.system.balance.get(), &Amount::from(4));
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
    state.balance = Amount::from(4);
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
            balance: Amount::from(4)
        })
    );
    Ok(())
}
