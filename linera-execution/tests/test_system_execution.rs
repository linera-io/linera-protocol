// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use linera_base::{
    crypto::{BcsSignable, HashValue},
    data_types::{BlockHeight, ChainDescription, ChainId, EffectId},
};
use linera_execution::{
    system::{Address, Amount, Balance, UserData},
    ApplicationDescription, ApplicationRegistryView, Effect, EffectContext, ExecutionResult,
    ExecutionStateView, Operation, OperationContext, Query, QueryContext, RawExecutionResult,
    Response, SystemEffect, SystemExecutionState, SystemOperation, SystemQuery, SystemResponse,
    TestExecutionRuntimeContext,
};
use linera_views::{memory::MemoryContext, views::View};
use serde::{Deserialize, Serialize};

#[tokio::test]
async fn test_simple_system_operation() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    state.balance = Balance::from(4);
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let mut applications = ApplicationRegistryView::load(view.context().clone()).await?;
    let operation = SystemOperation::Transfer {
        amount: Amount::from(4),
        recipient: Address::Burn,
        user_data: UserData::default(),
    };
    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
    };
    let result = view
        .execute_operation(
            &ApplicationDescription::System,
            &context,
            &Operation::System(operation),
            &mut applications,
        )
        .await
        .unwrap();
    assert_eq!(view.system.balance.get(), &Balance::from(0));
    assert_eq!(
        result,
        vec![ExecutionResult::System {
            result: RawExecutionResult::default(),
            new_application: None
        }]
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
    let mut applications = ApplicationRegistryView::load(view.context().clone()).await?;
    let effect = SystemEffect::Credit {
        amount: Amount::from(4),
        recipient: ChainId::root(0),
    };
    let context = EffectContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        certificate_hash: HashValue::new(&Dummy),
        effect_id: EffectId {
            chain_id: ChainId::root(1),
            height: BlockHeight(0),
            index: 0,
        },
    };
    let result = view
        .execute_effect(
            &ApplicationDescription::System,
            &context,
            &Effect::System(effect),
            &mut applications,
        )
        .await
        .unwrap();
    assert_eq!(view.system.balance.get(), &Balance::from(4));
    assert_eq!(
        result,
        vec![ExecutionResult::System {
            result: RawExecutionResult::default(),
            new_application: None
        }]
    );
    Ok(())
}

#[tokio::test]
async fn test_simple_system_query() -> anyhow::Result<()> {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    state.balance = Balance::from(4);
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let mut applications = ApplicationRegistryView::load(view.context().clone()).await?;
    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    let response = view
        .query_application(
            &ApplicationDescription::System,
            &context,
            &Query::System(SystemQuery),
            &mut applications,
        )
        .await
        .unwrap();
    assert_eq!(
        response,
        Response::System(SystemResponse {
            chain_id: ChainId::root(0),
            balance: Balance::from(4)
        })
    );
    Ok(())
}
