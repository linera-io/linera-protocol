// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use std::{collections::BTreeMap, vec};

use linera_base::{
    data_types::Amount,
    identifiers::{AccountOwner, ChainDescription},
};
use linera_execution::{
    test_utils::{
        create_dummy_query_context, test_accounts_strategy, ExpectedCall, RegisterMockApplication,
        SystemExecutionState,
    },
    BaseRuntime, Query,
};
use test_strategy::proptest;

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

    let (application_id, application) = view.register_mock_application().await.unwrap();

    application.expect_call(ExpectedCall::handle_query(
        move |runtime, _context, _query| {
            assert_eq!(runtime.read_chain_balance().unwrap(), chain_balance);
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_query_context();
    let query = Query::User {
        application_id,
        bytes: vec![],
    };

    view.query_application(context, query, None).await.unwrap();
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

    let (application_id, application) = view.register_mock_application().await.unwrap();

    application.expect_call(ExpectedCall::handle_query(
        move |runtime, _context, _query| {
            for (owner, balance) in accounts {
                assert_eq!(runtime.read_owner_balance(owner).unwrap(), balance);
            }
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_query_context();
    let query = Query::User {
        application_id,
        bytes: vec![],
    };

    view.query_application(context, query, None).await.unwrap();
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

    let (application_id, application) = view.register_mock_application().await.unwrap();

    application.expect_call(ExpectedCall::handle_query(
        move |runtime, _context, _query| {
            assert_eq!(
                runtime.read_owner_balance(missing_account).unwrap(),
                Amount::ZERO
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_query_context();
    let query = Query::User {
        application_id,
        bytes: vec![],
    };

    view.query_application(context, query, None).await.unwrap();
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

    let (application_id, application) = view.register_mock_application().await.unwrap();

    application.expect_call(ExpectedCall::handle_query(
        move |runtime, _context, _query| {
            assert_eq!(
                runtime.read_owner_balances().unwrap(),
                accounts.into_iter().collect::<Vec<_>>(),
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_query_context();
    let query = Query::User {
        application_id,
        bytes: vec![],
    };

    view.query_application(context, query, None).await.unwrap();
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

    let (application_id, application) = view.register_mock_application().await.unwrap();

    application.expect_call(ExpectedCall::handle_query(
        move |runtime, _context, _query| {
            assert_eq!(
                runtime.read_balance_owners().unwrap(),
                accounts.keys().copied().collect::<Vec<_>>()
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_query_context();
    let query = Query::User {
        application_id,
        bytes: vec![],
    };

    view.query_application(context, query, None).await.unwrap();
}
