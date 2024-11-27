// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use std::vec;

use linera_base::{data_types::Amount, identifiers::ChainDescription};
use linera_execution::{
    test_utils::{
        create_dummy_query_context, ExpectedCall, RegisterMockApplication, SystemExecutionState,
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
