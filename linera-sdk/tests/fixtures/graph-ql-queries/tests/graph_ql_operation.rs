// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Matching Engine application

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::{
    test::{QueryOutcome, TestValidator},
};
use graph_ql_queries::{GraphQlQueriesAbi, GraphQlQueriesOperation};


#[tokio::test]
async fn test_queries() {
    let (validator, module_id) =
        TestValidator::with_current_module::<GraphQlQueriesAbi, (), ()>().await;

    let mut chain = validator.new_chain().await;

    let application_id = chain
        .create_application(module_id, (), (), vec![])
        .await;


    let operation = GraphQlQueriesOperation::InsertCollMap { key1: "A".into(), key2: "X".into(), value: 49 };

    chain
        .add_block(|block| {
            block.with_operation(application_id, operation);
        })
        .await;


    // READ1

    for (query, expected_response) in [
        ("collMap { keys }", "{\"collMap\":{\"keys\":[\"A\"]}}"),
        ("collMap { entries { key, value { count } } }", "{\"collMap\":{\"entries\":[{\"key\":\"A\",\"value\":{\"count\":1}}]}}"),
        ("collMap { count }", "{\"collMap\":{\"count\":1}}"),
        ("collMap { entry(key: \"A\") { key, value { count } } }", "{\"collMap\":{\"entry\":{\"key\":\"A\",\"value\":{\"count\":1}}}}"),
        ("collMap { entry(key: \"B\") { key, value { count } } }", "{\"collMap\":{\"entry\":{\"key\":\"B\",\"value\":null}}}"),
        ("collMap { entry(key: \"A\") { key, value { entries(input: {}) { key, value } } } }",
         "{\"collMap\":{\"entry\":{\"key\":\"A\",\"value\":{\"entries\":[{\"key\":\"X\",\"value\":49}]}}}}"),
        ("collMap { entries(input: {}) { key, value { count } } }",
         "{\"collMap\":{\"entries\":[{\"key\":\"A\",\"value\":{\"count\":1}}]}}"),
        ("collMap { entries(input: {}) { key, value { entries(input: {}) { key, value } } } }",
         "{\"collMap\":{\"entries\":[{\"key\":\"A\",\"value\":{\"entries\":[{\"key\":\"X\",\"value\":49}]}}]}}"),
        ("collMap { entries { key, value { keys } } }",
         "{\"collMap\":{\"entries\":[{\"key\":\"A\",\"value\":{\"keys\":[\"X\"]}}]}}"),
        ("collMap { entries(input: {}) { key, value { keys } } }",
         "{\"collMap\":{\"entries\":[{\"key\":\"A\",\"value\":{\"keys\":[\"X\"]}}]}}"),
        ("collMap { entries(input: { filters: {} }) { key, value { keys } } }",
         "{\"collMap\":{\"entries\":[{\"key\":\"A\",\"value\":{\"keys\":[\"X\"]}}]}}"),
        ("collMap { entries(input: { filters: { keys: [\"A\"] } }) { key, value { keys } } }",
         "{\"collMap\":{\"entries\":[{\"key\":\"A\",\"value\":{\"keys\":[\"X\"]}}]}}"),
        ("collMap { entries(input: { filters: { keys: [\"B\"] } }) { key, value { keys } } }",
         "{\"collMap\":{\"entries\":[{\"key\":\"B\",\"value\":null}]}}"),
    ] {
        println!("query={}", query);
        let new_query = format!("query {{ {} }}", query);
        let QueryOutcome { response, .. } = chain.graphql_query(application_id, new_query).await;
        println!("expected_response={expected_response}");
        println!("response={response}");
        assert_eq!(format!("{response}"), expected_response);
        println!();
    }
}
