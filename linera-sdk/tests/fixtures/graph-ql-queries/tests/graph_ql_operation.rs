// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Matching Engine application

#![cfg(not(target_arch = "wasm32"))]

use graph_ql_queries::{GraphQlQueriesAbi, GraphQlQueriesOperation};
use linera_sdk::test::{QueryOutcome, TestValidator};

#[tokio::test]
async fn test_queries() {
    let (validator, module_id) =
        TestValidator::with_current_module::<GraphQlQueriesAbi, (), ()>().await;

    let mut chain = validator.new_chain().await;

    let application_id = chain.create_application(module_id, (), (), vec![]).await;

    let operation1 = GraphQlQueriesOperation::SetRegister { value: 124 };
    let operation2 = GraphQlQueriesOperation::InsertMapString {
        key: "a".into(),
        value: 91,
    };
    let operation3 = GraphQlQueriesOperation::InsertCollString {
        key: "a".into(),
        value: 91,
    };
    let operation4 = GraphQlQueriesOperation::InsertCollMap {
        key1: "A".into(),
        key2: "X".into(),
        value: 49,
    };

    chain
        .add_block(|block| {
            block
                .with_operation(application_id, operation1)
                .with_operation(application_id, operation2)
                .with_operation(application_id, operation3)
                .with_operation(application_id, operation4);
        })
        .await;

    // READ1

    for (query, expected_response) in [
        ("reg", "{\"reg\":124}"),
        ("mapS { keys }", "{\"mapS\":{\"keys\":[\"a\"]}}"),
        ("mapS { entry(key: \"a\") { key, value } }", "{\"mapS\":{\"entry\":{\"key\":\"a\",\"value\":91}}}"),
        ("mapS { entries { key, value } }", "{\"mapS\":{\"entries\":[{\"key\":\"a\",\"value\":91}]}}"),
        ("mapS { entries(input: {}) { key, value } }", "{\"mapS\":{\"entries\":[{\"key\":\"a\",\"value\":91}]}}"),
        ("mapS { entries(input: { filters: {} }) { key, value } }", "{\"mapS\":{\"entries\":[{\"key\":\"a\",\"value\":91}]}}"),
        ("mapS { entries(input: { filters: { keys: [\"a\"]} }) { key, value } }", "{\"mapS\":{\"entries\":[{\"key\":\"a\",\"value\":91}]}}"),
        ("mapS { entries(input: { filters: { keys: [\"b\"]} }) { key, value } }", "{\"mapS\":{\"entries\":[{\"key\":\"b\",\"value\":null}]}}"),
        ("mapS { count }", "{\"mapS\":{\"count\":1}}"),
        ("collS { keys }", "{\"collS\":{\"keys\":[\"a\"]}}"),
        ("collS { entry(key: \"a\") { key, value } }", "{\"collS\":{\"entry\":{\"key\":\"a\",\"value\":91}}}"),
        ("collS { entries { key, value } }", "{\"collS\":{\"entries\":[{\"key\":\"a\",\"value\":91}]}}"),
        ("collS { entries(input: {}) { key, value } }", "{\"collS\":{\"entries\":[{\"key\":\"a\",\"value\":91}]}}"),
        ("collS { entries(input: { filters: {} }) { key, value } }", "{\"collS\":{\"entries\":[{\"key\":\"a\",\"value\":91}]}}"),
        ("collS { entries(input: { filters: { keys: [\"a\"]} }) { key, value } }", "{\"collS\":{\"entries\":[{\"key\":\"a\",\"value\":91}]}}"),
        ("collS { entries(input: { filters: { keys: [\"b\"]} }) { key, value } }", "{\"collS\":{\"entries\":[{\"key\":\"b\",\"value\":null}]}}"),
        ("collS { count }", "{\"collS\":{\"count\":1}}"),
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
