// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Matching Engine application

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::{
    test::{QueryOutcome, TestValidator},
};
use complex_data_contract::{ComplexDataAbi, ComplexDataOperation};


#[tokio::test]
async fn test_queries() {
    let (validator, module_id) =
        TestValidator::with_current_module::<ComplexDataAbi, (), ()>().await;

    let mut chain = validator.new_chain().await;

    let application_id = chain
        .create_application(module_id, (), (), vec![])
        .await;


    let operation = ComplexDataOperation::InsertField4 { key1: "Bonjour".into(), key2: "A bientot".into(), value: 49 };

    chain
        .add_block(|block| {
            block.with_operation(application_id, operation);
        })
        .await;


    // READ1

    let query1 = "field4 { keys }";
    let query2 = "field4 { entries { key, value { count } } }";
    let query3 = "field4 { count }";
    let query4 = "field4 { entry(key: \"Bonjour\") { key, value { count } } }";
    let query5 = "field4 { entry(key: \"Bonjour\") { key, value { entries(input: {}) { key, value } } } }";
    let query6 = "field4 { entries(input: {}) { key, value { count } } }";
    let query7 = "field4 { entries(input: {}) { key, value { entries(input: {}) { key, value } } } }";

    let queries = [query1, query2, query3, query4, query5, query6, query7];

    for query in queries {
        println!("query={}", query);
        let new_query = format!("query {{ {} }}", query);
        let QueryOutcome { response, .. } = chain.graphql_query(application_id, new_query).await;
        println!("response={response}");
        println!();
    }
}
