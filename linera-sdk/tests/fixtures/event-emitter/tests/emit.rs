// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(not(target_arch = "wasm32"))]

use event_emitter::Operation;
use linera_sdk::test::{QueryOutcome, TestValidator};

#[tokio::test]
async fn test_emit_event() {
    let (validator, module_id) =
        TestValidator::with_current_module::<event_emitter::EventEmitterAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;
    let application_id = chain.create_application(module_id, (), (), vec![]).await;

    chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Emit {
                    stream_name: "test-stream".to_string(),
                    value: "test event".to_string(),
                },
            );
        })
        .await;

    let query = "query { emittedEvents { entries(start: 0, end: 1) } }";
    let QueryOutcome { response, .. } = chain.graphql_query(application_id, query).await;
    let value = response["emittedEvents"]["entries"][0].clone();
    assert_eq!(value, "test event");
}
