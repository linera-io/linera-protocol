// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(not(target_arch = "wasm32"))]

use event_emitter::EventEmitterAbi;
use event_subscriber::Operation;
use linera_sdk::test::{QueryOutcome, TestValidator};

#[tokio::test]
async fn test_cross_chain_event_subscription() {
    let validator = TestValidator::new().await;
    let publisher = validator.new_chain().await;

    let emitter_module_id = publisher
        .publish_bytecode_files_in::<EventEmitterAbi, (), ()>("../event-emitter")
        .await;
    let subscriber_module_id = publisher
        .publish_bytecode_files_in::<event_subscriber::EventSubscriberAbi, (), ()>(".")
        .await;

    let mut emitter_chain = validator.new_chain().await;
    let emitter_app_id = emitter_chain
        .create_application(emitter_module_id, (), (), vec![])
        .await;

    let mut subscriber_chain = validator.new_chain().await;
    let subscriber_app_id = subscriber_chain
        .create_application(subscriber_module_id, (), (), vec![])
        .await;

    subscriber_chain
        .add_block(|block| {
            block.with_operation(
                subscriber_app_id,
                Operation::Subscribe {
                    chain_id: emitter_chain.id(),
                    application_id: emitter_app_id.forget_abi(),
                    stream_name: "my-stream".to_string(),
                },
            );
        })
        .await;

    emitter_chain
        .add_block(|block| {
            block.with_operation(
                emitter_app_id,
                event_emitter::Operation::Emit {
                    stream_name: "my-stream".to_string(),
                    value: "hello from emitter".to_string(),
                },
            );
        })
        .await;

    subscriber_chain.handle_new_events().await;

    let query = "query { receivedEvents { entries(start: 0, end: 1) } }";
    let QueryOutcome { response, .. } = subscriber_chain
        .graphql_query(subscriber_app_id, query)
        .await;
    let value = response["receivedEvents"]["entries"][0].clone();
    assert_eq!(value, "hello from emitter");
}
