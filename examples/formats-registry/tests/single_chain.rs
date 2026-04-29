// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Formats Registry application.

#![cfg(not(target_arch = "wasm32"))]

use formats_registry::{FormatsRegistryAbi, Operation};
use linera_sdk::test::{QueryOutcome, TestValidator};

/// Returns the GraphQL hex form of a `ModuleId`, as expected by the
/// service's `get(moduleId: ...)` query.
fn module_id_to_hex(module_id: &linera_sdk::linera_base_types::ModuleId) -> String {
    serde_json::to_value(module_id)
        .expect("ModuleId serializes to a JSON string")
        .as_str()
        .expect("ModuleId serializes to a JSON string")
        .to_owned()
}

/// Writes a value for a `ModuleId` and reads it back through the service.
#[tokio::test(flavor = "multi_thread")]
async fn write_then_get() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;
    let application_id = chain.create_application(module_id, (), (), vec![]).await;
    let module_id = module_id.forget_abi();

    let value = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Write {
                    module_id,
                    value: value.clone(),
                },
            );
        })
        .await;

    let module_id_hex = module_id_to_hex(&module_id);
    let query = format!(r#"query {{ get(moduleId: "{module_id_hex}") }}"#);
    let QueryOutcome { response, .. } = chain.graphql_query(application_id, &*query).await;

    let bytes: Vec<u8> = response["get"]
        .as_array()
        .expect("get must return an array")
        .iter()
        .map(|v| v.as_u64().expect("byte") as u8)
        .collect();
    assert_eq!(bytes, value);
}

/// Querying a `ModuleId` that has never been written returns `null`.
#[tokio::test(flavor = "multi_thread")]
async fn get_unknown_module_returns_null() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;
    let application_id = chain.create_application(module_id, (), (), vec![]).await;
    let module_id = module_id.forget_abi();

    let module_id_hex = module_id_to_hex(&module_id);
    let query = format!(r#"query {{ get(moduleId: "{module_id_hex}") }}"#);
    let QueryOutcome { response, .. } = chain.graphql_query(application_id, &*query).await;

    assert!(response["get"].is_null());
}

/// Writing twice for the same `ModuleId` is rejected (entries are immutable).
#[tokio::test(flavor = "multi_thread")]
async fn second_write_is_rejected() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;
    let application_id = chain.create_application(module_id, (), (), vec![]).await;
    let module_id = module_id.forget_abi();

    chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Write {
                    module_id,
                    value: vec![0xAA],
                },
            );
        })
        .await;

    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Write {
                    module_id,
                    value: vec![0xBB],
                },
            );
        })
        .await;
    assert!(
        result.is_err(),
        "second Write for the same ModuleId must be rejected"
    );
}
