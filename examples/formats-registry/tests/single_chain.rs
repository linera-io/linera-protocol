// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Formats Registry application.

#![cfg(not(target_arch = "wasm32"))]

use formats_registry::{formats::FormatsRegistryApplication, FormatsRegistryAbi, Operation};
use linera_sdk::{
    formats::BcsApplication,
    linera_base_types::{AccountOwner, AccountSecretKey, Blob, DataBlobHash, ModuleId},
    test::{QueryOutcome, TestValidator},
};

/// Returns the GraphQL hex form of a `ModuleId`, as expected by the
/// service's `read(moduleId: ...)` query.
fn module_id_to_hex(module_id: &ModuleId) -> String {
    serde_json::to_value(module_id)
        .expect("ModuleId serializes to a JSON string")
        .as_str()
        .expect("ModuleId serializes to a JSON string")
        .to_owned()
}

/// Builds a data blob holding the BCS-encoded `Formats` of this application, which is
/// what the contract now requires every registered blob to deserialize to. Returns
/// the blob and its raw bytes.
fn formats_blob() -> (Blob, Vec<u8>) {
    let value = linera_sdk::bcs::to_bytes(
        &FormatsRegistryApplication::formats().expect("formats trace should succeed"),
    )
    .expect("Formats should serialize as BCS");
    let blob = Blob::new_data(value.clone());
    (blob, value)
}

/// Writes a value for a `ModuleId` and reads it back through the service.
#[tokio::test(flavor = "multi_thread")]
async fn write_then_read() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;
    let owner = AccountOwner::from(chain.public_key());
    let application_id = chain.create_application(module_id, (), (), vec![]).await;
    let module_id = module_id.forget_abi();

    let (blob, value) = formats_blob();
    let blob_hash = DataBlobHash(blob.id().hash);

    // Publish the formats blob in its own block first, so it is committed before the
    // `Write` operation reads and validates it.
    chain
        .add_block_with_blobs(
            |block| {
                block.with_data_blob(&blob);
            },
            vec![blob.clone()],
        )
        .await;

    chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Write {
                    owner,
                    module_id,
                    blob_hash,
                },
            );
        })
        .await;

    let module_id_hex = module_id_to_hex(&module_id);
    let query = format!(r#"query {{ read(moduleId: "{module_id_hex}") }}"#);
    let QueryOutcome { response, .. } = chain.graphql_query(application_id, &*query).await;

    let bytes: Vec<u8> = response["read"]
        .as_array()
        .expect("read must return an array")
        .iter()
        .map(|v| v.as_u64().expect("byte") as u8)
        .collect();
    assert_eq!(bytes, value);
}

/// Querying a `ModuleId` that has never been written returns `null`.
#[tokio::test(flavor = "multi_thread")]
async fn read_unknown_module_returns_null() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;
    let application_id = chain.create_application(module_id, (), (), vec![]).await;
    let module_id = module_id.forget_abi();

    let module_id_hex = module_id_to_hex(&module_id);
    let query = format!(r#"query {{ read(moduleId: "{module_id_hex}") }}"#);
    let QueryOutcome { response, .. } = chain.graphql_query(application_id, &*query).await;

    assert!(response["read"].is_null());
}

/// Writing twice for the same `ModuleId` is rejected (entries are immutable).
#[tokio::test(flavor = "multi_thread")]
async fn second_write_is_rejected() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;
    let owner = AccountOwner::from(chain.public_key());
    let application_id = chain.create_application(module_id, (), (), vec![]).await;
    let module_id = module_id.forget_abi();

    let (blob, _value) = formats_blob();
    let blob_hash = DataBlobHash(blob.id().hash);
    chain
        .add_block_with_blobs(
            |block| {
                block.with_data_blob(&blob);
            },
            vec![blob.clone()],
        )
        .await;
    chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Write {
                    owner,
                    module_id,
                    blob_hash,
                },
            );
        })
        .await;

    // The data blob is already in storage, so the second write needs no new blob; it
    // must still be rejected because the module is already registered.
    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Write {
                    owner,
                    module_id,
                    blob_hash,
                },
            );
        })
        .await;
    assert!(
        result.is_err(),
        "second Write for the same ModuleId must be rejected"
    );
}

/// Once an admin set is configured, writes are gated on admin membership — even on
/// the creation chain. An admin can write; a non-admin signer cannot.
#[tokio::test(flavor = "multi_thread")]
async fn admin_policy_gates_local_writes() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;
    let owner = AccountOwner::from(chain.public_key());
    let application_id = chain.create_application(module_id, (), (), vec![]).await;
    let module_id = module_id.forget_abi();

    // Restrict admins to a third party that is not the chain's signer.
    let other_admin = AccountOwner::from(AccountSecretKey::generate().public());
    chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::SetAdmins {
                    owner,
                    admins: Some(vec![other_admin]),
                },
            );
        })
        .await;

    let QueryOutcome { response, .. } = chain
        .graphql_query(application_id, "query { admins }")
        .await;
    let admins = response["admins"]
        .as_array()
        .expect("admins must be an array");
    assert_eq!(admins.len(), 1);

    // Publish a valid formats blob so the write gets past the operation-side blob
    // validation and is rejected specifically by the admin policy.
    let (blob, _value) = formats_blob();
    let blob_hash = DataBlobHash(blob.id().hash);
    chain
        .add_block_with_blobs(
            |block| {
                block.with_data_blob(&blob);
            },
            vec![blob.clone()],
        )
        .await;

    // The chain's signer is no longer an admin, so its write is rejected by the admin
    // policy applied on the creation chain.
    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Write {
                    owner,
                    module_id,
                    blob_hash,
                },
            );
        })
        .await;
    assert!(
        result.is_err(),
        "a non-admin signer must not be able to write once admins are set"
    );
}
