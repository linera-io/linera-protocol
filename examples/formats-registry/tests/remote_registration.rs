// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cross-chain integration tests for remote module registration by admins.

#![cfg(not(target_arch = "wasm32"))]

use formats_registry::{FormatsRegistryAbi, Operation};
use linera_sdk::{
    linera_base_types::{AccountOwner, Blob, DataBlobHash, ModuleId},
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

/// An admin operating from a remote chain can register a module: the write is
/// forwarded to the creation chain and applied there.
#[tokio::test(flavor = "multi_thread")]
async fn remote_admin_can_register() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut creator_chain = validator.new_chain().await;
    let creator_owner = AccountOwner::from(creator_chain.public_key());
    let application_id = creator_chain
        .create_application(module_id, (), (), vec![])
        .await;
    let module_id = module_id.forget_abi();

    let remote_chain = validator.new_chain().await;
    let remote_owner = AccountOwner::from(remote_chain.public_key());

    // The creator authorizes the remote owner as an admin. This is allowed because
    // no admin set has been configured yet and the operation runs locally on the
    // creation chain.
    creator_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::SetAdmins {
                    owner: creator_owner,
                    admins: Some(vec![remote_owner]),
                },
            );
        })
        .await;

    // The remote admin submits a write on its own chain; it publishes the data blob
    // there and the write is forwarded to the creation chain as a cross-chain message.
    let value = vec![9u8, 8, 7];
    let blob = Blob::new_data(value.clone());
    let blob_hash = DataBlobHash(blob.id().hash);
    remote_chain
        .add_block_with_blobs(
            |block| {
                block.with_data_blob(&blob).with_operation(
                    application_id,
                    Operation::Write {
                        owner: remote_owner,
                        module_id,
                        blob_hash,
                    },
                );
            },
            vec![blob.clone()],
        )
        .await;

    // The creation chain processes the forwarded message and stores the value.
    creator_chain.handle_received_messages().await;

    let module_id_hex = module_id_to_hex(&module_id);
    let query = format!(r#"query {{ read(moduleId: "{module_id_hex}") }}"#);
    let QueryOutcome { response, .. } = creator_chain.graphql_query(application_id, &*query).await;
    let bytes: Vec<u8> = response["read"]
        .as_array()
        .expect("read must return an array")
        .iter()
        .map(|v| v.as_u64().expect("byte") as u8)
        .collect();
    assert_eq!(bytes, value);
}

/// A non-admin operating from a remote chain cannot register a module: the
/// forwarded message is rejected when executed on the creation chain.
#[tokio::test(flavor = "multi_thread")]
async fn remote_non_admin_is_rejected() {
    let (validator, module_id) =
        TestValidator::with_current_module::<FormatsRegistryAbi, (), ()>().await;
    let mut creator_chain = validator.new_chain().await;
    let creator_owner = AccountOwner::from(creator_chain.public_key());
    let application_id = creator_chain
        .create_application(module_id, (), (), vec![])
        .await;
    let module_id = module_id.forget_abi();

    let remote_chain = validator.new_chain().await;
    let remote_owner = AccountOwner::from(remote_chain.public_key());

    // Admins are configured, but the remote owner is not among them.
    creator_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::SetAdmins {
                    owner: creator_owner,
                    admins: Some(vec![creator_owner]),
                },
            );
        })
        .await;

    let blob = Blob::new_data(vec![1u8, 2, 3]);
    let blob_hash = DataBlobHash(blob.id().hash);
    let (write_certificate, _) = remote_chain
        .add_block_with_blobs(
            |block| {
                block.with_data_blob(&blob).with_operation(
                    application_id,
                    Operation::Write {
                        owner: remote_owner,
                        module_id,
                        blob_hash,
                    },
                );
            },
            vec![blob.clone()],
        )
        .await;

    // The creation chain refuses to execute the forwarded write from a non-admin.
    let result = creator_chain
        .try_add_block(|block| {
            block.with_messages_from(&write_certificate);
        })
        .await;
    assert!(
        result.is_err(),
        "a non-admin remote write must be rejected on the creation chain"
    );
}
