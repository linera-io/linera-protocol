// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage_service::child::StorageServiceChild;

use linera_views::{batch::Batch, common::WritableKeyValueStore};

use linera_views::test_utils::{
    get_random_test_scenarios, run_reads, run_writes_from_blank, run_writes_from_state,
};

use linera_storage_service::client::create_shared_test_store;

/// The endpoint used for the storage service tests.
const STORAGE_SERVICE_ENDPOINT: &str = "127.0.0.1:8942";

#[cfg(test)]
async fn clean_storage() {
    let key_value_store = create_shared_test_store().await;
    let mut batch = Batch::new();
    batch.delete_key_prefix(vec![]);
    key_value_store.write_batch(batch, &[]).await.unwrap();
}

#[tokio::test]
async fn test_reads_shared_store() {
    let _guard = StorageServiceChild::new(STORAGE_SERVICE_ENDPOINT.to_string())
        .run_service()
        .await;
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_shared_test_store().await;
        run_reads(key_value_store, scenario).await;
        clean_storage().await;
    }
}

#[tokio::test]
async fn test_shared_store_writes_from_blank() {
    let _guard = StorageServiceChild::new(STORAGE_SERVICE_ENDPOINT.to_string())
        .run_service()
        .await;
    let key_value_store = create_shared_test_store().await;
    run_writes_from_blank(&key_value_store).await;
    clean_storage().await;
}

#[tokio::test]
async fn test_shared_store_writes_from_state() {
    let _guard = StorageServiceChild::new(STORAGE_SERVICE_ENDPOINT.to_string())
        .run_service()
        .await;
    let key_value_store = create_shared_test_store().await;
    run_writes_from_state(&key_value_store).await;
    clean_storage().await;
}
