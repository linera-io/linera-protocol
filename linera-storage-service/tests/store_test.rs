// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// The store created by the create_shared_test_store
/// are all pointing to the same storage.
/// This is in contrast to other storage that are not
/// persistent (e.g. memory) or uses a random table_name
/// (e.g. rocksDb, DynamoDb, ScyllaDb)
/// This requires two changes:
/// * Only one test being run at a time with a semaphore.
/// * After the test, the storage is cleaned

pub static SHARED_STORE_SEMAPHORE: Semaphore = Semaphore::const_new(1);

use linera_views::{batch::Batch, common::WritableKeyValueStore};
use tokio::sync::Semaphore;

use linera_views::test_utils::{
    get_random_test_scenarios, run_reads, run_writes_from_blank, run_writes_from_state,
};

use linera_shared_storage::shared_store_client::create_shared_test_store;

#[cfg(test)]
async fn clean_storage() {
    let key_value_store = create_shared_test_store().await;
    let mut batch = Batch::new();
    batch.delete_key_prefix(vec![]);
    key_value_store.write_batch(batch, &[]).await.unwrap();
}

#[tokio::test]
async fn test_reads_shared_store() {
    let _lock = SHARED_STORE_SEMAPHORE.acquire().await;
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_shared_test_store().await;
        run_reads(key_value_store, scenario).await;
        clean_storage().await;
    }
}

#[tokio::test]
async fn test_shared_store_writes_from_blank() {
    let _lock = SHARED_STORE_SEMAPHORE.acquire().await;
    let key_value_store = create_shared_test_store().await;
    run_writes_from_blank(&key_value_store).await;
    clean_storage().await;
}

#[tokio::test]
async fn test_shared_store_writes_from_state() {
    let _lock = SHARED_STORE_SEMAPHORE.acquire().await;
    let key_value_store = create_shared_test_store().await;
    run_writes_from_state(&key_value_store).await;
    clean_storage().await;
}
