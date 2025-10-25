// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "storage-service")]

use anyhow::Result;
use linera_storage_service::{
    client::{StorageServiceDatabase, StorageServiceDatabaseInternal},
};
use linera_views::{
    batch::Batch,
    random::generate_test_namespace,
    store::{TestKeyValueDatabase as _, KeyValueDatabase as _, ReadableKeyValueStore as _, WritableKeyValueStore as _},
    test_utils::{
        get_random_byte_vector, get_random_test_scenarios, namespace_admin_test,
        root_key_admin_test, run_reads, run_test_batch_from_blank, run_writes_from_blank,
        run_writes_from_state,
    },
};

#[tokio::test]
async fn test_storage_service_reads() -> Result<()> {
    for scenario in get_random_test_scenarios() {
        let store = StorageServiceDatabaseInternal::new_test_store().await?;
        run_reads(store, scenario).await;
    }
    Ok(())
}

#[tokio::test]
async fn test_storage_service_writes_from_blank() -> Result<()> {
    let store = StorageServiceDatabaseInternal::new_test_store().await?;
    run_writes_from_blank(&store).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_writes_from_state() -> Result<()> {
    let store = StorageServiceDatabaseInternal::new_test_store().await?;
    run_writes_from_state(&store).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_namespace_admin() {
    namespace_admin_test::<StorageServiceDatabaseInternal>().await;
}

#[tokio::test]
async fn test_storage_service_root_key_admin() {
    root_key_admin_test::<StorageServiceDatabaseInternal>().await;
}

#[tokio::test]
async fn test_storage_service_big_raw_write() -> Result<()> {
    let store = StorageServiceDatabaseInternal::new_test_store().await?;
    let n = 5000000;
    let mut rng = linera_views::random::make_deterministic_rng();
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut batch = Batch::new();
    let key_prefix = vec![43];
    batch.put_key_value_bytes(vec![43, 57], vector);
    run_test_batch_from_blank(&store, key_prefix, batch).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_open_shared() -> Result<()> {
    let config = StorageServiceDatabase::new_test_config().await?;
    let namespace = generate_test_namespace();
    let database = StorageServiceDatabase::maybe_create_and_connect(&config, &namespace).await?;
    let store1 = database.open_shared(&[2, 3, 4, 5])?;
    let mut batch = Batch::new();
    batch.put_key_value_bytes(vec![6, 7], vec![123, 135]);
    store1.write_batch(batch).await?;

    let store2 = database.open_shared(&[])?;
    let key_values = store2.find_key_values_by_prefix(&[2]).await?;
    assert_eq!(key_values.len(), 0);
    Ok(())
}

