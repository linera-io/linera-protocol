// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "storage-service")]

use anyhow::Result;
use linera_storage_service::client::StorageServiceDatabaseInternal;
use linera_views::{
    batch::Batch,
    store::{KeyValueDatabase, TestKeyValueDatabase},
    test_utils::{
        get_random_byte_vector, get_random_test_scenarios, namespace_admin_test,
        root_key_admin_test, run_reads, run_test_batch_from_blank, run_writes_from_blank,
        run_writes_from_state,
    },
};

#[tokio::test]
async fn test_storage_service_reads() -> Result<()> {
    for scenario in get_random_test_scenarios() {
        let config = StorageServiceDatabaseInternal::new_test_config().await?;
        let namespace = "test";
        let database = StorageServiceDatabaseInternal::connect(&config, namespace).await?;
        let store = database.open_exclusive(&[])?;
        run_reads(store, scenario).await;
    }
    Ok(())
}

#[tokio::test]
async fn test_storage_service_writes_from_blank() -> Result<()> {
    let config = StorageServiceDatabaseInternal::new_test_config().await?;
    let namespace = "test";
    let database = StorageServiceDatabaseInternal::connect(&config, namespace).await?;
    let store = database.open_exclusive(&[])?;
    run_writes_from_blank(&store).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_writes_from_state() -> Result<()> {
    let config = StorageServiceDatabaseInternal::new_test_config().await?;
    let namespace = "test";
    let database = StorageServiceDatabaseInternal::connect(&config, namespace).await?;
    let store = database.open_exclusive(&[])?;
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
    let config = StorageServiceDatabaseInternal::new_test_config().await?;
    let namespace = "test";
    let database = StorageServiceDatabaseInternal::connect(&config, namespace).await?;
    let store = database.open_exclusive(&[])?;
    let n = 5000000;
    let mut rng = linera_views::random::make_deterministic_rng();
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut batch = Batch::new();
    let key_prefix = vec![43];
    batch.put_key_value_bytes(vec![43, 57], vector);
    run_test_batch_from_blank(&store, key_prefix, batch).await;
    Ok(())
}
