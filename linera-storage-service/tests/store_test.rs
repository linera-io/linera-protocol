// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "storage-service")]

use anyhow::Result;
use linera_storage_service::client::{ServiceStoreClient, ServiceStoreClientInternal};
use linera_views::{
    batch::Batch,
    store::TestKeyValueStore as _,
    test_utils::{
        get_random_byte_vector, get_random_test_scenarios, namespace_admin_test,
        root_key_admin_test, run_reads, run_test_batch_from_blank, run_writes_from_blank,
        run_writes_from_state,
    },
};

#[tokio::test]
async fn test_storage_service_reads() -> Result<()> {
    for scenario in get_random_test_scenarios() {
        let store = ServiceStoreClientInternal::new_test_store().await?;
        run_reads(store, scenario).await;
    }
    Ok(())
}

#[tokio::test]
async fn test_storage_service_writes_from_blank() -> Result<()> {
    let store = ServiceStoreClientInternal::new_test_store().await?;
    run_writes_from_blank(&store).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_writes_from_state() -> Result<()> {
    let store = ServiceStoreClientInternal::new_test_store().await?;
    run_writes_from_state(&store).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_namespace_admin() {
    namespace_admin_test::<ServiceStoreClient>().await;
}

#[tokio::test]
async fn test_storage_service_root_key_admin() {
    root_key_admin_test::<ServiceStoreClient>().await;
}

#[tokio::test]
async fn test_storage_service_big_raw_write() -> Result<()> {
    let store = ServiceStoreClientInternal::new_test_store().await?;
    let n = 5000000;
    let mut rng = linera_views::random::make_deterministic_rng();
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut batch = Batch::new();
    let key_prefix = vec![43];
    batch.put_key_value_bytes(vec![43, 57], vector);
    run_test_batch_from_blank(&store, key_prefix, batch).await;
    Ok(())
}
