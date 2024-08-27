// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "storage-service")]

use anyhow::Result;
use linera_storage_service::{
    client::{create_service_test_store, service_config_from_endpoint, ServiceStoreClient},
    storage_service_test_endpoint,
};
use linera_views::{
    batch::Batch,
    test_utils,
    test_utils::{
        admin_test, get_random_byte_vector, get_random_test_scenarios, run_reads,
        run_test_batch_from_blank, run_writes_from_blank, run_writes_from_state,
    },
};

#[tokio::test]
async fn test_storage_service_reads() -> Result<()> {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_service_test_store().await?;
        run_reads(key_value_store, scenario).await;
    }
    Ok(())
}

#[tokio::test]
async fn test_storage_service_writes_from_blank() -> Result<()> {
    let key_value_store = create_service_test_store().await?;
    run_writes_from_blank(&key_value_store).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_writes_from_state() -> Result<()> {
    let key_value_store = create_service_test_store().await?;
    run_writes_from_state(&key_value_store).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_admin() -> Result<()> {
    let config = service_config_from_endpoint()?;
    admin_test::<ServiceStoreClient>(&config).await;
    Ok(())
}

#[tokio::test]
async fn test_storage_service_big_raw_write() -> Result<()> {
    let key_value_store = create_service_test_store().await?;
    let n = 5000000;
    let mut rng = test_utils::make_deterministic_rng();
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut batch = Batch::new();
    let key_prefix = vec![43];
    batch.put_key_value_bytes(vec![43, 57], vector);
    run_test_batch_from_blank(&key_value_store, key_prefix, batch).await;
    Ok(())
}
