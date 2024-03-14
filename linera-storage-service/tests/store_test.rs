// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage_service::{
    child::{get_free_port, StorageService},
    client::{create_service_test_store, service_config_from_endpoint, ServiceStoreClient},
};
use linera_views::test_utils::{
    admin_test, get_random_test_scenarios, run_reads, run_writes_from_blank, run_writes_from_state,
};
use linera_views::batch::Batch;
use linera_views::test_utils;
use linera_views::test_utils::get_random_byte_vector;
use linera_views::test_utils::run_test_batch_from_blank;

/// The endpoint used for the storage service tests.
#[cfg(test)]
fn get_storage_service_guard(endpoint: &str) -> StorageService {
    let binary = env!("CARGO_BIN_EXE_storage_service_server").to_string();
    StorageService::new(endpoint, binary)
}

#[tokio::test]
async fn test_reads_service_store() {
    let endpoint = get_free_port().await.unwrap();
    for scenario in get_random_test_scenarios() {
        let _guard = get_storage_service_guard(&endpoint).run().await;
        let key_value_store = create_service_test_store(&endpoint).await.unwrap();
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_service_store_writes_from_blank() {
    let endpoint = get_free_port().await.unwrap();
    let _guard = get_storage_service_guard(&endpoint).run().await;
    let key_value_store = create_service_test_store(&endpoint).await.unwrap();
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_service_store_writes_from_state() {
    let endpoint = get_free_port().await.unwrap();
    let _guard = get_storage_service_guard(&endpoint).run().await;
    let key_value_store = create_service_test_store(&endpoint).await.unwrap();
    run_writes_from_state(&key_value_store).await;
}

#[tokio::test]
async fn test_service_admin() {
    let endpoint = get_free_port().await.unwrap();
    let _guard = get_storage_service_guard(&endpoint).run().await;
    let config = service_config_from_endpoint(&endpoint).expect("config");
    admin_test::<ServiceStoreClient>(&config).await;
}

#[tokio::test]
async fn test_service_big_raw_write() {
    let endpoint = get_free_port().await.unwrap();
    let _guard = get_storage_service_guard(&endpoint).run_service().await;
    let key_value_store = create_service_test_store(&endpoint).await.unwrap();
    let n = 5000000;
    let mut rng = test_utils::make_deterministic_rng();
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut batch = Batch::new();
    let key_prefix = vec![43];
    batch.put_key_value_bytes(vec![43, 57], vector);
    run_test_batch_from_blank(&key_value_store, key_prefix, batch).await;
}
