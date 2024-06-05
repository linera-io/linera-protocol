// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage_service::{
    client::{create_service_test_store, service_config_from_endpoint, ServiceStoreClient},
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
async fn test_reads_service_store() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_service_test_store("127.0.0.1:1235").await.unwrap();
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_service_store_writes_from_blank() {
    let key_value_store = create_service_test_store("127.0.0.1:1235").await.unwrap();
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_service_store_writes_from_state() {
    let key_value_store = create_service_test_store("127.0.0.1:1235").await.unwrap();
    run_writes_from_state(&key_value_store).await;
}

#[tokio::test]
async fn test_service_admin() {
    let config = service_config_from_endpoint("127.0.0.1:1235").expect("config");
    admin_test::<ServiceStoreClient>(&config).await;
}

#[tokio::test]
async fn test_service_big_raw_write() {
    let key_value_store = create_service_test_store("127.0.0.1:1235").await.unwrap();
    let n = 5000000;
    let mut rng = test_utils::make_deterministic_rng();
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut batch = Batch::new();
    let key_prefix = vec![43];
    batch.put_key_value_bytes(vec![43, 57], vector);
    run_test_batch_from_blank(&key_value_store, key_prefix, batch).await;
}
