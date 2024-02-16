// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage_service::child::StorageServiceSpanner;
use linera_views::test_utils::{
    get_random_test_scenarios, run_reads, run_writes_from_blank, run_writes_from_state,
};

use linera_storage_service::client::create_service_test_store;

/// The endpoint used for the storage service tests.
#[cfg(test)]
fn get_storage_service_guard(endpoint: String) -> StorageServiceSpanner {
    let binary = env!("CARGO_BIN_EXE_storage_service_server").to_string();
    println!("binary={}", binary);
    StorageServiceSpanner::new(endpoint, binary)
}

#[tokio::test]
async fn print_binary_names() {
    let binary = env!("CARGO_BIN_EXE_storage_service_server").to_string();
    println!("binary={}", binary);
//    let path = util::resolve_binary("storage_service_server", env!("CARGO_PKG_NAME")).await.expec("path");
//    println!("path={}", path);
}




#[tokio::test]
async fn test_reads_service_store() {
    let endpoint = "127.0.0.1:8942".to_string();
    for scenario in get_random_test_scenarios() {
        let _guard = get_storage_service_guard(endpoint.clone())
            .run_service()
            .await;
        let key_value_store = create_service_test_store(endpoint.clone()).await.unwrap();
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_service_store_writes_from_blank() {
    let endpoint = "127.0.0.1:8943".to_string();
    let _guard = get_storage_service_guard(endpoint.clone())
        .run_service()
        .await;
    let key_value_store = create_service_test_store(endpoint).await.unwrap();
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_service_store_writes_from_state() {
    let endpoint = "127.0.0.1:8944".to_string();
    let _guard = get_storage_service_guard(endpoint.clone())
        .run_service()
        .await;
    let key_value_store = create_service_test_store(endpoint).await.unwrap();
    run_writes_from_state(&key_value_store).await;
}
