// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage_service::child::StorageServiceSpanner;
use linera_views::test_utils::{
    get_random_test_scenarios, run_reads, run_writes_from_blank, run_writes_from_state,
};

use linera_storage_service::client::create_shared_test_store;

/// The endpoint used for the storage service tests.
#[cfg(test)]
fn get_storage_service_guard(port: u16) -> StorageServiceSpanner {
    let endpoint = format!("127.0.0.1:{}", port);
    let binary = env!("CARGO_BIN_EXE_storage_service_server").to_string();
    StorageServiceSpanner::new(endpoint, binary)
}

#[tokio::test]
async fn test_reads_shared_store() {
    let port = 8942;
    for scenario in get_random_test_scenarios() {
        let _guard = get_storage_service_guard(port).run_service().await;
        let key_value_store = create_shared_test_store(port).await;
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_shared_store_writes_from_blank() {
    let port = 8943;
    let _guard = get_storage_service_guard(port).run_service().await;
    let key_value_store = create_shared_test_store(port).await;
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_shared_store_writes_from_state() {
    let port = 8944;
    let _guard = get_storage_service_guard(port).run_service().await;
    let key_value_store = create_shared_test_store(port).await;
    run_writes_from_state(&key_value_store).await;
}
