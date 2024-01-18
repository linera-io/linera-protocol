// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::test_utils::{
    get_random_test_scenarios, run_reads, run_writes_from_blank, run_writes_from_state,
};

use linera_shared_storage::shared_store_client::create_shared_test_store;

#[tokio::test]
async fn test_reads_shared_store() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_shared_test_store().await;
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_shared_store_writes_from_blank() {
    let key_value_store = create_shared_test_store().await;
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_shared_store_writes_from_state() {
    let key_value_store = create_shared_test_store().await;
    run_writes_from_state(&key_value_store).await;
}
