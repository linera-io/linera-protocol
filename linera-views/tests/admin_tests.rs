// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "dynamodb")]
use linera_views::dynamo_db::DynamoDbStore;
#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::RocksDbStore;
#[cfg(feature = "scylladb")]
use linera_views::scylla_db::ScyllaDbStore;
use linera_views::{
    memory::MemoryStore,
    test_utils::admin_test,
};

#[tokio::test]
async fn admin_test_memory() {
    admin_test::<MemoryStore>().await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn admin_test_rocks_db() {
    admin_test::<RocksDbStore>().await;
}

#[cfg(feature = "dynamodb")]
#[tokio::test]
async fn admin_test_dynamo_db() {
    admin_test::<DynamoDbStore>().await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn admin_test_scylla_db() {
    admin_test::<ScyllaDbStore>().await;
}
