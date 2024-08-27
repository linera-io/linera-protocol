// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "dynamodb")]
use linera_views::dynamo_db::DynamoDbStore;
#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::RocksDbStore;
#[cfg(feature = "scylladb")]
use linera_views::scylla_db::{create_scylla_db_test_config, ScyllaDbStore};
use linera_views::{
    memory::MemoryStore,
    common::{AdminKeyValueStore as _},
    test_utils::admin_test,
};

#[tokio::test]
async fn admin_test_memory() {
    let config = MemoryStore::get_test_config().await.expect("config");
    admin_test::<MemoryStore>(&config).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn admin_test_rocks_db() {
    let config = RocksDbStore::get_test_config().await.expect("config");
    admin_test::<RocksDbStore>(&config).await;
}

#[cfg(feature = "dynamodb")]
#[tokio::test]
async fn admin_test_dynamo_db() {
    let config = DynamoDbStore::get_test_config().await.expect("config");
    admin_test::<DynamoDbStore>(&config).await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn admin_test_scylla_db() {
    let config = create_scylla_db_test_config().await;
    admin_test::<ScyllaDbStore>(&config).await;
}
