// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_dynamodb)]
use linera_views::dynamo_db::DynamoDbStore;
#[cfg(with_rocksdb)]
use linera_views::rocks_db::RocksDbStore;
#[cfg(with_scylladb)]
use linera_views::scylla_db::ScyllaDbStore;
use linera_views::{memory::MemoryStore, test_utils::{namespace_admin_test, root_key_admin_test}};

#[tokio::test]
async fn namespace_admin_test_memory() {
    namespace_admin_test::<MemoryStore>().await;
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn namespace_admin_test_rocks_db() {
    namespace_admin_test::<RocksDbStore>().await;
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn namespace_admin_test_dynamo_db() {
    namespace_admin_test::<DynamoDbStore>().await;
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn namespace_admin_test_scylla_db() {
    namespace_admin_test::<ScyllaDbStore>().await;
}

#[tokio::test]
async fn root_key_admin_test_memory() {
    root_key_admin_test::<MemoryStore>().await;
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn root_key_admin_test_rocks_db() {
    root_key_admin_test::<RocksDbStore>().await;
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn root_key_admin_test_dynamo_db() {
    root_key_admin_test::<DynamoDbStore>().await;
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn root_key_admin_test_scylla_db() {
    root_key_admin_test::<ScyllaDbStore>().await;
}
