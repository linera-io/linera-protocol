// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "rocksdb", feature = "dynamodb", feature = "scylladb"))]

#[cfg(feature = "dynamodb")]
use linera_views::dynamo_db::{create_dynamo_db_test_config, DynamoDbStore};
#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::{create_rocks_db_test_config, RocksDbStore};
#[cfg(feature = "scylladb")]
use linera_views::scylla_db::{create_scylla_db_test_config, ScyllaDbStore};
use linera_views::test_utils::admin_test;

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn admin_test_rocks_db() {
    let (config, _dir) = create_rocks_db_test_config().await;
    admin_test::<RocksDbStore>(&config).await;
}

#[cfg(feature = "dynamodb")]
#[tokio::test]
async fn admin_test_dynamo_db() {
    let config = create_dynamo_db_test_config().await;
    admin_test::<DynamoDbStore>(&config).await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn admin_test_scylla_db() {
    let config = create_scylla_db_test_config().await;
    admin_test::<ScyllaDbStore>(&config).await;
}
