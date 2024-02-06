// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::common::AdminKeyValueStore;
use crate::dynamo_db::{
    create_dynamo_db_test_config,
    DynamoDbStore,
};

/// Tests if the table for the storage is created when needed.
#[tokio::test]
async fn table_is_created() {
    let	config = create_dynamo_db_test_config().await;
    DynamoDbStore::delete_all(&config).await.expect("successful cleaning");
    let namespace = "lineratableiscreated".to_string();
    let initial_tables = DynamoDbStore::list_all(&config).await.expect("initial_tables");
    assert!(!initial_tables.contains(&namespace));

    DynamoDbStore::create(&config, &namespace).await.expect("to create a table");

    let tables = DynamoDbStore::list_all(&config).await.expect("tables");
    assert!(tables.contains(&namespace));
}

/// Tests if two independent tables for two separate storages are created.
#[tokio::test]
async fn separate_tables_are_created() {
    let	config = create_dynamo_db_test_config().await;
    DynamoDbStore::delete_all(&config).await.expect("successful cleaning");
    let namespace1 = "first".to_string();
    let namespace2 = "second".to_string();

    let initial_tables = DynamoDbStore::list_all(&config).await.expect("initial_tables");
    assert!(!initial_tables.contains(&namespace1));
    assert!(!initial_tables.contains(&namespace2));

    DynamoDbStore::create(&config, &namespace1).await.expect("to create namespace1");
    DynamoDbStore::create(&config, &namespace2).await.expect("to create namespace2");

    let tables = DynamoDbStore::list_all(&config).await.expect("tables");
    assert!(tables.contains(&namespace1));
    assert!(tables.contains(&namespace2));
}
