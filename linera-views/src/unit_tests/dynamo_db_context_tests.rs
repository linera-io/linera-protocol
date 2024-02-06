// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{DynamoDbContext, TableStatus};
use crate::dynamo_db::{
    clear_tables, create_dynamo_db_common_config, list_tables_from_client, DynamoDbContextError,
    DynamoDbStoreConfig, LocalStackTestContext,
};
use anyhow::Error;

async fn get_table_status(
    localstack: &LocalStackTestContext,
    namespace: &str,
) -> Result<TableStatus, DynamoDbContextError> {
    let common_config = create_dynamo_db_common_config();
    let store_config = DynamoDbStoreConfig {
        config: localstack.dynamo_db_config(),
        common_config,
    };
    let (_storage, table_status) =
        DynamoDbContext::new_for_testing(store_config, namespace, vec![], ()).await?;
    Ok(table_status)
}

/// Tests if the table for the storage is created when needed.
#[tokio::test]
async fn table_is_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_dynamodb::Client::from_conf(localstack.dynamo_db_config());
    clear_tables(&client).await?;
    let namespace = "lineratableiscreated".to_string();
    let initial_tables = list_tables_from_client(&client).await?;
    assert!(!initial_tables.contains(&namespace));

    let table_status = get_table_status(&localstack, &namespace).await?;

    let tables = list_tables_from_client(&client).await?;
    assert!(tables.contains(&namespace));
    assert_eq!(table_status, TableStatus::New);

    Ok(())
}

/// Tests if two independent tables for two separate storages are created.
#[tokio::test]
async fn separate_tables_are_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_dynamodb::Client::from_conf(localstack.dynamo_db_config());
    clear_tables(&client).await?;
    let namespace1 = "first".to_string();
    let namespace2 = "second".to_string();

    let initial_tables = list_tables_from_client(&client).await?;
    assert!(!initial_tables.contains(&namespace1));
    assert!(!initial_tables.contains(&namespace2));

    let first_table_status = get_table_status(&localstack, &namespace1).await?;
    let second_table_status = get_table_status(&localstack, &namespace2).await?;

    let tables = list_tables_from_client(&client).await?;
    assert!(tables.contains(&namespace1));
    assert!(tables.contains(&namespace2));
    assert_eq!(first_table_status, TableStatus::New);
    assert_eq!(second_table_status, TableStatus::New);

    Ok(())
}
