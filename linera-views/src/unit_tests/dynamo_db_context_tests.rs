// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{DynamoDbContext, TableName, TableStatus};
use crate::dynamo_db::{
    clear_tables, create_dynamo_db_common_config, list_tables, DynamoDbContextError,
    DynamoDbKvStoreConfig, LocalStackTestContext,
};
use anyhow::Error;

async fn get_table_status(
    localstack: &LocalStackTestContext,
    table_name: &TableName,
) -> Result<TableStatus, DynamoDbContextError> {
    let common_config = create_dynamo_db_common_config();
    let store_config = DynamoDbKvStoreConfig {
        config: localstack.dynamo_db_config(),
        table_name: table_name.clone(),
        common_config,
    };
    let (_storage, table_status) =
        DynamoDbContext::new_for_testing(store_config, vec![], ()).await?;
    Ok(table_status)
}

/// Tests if the table for the storage is created when needed.
#[tokio::test]
async fn table_is_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_dynamodb::Client::from_conf(localstack.dynamo_db_config());
    clear_tables(&client).await?;
    let table_name = "lineratableiscreated"
        .parse::<TableName>()
        .expect("Invalid table name");

    let initial_tables = list_tables(&client).await?;
    assert!(!initial_tables.contains(table_name.as_ref()));

    let table_status = get_table_status(&localstack, &table_name).await?;

    let tables = list_tables(&client).await?;
    assert!(tables.contains(table_name.as_ref()));
    assert_eq!(table_status, TableStatus::New);

    Ok(())
}

/// Tests if two independent tables for two separate storages are created.
#[tokio::test]
async fn separate_tables_are_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_dynamodb::Client::from_conf(localstack.dynamo_db_config());
    clear_tables(&client).await?;
    let first_table = "first".parse::<TableName>().expect("Invalid table name");
    let second_table = "second".parse::<TableName>().expect("Invalid table name");

    let initial_tables = list_tables(&client).await?;
    assert!(!initial_tables.contains(first_table.as_ref()));
    assert!(!initial_tables.contains(second_table.as_ref()));

    let first_table_status = get_table_status(&localstack, &first_table).await?;
    let second_table_status = get_table_status(&localstack, &second_table).await?;

    let tables = list_tables(&client).await?;
    assert!(tables.contains(first_table.as_ref()));
    assert!(tables.contains(second_table.as_ref()));
    assert_eq!(first_table_status, TableStatus::New);
    assert_eq!(second_table_status, TableStatus::New);

    Ok(())
}
