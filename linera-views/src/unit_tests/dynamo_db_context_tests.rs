// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{DynamoDbContext, TableName, TableStatus};
use crate::{
    dynamo_db::{
        list_tables, DynamoDbContextError, LocalStackTestContext,
        TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES, TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
    },
    lru_caching::TEST_CACHE_SIZE,
};
use anyhow::Error;

async fn get_table_status(
    localstack: &LocalStackTestContext,
    table: &TableName,
) -> Result<TableStatus, DynamoDbContextError> {
    let (_storage, table_status) = DynamoDbContext::from_config(
        localstack.dynamo_db_config(),
        table.clone(),
        Some(TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES),
        TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
        TEST_CACHE_SIZE,
        vec![],
        (),
    )
    .await?;
    Ok(table_status)
}

/// Tests if the table for the storage is created when needed.
#[tokio::test]
async fn table_is_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_dynamodb::Client::from_conf(localstack.dynamo_db_config());
    let table: TableName = "linera".parse().expect("Invalid table name");

    let initial_tables = list_tables(&client).await?;
    assert!(!initial_tables.contains(table.as_ref()));

    let table_status = get_table_status(&localstack, &table).await?;

    let tables = list_tables(&client).await?;
    assert!(tables.contains(table.as_ref()));
    assert_eq!(table_status, TableStatus::New);

    Ok(())
}

/// Tests if two independent tables for two separate storages are created.
#[tokio::test]
async fn separate_tables_are_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_dynamodb::Client::from_conf(localstack.dynamo_db_config());
    let first_table: TableName = "first".parse().expect("Invalid table name");
    let second_table: TableName = "second".parse().expect("Invalid table name");

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
