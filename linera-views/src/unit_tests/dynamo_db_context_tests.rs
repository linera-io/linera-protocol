use super::{DynamoDbContext, TableName, TableStatus};
use crate::test_utils::{list_tables, LocalStackTestContext};
use anyhow::Error;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Test if the table for the storage is created when needed.
#[tokio::test]
#[ignore]
async fn table_is_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_dynamodb::Client::from_conf(localstack.dynamo_db_config());
    let table: TableName = "linera".parse().expect("Invalid table name");

    let initial_tables = list_tables(&client).await?;
    assert!(!initial_tables.contains(table.as_ref()));

    let (_storage, table_status) = DynamoDbContext::from_config(
        localstack.dynamo_db_config(),
        table.clone(),
        dummy_lock().await,
        vec![],
    )
    .await?;

    let tables = list_tables(&client).await?;
    assert!(tables.contains(table.as_ref()));
    assert_eq!(table_status, TableStatus::New);

    Ok(())
}

/// Test if two independent tables for two separate storages are created.
#[tokio::test]
#[ignore]
async fn separate_tables_are_created() -> Result<(), Error> {
    let localstack = LocalStackTestContext::new().await?;
    let client = aws_sdk_dynamodb::Client::from_conf(localstack.dynamo_db_config());
    let first_table: TableName = "first".parse().expect("Invalid table name");
    let second_table: TableName = "second".parse().expect("Invalid table name");

    let initial_tables = list_tables(&client).await?;
    assert!(!initial_tables.contains(first_table.as_ref()));
    assert!(!initial_tables.contains(second_table.as_ref()));

    let (_storage, first_table_status) = DynamoDbContext::from_config(
        localstack.dynamo_db_config(),
        first_table.clone(),
        dummy_lock().await,
        vec![],
    )
    .await?;
    let (_storage, second_table_status) = DynamoDbContext::from_config(
        localstack.dynamo_db_config(),
        second_table.clone(),
        dummy_lock().await,
        vec![],
    )
    .await?;

    let tables = list_tables(&client).await?;
    assert!(tables.contains(first_table.as_ref()));
    assert!(tables.contains(second_table.as_ref()));
    assert_eq!(first_table_status, TableStatus::New);
    assert_eq!(second_table_status, TableStatus::New);

    Ok(())
}

/// Create a dummy [`OwnedMutexGuard`].
async fn dummy_lock() -> OwnedMutexGuard<()> {
    let dummy_mutex = Arc::new(Mutex::new(()));

    dummy_mutex.lock_owned().await
}
