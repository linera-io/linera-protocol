// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::batch::SimplifiedBatch;
use crate::{
    batch::{Batch, DeletePrefixExpander, SimpleUnorderedBatch},
    common::{
        get_uleb128_size, CommonStoreConfig, ContextFromStore, KeyIterable, KeyValueIterable, KeyValueStore,
        ReadableKeyValueStore, TableStatus, WritableKeyValueStore, MIN_VIEW_TAG,
    },
    lru_caching::LruCachingStore,
    value_splitting::{DatabaseConsistencyError, ValueSplittingStore},
};
use async_lock::{Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        batch_write_item::BatchWriteItemError,
        create_table::CreateTableError,
        delete_table::DeleteTableError,
        get_item::GetItemError,
        list_tables::ListTablesError,
        query::{QueryError, QueryOutput},
        transact_write_items::TransactWriteItemsError,
    },
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, Delete, KeySchemaElement, KeyType,
        ProvisionedThroughput, Put, ScalarAttributeType, TransactWriteItem,
    },
    Client,
};
use aws_smithy_types::error::operation::BuildError;
use bcs::serialized_size;
use futures::future::join_all;
use linera_base::ensure;
use serde::{Deserialize, Serialize};
use static_assertions as sa;
use std::{collections::HashMap, env, mem, str::FromStr, sync::Arc};
use thiserror::Error;

#[cfg(any(test, feature = "test"))]
use {
    crate::lru_caching::TEST_CACHE_SIZE,
    crate::test_utils::get_table_name,
    anyhow::Error,
    tokio::sync::{Mutex, MutexGuard},
};

/// Name of the environment variable with the address to a LocalStack instance.
const LOCALSTACK_ENDPOINT: &str = "LOCALSTACK_ENDPOINT";

/// The configuration to connect to DynamoDB.
pub type Config = aws_sdk_dynamodb::Config;

/// Gets the AWS configuration from the environment
pub async fn get_base_config() -> Result<Config, DynamoDbContextError> {
    let base_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    Ok((&base_config).into())
}

fn get_endpoint_address() -> Option<String> {
    let endpoint_address = env::var(LOCALSTACK_ENDPOINT);
    match endpoint_address {
        Err(_) => None,
        Ok(address) => Some(address),
    }
}

/// Gets the localstack config
pub async fn get_localstack_config() -> Result<Config, DynamoDbContextError> {
    let base_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let endpoint_address = get_endpoint_address().unwrap();
    let config = aws_sdk_dynamodb::config::Builder::from(&base_config)
        .endpoint_url(endpoint_address)
        .build();
    Ok(config)
}

/// Getting a configuration for the system
pub async fn get_config(use_localstack: bool) -> Result<Config, DynamoDbContextError> {
    if use_localstack {
        get_localstack_config().await
    } else {
        get_base_config().await
    }
}

/// A type to help tests that need a LocalStack instance.
#[cfg(any(test, feature = "test"))]
pub struct LocalStackTestContext {
    config: Config,
    _guard: MutexGuard<'static, ()>,
}

#[cfg(any(test, feature = "test"))]
impl LocalStackTestContext {
    /// Creates an instance of [`LocalStackTestContext`], loading the necessary LocalStack
    /// configuration.
    ///
    /// An address to the LocalStack instance must be specified using a `LOCALSTACK_ENDPOINT`
    /// environment variable.
    ///
    /// This also locks the `LOCALSTACK_GUARD` to enforce that only one test has access to the
    /// LocalStack instance.
    pub async fn new() -> Result<LocalStackTestContext, Error> {
        let config = get_localstack_config().await?;
        let _guard = LOCALSTACK_GUARD.lock().await;

        let context = LocalStackTestContext { config, _guard };

        Ok(context)
    }

    /// Creates a new [`aws_sdk_dynamodb::Config`] for tests, using a LocalStack instance.
    pub fn dynamo_db_config(&self) -> aws_sdk_dynamodb::Config {
        self.config.clone()
    }
}

#[cfg(test)]
#[path = "unit_tests/dynamo_db_context_tests.rs"]
mod dynamo_db_context_tests;

/// The tag used for the journal stuff.
const JOURNAL_TAG: u8 = 0;
sa::const_assert!(JOURNAL_TAG < MIN_VIEW_TAG);

/// The attribute name of the partition key.
const PARTITION_ATTRIBUTE: &str = "item_partition";

/// A dummy value to use as the partition key.
const DUMMY_PARTITION_KEY: &[u8] = &[0];

/// A key being used for testing existence of tables
const DB_KEY: &[u8] = &[0];

/// The attribute name of the primary key (used as a sort key).
const KEY_ATTRIBUTE: &str = "item_key";

/// The attribute name of the table value blob.
const VALUE_ATTRIBUTE: &str = "item_value";

/// The attribute for obtaining the primary key (used as a sort key) with the stored value.
const KEY_VALUE_ATTRIBUTE: &str = "item_key, item_value";

/// TODO(#1084): The scheme below with the MAX_VALUE_SIZE has to be checked
/// This is the maximum size of a raw value in DynamoDb.
const RAW_MAX_VALUE_SIZE: usize = 409600;

/// Fundamental constants in DynamoDB: The maximum size of a value is 400KB
/// See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html
/// However, the value being written can also be the serialization of a SimpleUnorderedBatch
/// Therefore the actual MAX_VALUE_SIZE might be lower.
/// At the maximum the key_size is 1024 bytes (see below) and we pack just one entry.
/// So if the key has 1024 bytes this gets us the inequality
/// 1 + 1 + serialized_size(1024)? + serialized_size(x)? <= 400*1024
/// and so this simplifies to 1 + 1 + (2 + 1024) + (3 + x) <= 400 * 1024
/// (we write 3 because get_uleb128_size(400*1024) = 3)
/// and so to a maximal value of 408569;
const VISIBLE_MAX_VALUE_SIZE: usize = 408569;

/// Fundamental constant in DynamoDB: The maximum size of a key is 1024 bytes
/// See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
const MAX_KEY_SIZE: usize = 1024;

/// Fundamental constants in DynamoDB: The maximum size of a TransactWriteItem is 4M.
/// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
const MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE: usize = 4194304;

/// The DynamoDb database is potentially handling an infinite number of connections.
/// However, for testing or some other purpose we really need to decrease the number of
/// connections.
#[cfg(any(test, feature = "test"))]
const TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of entries in a stream of the tests can be controlled by this parameter for tests.
#[cfg(any(test, feature = "test"))]
const TEST_DYNAMO_DB_MAX_STREAM_QUERIES: usize = 10;

/// Fundamental constants in DynamoDB: The maximum size of a TransactWriteItem is 100.
/// See <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html>
const MAX_TRANSACT_WRITE_ITEM_SIZE: usize = 100;

/// Builds the key attributes for a table item.
///
/// The key is composed of two attributes that are both binary blobs. The first attribute is a
/// partition key and is currently just a dummy value that ensures all items are in the same
/// partion. This is necessary for range queries to work correctly.
///
/// The second attribute is the actual key value, which is generated by concatenating the
/// context prefix. The Vec<u8> expression is obtained from self.derive_key.
fn build_key(key: Vec<u8>) -> HashMap<String, AttributeValue> {
    [
        (
            PARTITION_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(DUMMY_PARTITION_KEY)),
        ),
        (KEY_ATTRIBUTE.to_owned(), AttributeValue::B(Blob::new(key))),
    ]
    .into()
}

/// Builds the value attribute for storing a table item.
fn build_key_value(key: Vec<u8>, value: Vec<u8>) -> HashMap<String, AttributeValue> {
    [
        (
            PARTITION_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(DUMMY_PARTITION_KEY)),
        ),
        (KEY_ATTRIBUTE.to_owned(), AttributeValue::B(Blob::new(key))),
        (
            VALUE_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(value)),
        ),
    ]
    .into()
}

/// Extracts the key attribute from an item.
fn extract_key(
    prefix_len: usize,
    attributes: &HashMap<String, AttributeValue>,
) -> Result<&[u8], DynamoDbContextError> {
    let key = attributes
        .get(KEY_ATTRIBUTE)
        .ok_or(DynamoDbContextError::MissingKey)?;
    match key {
        AttributeValue::B(blob) => Ok(&blob.as_ref()[prefix_len..]),
        key => Err(DynamoDbContextError::wrong_key_type(key)),
    }
}

/// Extracts the value attribute from an item.
fn extract_value(
    attributes: &HashMap<String, AttributeValue>,
) -> Result<&[u8], DynamoDbContextError> {
    // According to the official AWS DynamoDB documentation:
    // "Binary must have a length greater than zero if the attribute is used as a key attribute for a table or index"
    let value = attributes
        .get(VALUE_ATTRIBUTE)
        .ok_or(DynamoDbContextError::MissingValue)?;
    match value {
        AttributeValue::B(blob) => Ok(blob.as_ref()),
        value => Err(DynamoDbContextError::wrong_value_type(value)),
    }
}

/// Extracts the value attribute from an item (returned by value).
fn extract_value_owned(
    attributes: &mut HashMap<String, AttributeValue>,
) -> Result<Vec<u8>, DynamoDbContextError> {
    let value = attributes
        .remove(VALUE_ATTRIBUTE)
        .ok_or(DynamoDbContextError::MissingValue)?;
    match value {
        AttributeValue::B(blob) => Ok(blob.into_inner()),
        value => Err(DynamoDbContextError::wrong_value_type(&value)),
    }
}

/// Extracts the key and value attributes from an item.
fn extract_key_value(
    prefix_len: usize,
    attributes: &HashMap<String, AttributeValue>,
) -> Result<(&[u8], &[u8]), DynamoDbContextError> {
    let key = extract_key(prefix_len, attributes)?;
    let value = extract_value(attributes)?;
    Ok((key, value))
}

/// Extracts the `(key, value)` pair attributes from an item (returned by value).
fn extract_key_value_owned(
    prefix_len: usize,
    attributes: &mut HashMap<String, AttributeValue>,
) -> Result<(Vec<u8>, Vec<u8>), DynamoDbContextError> {
    let key = extract_key(prefix_len, attributes)?.to_vec();
    let value = extract_value_owned(attributes)?;
    Ok((key, value))
}

#[derive(Default)]
struct TransactionBuilder {
    transacts: Vec<TransactWriteItem>,
}

impl TransactionBuilder {
    fn insert_delete_request(
        &mut self,
        key: Vec<u8>,
        store: &DynamoDbStoreInternal,
    ) -> Result<(), DynamoDbContextError> {
        let transact = store.build_delete_transact(key)?;
        self.transacts.push(transact);
        Ok(())
    }

    fn insert_put_request(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        store: &DynamoDbStoreInternal,
    ) -> Result<(), DynamoDbContextError> {
        let transact = store.build_put_transact(key, value)?;
        self.transacts.push(transact);
        Ok(())
    }

    async fn submit(self, store: &DynamoDbStoreInternal) -> Result<(), DynamoDbContextError> {
        store.submit_transactions(self.transacts).await
    }
}

#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the header of the journal.
    Journal = 1,
    /// Prefix for the block entry.
    Entry,
}

fn get_journaling_key(base_key: &[u8], tag: u8, pos: u32) -> Result<Vec<u8>, DynamoDbContextError> {
    // We used the value 0 because it does not collide with other key values.
    // since other tags are starting from 1.
    let mut key = base_key.to_vec();
    key.extend([JOURNAL_TAG]);
    key.extend([tag]);
    bcs::serialize_into(&mut key, &pos)?;
    Ok(key)
}

/// The header that contains the current state of the journal.
#[derive(Serialize, Deserialize)]
struct JournalHeader {
    block_count: u32,
}

impl JournalHeader {
    /// Resolves the database by using the header that has been retrieved
    async fn coherently_resolve_journal(
        mut self,
        store: &DynamoDbStoreInternal,
        base_key: &[u8],
    ) -> Result<(), DynamoDbContextError> {
        loop {
            if self.block_count == 0 {
                break;
            }
            let key = get_journaling_key(base_key, KeyTag::Entry as u8, self.block_count - 1)?;
            let value = store.read_value::<DynamoDbBatch>(&key).await?;
            if let Some(value) = value {
                let mut tb = TransactionBuilder::default();
                tb.insert_delete_request(key, store)?; // Delete the preceding journal entry
                for delete in value.0.deletions {
                    tb.insert_delete_request(delete, store)?;
                }
                for key_value in value.0.insertions {
                    tb.insert_put_request(key_value.0, key_value.1, store)?;
                }
                self.block_count -= 1;
                DynamoDbBatch::add_journal_header_operations(&mut tb, &self, store, base_key)?;
                tb.submit(store).await?;
            } else {
                return Err(DynamoDbContextError::DatabaseRecoveryFailed);
            }
        }
        Ok(())
    }
}

/// The bunch of deletes and writes to be done.
#[derive(Serialize, Deserialize)]
struct DynamoDbBatch(SimpleUnorderedBatch);

impl DynamoDbBatch {
    fn is_fastpath_feasible(&self) -> bool {
        if self.0.len() > MAX_TRANSACT_WRITE_ITEM_SIZE {
            return false;
        }
        self.0.bytes() <= MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE
    }

    fn add_journal_header_operations(
        transact_builder: &mut TransactionBuilder,
        header: &JournalHeader,
        store: &DynamoDbStoreInternal,
        base_key: &[u8],
    ) -> Result<(), DynamoDbContextError> {
        let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
        if header.block_count > 0 {
            let value = bcs::to_bytes(header)?;
            transact_builder.insert_put_request(key, value, store)?;
        } else {
            transact_builder.insert_delete_request(key, store)?;
        }
        Ok(())
    }

    /// Writes blocks to the database and resolves them later.
    pub async fn write_journal(
        self,
        store: &DynamoDbStoreInternal,
        base_key: &[u8],
    ) -> Result<JournalHeader, DynamoDbContextError> {
        let mut delete_iter = self.0.deletions.into_iter().peekable();
        let mut insert_iter = self.0.insertions.into_iter().peekable();
        let mut value_size = 0;
        let mut transact_size = 0;
        let mut deletions = Vec::new();
        let mut insertions = Vec::new();
        let mut block_count = 0;
        let mut transacts = Vec::new();
        loop {
            if let Some(delete) = delete_iter.next() {
                value_size += serialized_size(&delete)?;
                deletions.push(delete);
            } else if let Some((key, value)) = insert_iter.next() {
                value_size += serialized_size(&key)? + serialized_size(&value)?;
                insertions.push((key, value));
            } else {
                break;
            }
            let (value_flush, transact_flush) = if (delete_iter.len() + insert_iter.len() == 0)
                || deletions.len() + insertions.len() == MAX_TRANSACT_WRITE_ITEM_SIZE - 2
            {
                (true, true)
            } else {
                let next_size = if let Some(delete) = delete_iter.peek() {
                    serialized_size(&delete)?
                } else {
                    // Unwrapping: We checked above that at least one iterator is not empty.
                    let (key, value) = insert_iter.peek().unwrap();
                    serialized_size(&key)? + serialized_size(&value)?
                };
                let next_value_size = value_size
                    + next_size
                    + get_uleb128_size(deletions.len() + 1)
                    + get_uleb128_size(insertions.len() + 1);
                let next_transact_size = transact_size + next_value_size;
                let value_flush = if next_transact_size > MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE {
                    true
                } else {
                    next_value_size > RAW_MAX_VALUE_SIZE
                };
                let transact_flush = next_transact_size > MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE;
                (value_flush, transact_flush)
            };
            if value_flush {
                value_size +=
                    get_uleb128_size(deletions.len()) + get_uleb128_size(insertions.len());
                let simple_unordered_batch = SimpleUnorderedBatch {
                    deletions: mem::take(&mut deletions),
                    insertions: mem::take(&mut insertions),
                };
                let entry = DynamoDbBatch(simple_unordered_batch);
                let value = bcs::to_bytes(&entry)?;
                assert_eq!(value.len(), value_size);
                let key = get_journaling_key(base_key, KeyTag::Entry as u8, block_count)?;
                transacts.push(store.build_put_transact(key, value)?);
                block_count += 1;
                transact_size += value_size;
                value_size = 0;
            }
            if transact_flush {
                store.submit_transactions(mem::take(&mut transacts)).await?;
                transact_size = 0;
            }
        }
        let header = JournalHeader { block_count };
        if block_count > 0 {
            let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
            let value = bcs::to_bytes(&header)?;
            store.write_single_key_value(key, value).await?;
        }
        Ok(header)
    }

    /// This code is for submitting the transaction in one single transaction when that is possible.
    pub async fn write_fastpath_failsafe(
        self,
        store: &DynamoDbStoreInternal,
    ) -> Result<(), DynamoDbContextError> {
        let mut tb = TransactionBuilder::default();
        for key in self.0.deletions {
            tb.insert_delete_request(key, store)?;
        }
        for key_value in self.0.insertions {
            tb.insert_put_request(key_value.0, key_value.1, store)?;
        }
        tb.submit(store).await
    }

    async fn from_batch(
        store: &DynamoDbStoreInternal,
        batch: Batch,
    ) -> Result<Self, DynamoDbContextError> {
        // The DynamoDB does not support the `DeletePrefix` operation.
        // Therefore it does not make sense to have a delete prefix and they have to
        // be downloaded for making a list.
        // Also we remove the deletes that are followed by inserts on the same key because
        // the TransactWriteItem and BatchWriteItem are not going to work that way.
        let unordered_batch = batch.simplify();
        let simple_unordered_batch = unordered_batch.expand_delete_prefixes(store).await?;
        Ok(DynamoDbBatch(simple_unordered_batch))
    }
}

/// A DynamoDB client.
#[derive(Debug, Clone)]
pub struct DynamoDbStoreInternal {
    client: Client,
    table: TableName,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
}

#[async_trait]
impl DeletePrefixExpander for DynamoDbStoreInternal {
    type Error = DynamoDbContextError;
    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut vector_list = Vec::new();
        for key in self.find_keys_by_prefix(key_prefix).await?.iterator() {
            vector_list.push(key?.to_vec());
        }
        Ok(vector_list)
    }
}

/// The initial configuration of the system
#[derive(Debug)]
pub struct DynamoDbStoreConfig {
    /// The AWS configuration
    pub config: Config,
    /// The table_name used
    pub table_name: TableName,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

impl DynamoDbStoreInternal {
    fn build_delete_transact(
        &self,
        key: Vec<u8>,
    ) -> Result<TransactWriteItem, DynamoDbContextError> {
        ensure!(!key.is_empty(), DynamoDbContextError::ZeroLengthKey);
        ensure!(key.len() <= MAX_KEY_SIZE, DynamoDbContextError::KeyTooLong);
        let request = Delete::builder()
            .table_name(&self.table.0)
            .set_key(Some(build_key(key)))
            .build()?;
        Ok(TransactWriteItem::builder().delete(request).build())
    }

    fn build_put_transact(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<TransactWriteItem, DynamoDbContextError> {
        ensure!(!key.is_empty(), DynamoDbContextError::ZeroLengthKey);
        ensure!(key.len() <= MAX_KEY_SIZE, DynamoDbContextError::KeyTooLong);
        ensure!(
            value.len() <= RAW_MAX_VALUE_SIZE,
            DynamoDbContextError::ValueLengthTooLarge
        );
        let request = Put::builder()
            .table_name(&self.table.0)
            .set_item(Some(build_key_value(key, value)))
            .build()?;
        Ok(TransactWriteItem::builder().put(request).build())
    }

    async fn submit_transactions(
        &self,
        transacts: Vec<TransactWriteItem>,
    ) -> Result<(), DynamoDbContextError> {
        if transacts.len() > MAX_TRANSACT_WRITE_ITEM_SIZE {
            return Err(DynamoDbContextError::TransactUpperLimitSize);
        }
        if !transacts.is_empty() {
            let _guard = self.acquire().await;
            self.client
                .transact_write_items()
                .set_transact_items(Some(transacts))
                .send()
                .await?;
        }
        Ok(())
    }

    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
        }
    }

    /// Testing the existence of a table
    pub async fn test_table_existence(
        client: &Client,
        table: &TableName,
    ) -> Result<bool, DynamoDbContextError> {
        let key_db = build_key(DB_KEY.to_vec());
        let response = client
            .get_item()
            .table_name(table.as_ref())
            .set_key(Some(key_db))
            .send()
            .await;
        let Err(error) = response else {
            return Ok(true);
        };
        let test = match &error {
            SdkError::ServiceError(error) => match error.err() {
                GetItemError::ResourceNotFoundException(error) => {
                    error.message
                        == Some("Cannot do operations on a non-existent table".to_string())
                }
                _ => false,
            },
            _ => false,
        };
        if test {
            Ok(false)
        } else {
            Err(error.into())
        }
    }

    /// Creates a new [`DynamoDbStoreInternal`] instance from scratch using the provided `config` parameters.
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let client = Client::from_conf(store_config.config);
        if Self::test_table_existence(&client, &store_config.table_name).await? {
            client
                .delete_table()
                .table_name(&store_config.table_name.0)
                .send()
                .await?;
        }
        let stop_if_table_exists = true;
        let create_if_missing = true;
        Self::new_internal(
            client,
            store_config.table_name,
            store_config.common_config,
            stop_if_table_exists,
            create_if_missing,
        )
        .await
    }

    /// Testing the existence of a table
    pub async fn test_existence(
        store_config: DynamoDbStoreConfig,
    ) -> Result<bool, DynamoDbContextError> {
        let client = Client::from_conf(store_config.config);
        Self::test_table_existence(&client, &store_config.table_name).await
    }

    async fn delete_all(store_config: DynamoDbStoreConfig) -> Result<(), DynamoDbContextError> {
        let client = Client::from_conf(store_config.config);
        clear_tables(&client).await?;
        Ok(())
    }

    async fn list_tables(
        store_config: DynamoDbStoreConfig,
    ) -> Result<Vec<String>, DynamoDbContextError> {
        let client = Client::from_conf(store_config.config);
        list_tables_from_client(&client).await
    }

    async fn delete_single(store_config: DynamoDbStoreConfig) -> Result<(), DynamoDbContextError> {
        let client = Client::from_conf(store_config.config);
        client
            .delete_table()
            .table_name(&store_config.table_name.0)
            .send()
            .await?;
        Ok(())
    }

    /// Creates a new [`DynamoDbStoreInternal`] instance using the provided `config` parameters.
    pub async fn new(
        store_config: DynamoDbStoreConfig,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let client = Client::from_conf(store_config.config);
        let stop_if_table_exists = false;
        let create_if_missing = false;
        Self::new_internal(
            client,
            store_config.table_name,
            store_config.common_config,
            stop_if_table_exists,
            create_if_missing,
        )
        .await
    }

    /// Initializes a RocksDB database from a specified path.
    pub async fn initialize(
        store_config: DynamoDbStoreConfig,
    ) -> Result<Self, DynamoDbContextError> {
        let client = Client::from_conf(store_config.config);
        let stop_if_table_exists = false;
        let create_if_missing = true;
        let (client, table_status) = Self::new_internal(
            client,
            store_config.table_name,
            store_config.common_config,
            stop_if_table_exists,
            create_if_missing,
        )
        .await?;
        if table_status == TableStatus::Existing {
            return Err(DynamoDbContextError::AlreadyExistingDatabase);
        }
        Ok(client)
    }

    /// Creates a new [`DynamoDbStoreInternal`] instance using the provided `config` parameters.
    async fn new_internal(
        client: Client,
        table: TableName,
        common_config: CommonStoreConfig,
        stop_if_table_exists: bool,
        create_if_missing: bool,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let kv_name = format!("table={:?} common_config={:?}", table, common_config);
        let mut existing_table = Self::test_table_existence(&client, &table).await?;
        let semaphore = common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = common_config.max_stream_queries;
        let store = Self {
            client,
            table,
            semaphore,
            max_stream_queries,
        };
        if !existing_table {
            if create_if_missing {
                existing_table = store.create_table(stop_if_table_exists).await?;
            } else {
                tracing::info!("DynamoDb: Missing database for kv_name={}", kv_name);
                return Err(DynamoDbContextError::MissingDatabase(kv_name));
            }
        }
        let table_status = if existing_table {
            TableStatus::Existing
        } else {
            TableStatus::New
        };
        Ok((store, table_status))
    }

    async fn get_query_output(
        &self,
        attribute_str: &str,
        key_prefix: &[u8],
        start_key_map: Option<HashMap<String, AttributeValue>>,
    ) -> Result<QueryOutput, DynamoDbContextError> {
        let _guard = self.acquire().await;
        let response = self
            .client
            .query()
            .table_name(self.table.as_ref())
            .projection_expression(attribute_str)
            .key_condition_expression(format!(
                "{PARTITION_ATTRIBUTE} = :partition and begins_with({KEY_ATTRIBUTE}, :prefix)"
            ))
            .expression_attribute_values(
                ":partition",
                AttributeValue::B(Blob::new(DUMMY_PARTITION_KEY)),
            )
            .expression_attribute_values(":prefix", AttributeValue::B(Blob::new(key_prefix)))
            .set_exclusive_start_key(start_key_map)
            .send()
            .await?;
        Ok(response)
    }

    async fn read_value_bytes_general(
        &self,
        key_db: HashMap<String, AttributeValue>,
    ) -> Result<Option<Vec<u8>>, DynamoDbContextError> {
        let _guard = self.acquire().await;
        let response = self
            .client
            .get_item()
            .table_name(self.table.as_ref())
            .set_key(Some(key_db))
            .send()
            .await?;

        match response.item {
            Some(mut item) => {
                let value = extract_value_owned(&mut item)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn contains_key_general(
        &self,
        key_db: HashMap<String, AttributeValue>,
    ) -> Result<bool, DynamoDbContextError> {
        let _guard = self.acquire().await;
        let response = self
            .client
            .get_item()
            .table_name(self.table.as_ref())
            .set_key(Some(key_db))
            .projection_expression(PARTITION_ATTRIBUTE)
            .send()
            .await?;

        Ok(response.item.is_some())
    }

    async fn write_single_key_value(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), DynamoDbContextError> {
        let mut tb = TransactionBuilder::default();
        tb.insert_put_request(key, value, self)?;
        tb.submit(self).await
    }

    /// Creates the table. Returns whether there was already a table.
    /// If `stop_if_exists` is true then an already present table raises an error.
    ///
    /// We need that because the `test_table_existence` might return `false`
    /// from several processes but only one creates it.
    async fn create_table(&self, stop_if_table_exists: bool) -> Result<bool, DynamoDbContextError> {
        let _guard = self.acquire().await;
        let result = self
            .client
            .create_table()
            .table_name(self.table.as_ref())
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(PARTITION_ATTRIBUTE)
                    .attribute_type(ScalarAttributeType::B)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(KEY_ATTRIBUTE)
                    .attribute_type(ScalarAttributeType::B)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(PARTITION_ATTRIBUTE)
                    .key_type(KeyType::Hash)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(KEY_ATTRIBUTE)
                    .key_type(KeyType::Range)
                    .build()?,
            )
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(10)
                    .write_capacity_units(10)
                    .build()?,
            )
            .send()
            .await;
        let Err(error) = result else {
            return Ok(false);
        };
        if stop_if_table_exists {
            return Err(error.into());
        }
        let test = match &error {
            SdkError::ServiceError(error) => {
                matches!(error.err(), CreateTableError::ResourceInUseException(_))
            }
            _ => false,
        };
        if test {
            Ok(true)
        } else {
            Err(error.into())
        }
    }

    async fn get_list_responses(
        &self,
        attribute: &str,
        key_prefix: &[u8],
    ) -> Result<QueryResults, DynamoDbContextError> {
        ensure!(
            !key_prefix.is_empty(),
            DynamoDbContextError::ZeroLengthKeyPrefix
        );
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            DynamoDbContextError::KeyPrefixTooLong
        );
        let mut responses = Vec::new();
        let mut start_key = None;
        loop {
            let response = self
                .get_query_output(attribute, key_prefix, start_key)
                .await?;
            let last_evaluated = response.last_evaluated_key.clone();
            responses.push(response);
            match last_evaluated {
                None => {
                    break;
                }
                Some(value) => {
                    start_key = Some(value);
                }
            }
        }
        Ok(QueryResults {
            prefix_len: key_prefix.len(),
            responses,
        })
    }
}

struct QueryResponses {
    prefix_len: usize,
    responses: Vec<QueryOutput>,
}

// Inspired by https://depth-first.com/articles/2020/06/22/returning-rust-iterators/
#[doc(hidden)]
#[allow(clippy::type_complexity)]
pub struct DynamoDbKeyBlockIterator<'a> {
    prefix_len: usize,
    pos: usize,
    iters: Vec<
        std::iter::Flatten<
            std::option::Iter<'a, Vec<HashMap<std::string::String, AttributeValue>>>,
        >,
    >,
}

impl<'a> Iterator for DynamoDbKeyBlockIterator<'a> {
    type Item = Result<&'a [u8], DynamoDbContextError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.iters[self.pos].next();
        match result {
            None => {
                if self.pos == self.iters.len() - 1 {
                    return None;
                }
                self.pos += 1;
                self.iters[self.pos]
                    .next()
                    .map(|x| extract_key(self.prefix_len, x))
            }
            Some(result) => Some(extract_key(self.prefix_len, result)),
        }
    }
}

/// A set of keys returned by a search query on DynamoDB.
pub struct DynamoDbKeys {
    result_queries: QueryResponses,
}

impl KeyIterable<DynamoDbContextError> for DynamoDbKeys {
    type Iterator<'a> = DynamoDbKeyBlockIterator<'a> where Self: 'a;

    fn iterator(&self) -> Self::Iterator<'_> {
        let pos = 0;
        let mut iters = Vec::new();
        for response in &self.result_queries.responses {
            let iter = response.items.iter().flatten();
            iters.push(iter);
        }
        DynamoDbKeyBlockIterator {
            prefix_len: self.result_queries.prefix_len,
            pos,
            iters,
        }
    }
}

/// A set of `(key, value)` returned by a search query on DynamoDb.
pub struct DynamoDbKeyValues {
    result_queries: QueryResponses,
}

#[doc(hidden)]
#[allow(clippy::type_complexity)]
pub struct DynamoDbKeyValueIterator<'a> {
    prefix_len: usize,
    pos: usize,
    iters: Vec<
        std::iter::Flatten<
            std::option::Iter<'a, Vec<HashMap<std::string::String, AttributeValue>>>,
        >,
    >,
}

impl<'a> Iterator for DynamoDbKeyValueIterator<'a> {
    type Item = Result<(&'a [u8], &'a [u8]), DynamoDbContextError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.iters[self.pos].next();
        match result {
            None => {
                if self.pos == self.iters.len() - 1 {
                    return None;
                }
                self.pos += 1;
                self.iters[self.pos]
                    .next()
                    .map(|x| extract_key_value(self.prefix_len, x))
            }
            Some(result) => Some(extract_key_value(self.prefix_len, result)),
        }
    }
}

#[doc(hidden)]
#[allow(clippy::type_complexity)]
pub struct DynamoDbKeyValueIteratorOwned {
    prefix_len: usize,
    pos: usize,
    iters: Vec<
        std::iter::Flatten<
            std::option::IntoIter<Vec<HashMap<std::string::String, AttributeValue>>>,
        >,
    >,
}

impl Iterator for DynamoDbKeyValueIteratorOwned {
    type Item = Result<(Vec<u8>, Vec<u8>), DynamoDbContextError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.iters[self.pos].next();
        match result {
            None => {
                if self.pos == self.iters.len() - 1 {
                    return None;
                }
                self.pos += 1;
                self.iters[self.pos]
                    .next()
                    .map(|mut x| extract_key_value_owned(self.prefix_len, &mut x))
            }
            Some(mut result) => Some(extract_key_value_owned(self.prefix_len, &mut result)),
        }
    }
}

impl KeyValueIterable<DynamoDbContextError> for DynamoDbKeyValues {
    type Iterator<'a> = DynamoDbKeyValueIterator<'a> where Self: 'a;
    type IteratorOwned = DynamoDbKeyValueIteratorOwned;

    fn iterator(&self) -> Self::Iterator<'_> {
        let pos = 0;
        let mut iters = Vec::new();
        for response in &self.result_queries.responses {
            let iter = response.items.iter().flatten();
            iters.push(iter);
        }
        DynamoDbKeyValueIterator {
            prefix_len: self.result_queries.prefix_len,
            pos,
            iters,
        }
    }

    fn into_iterator_owned(self) -> Self::IteratorOwned {
        let pos = 0;
        let mut iters = Vec::new();
        for response in self.result_queries.responses.into_iter() {
            let iter = response.items.into_iter().flatten();
            iters.push(iter);
        }
        DynamoDbKeyValueIteratorOwned {
            prefix_len: self.result_queries.prefix_len,
            pos,
            iters,
        }
    }
}

#[async_trait]
impl ReadableKeyValueStore<DynamoDbContextError> for DynamoDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = DynamoDbKeys;
    type KeyValues = DynamoDbKeyValues;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DynamoDbContextError> {
        ensure!(key.len() <= MAX_KEY_SIZE, DynamoDbContextError::KeyTooLong);
        let key_db = build_key(key.to_vec());
        self.read_value_bytes_general(key_db).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, DynamoDbContextError> {
        ensure!(key.len() <= MAX_KEY_SIZE, DynamoDbContextError::KeyTooLong);
        let key_db = build_key(key.to_vec());
        self.contains_key_general(key_db).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, DynamoDbContextError> {
        let mut handles = Vec::new();
        for key in keys {
            ensure!(key.len() <= MAX_KEY_SIZE, DynamoDbContextError::KeyTooLong);
            let key_db = build_key(key);
            let handle = self.read_value_bytes_general(key_db);
            handles.push(handle);
        }
        let result = join_all(handles).await;
        Ok(result.into_iter().collect::<Result<_, _>>()?)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<DynamoDbKeys, DynamoDbContextError> {
        let result_queries = self.get_list_responses(KEY_ATTRIBUTE, key_prefix).await?;
        Ok(DynamoDbKeys { result_queries })
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<DynamoDbKeyValues, DynamoDbContextError> {
        let result_queries = self
            .get_list_responses(KEY_VALUE_ATTRIBUTE, key_prefix)
            .await?;
        Ok(DynamoDbKeyValues { result_queries })
    }
}

#[async_trait]
impl WritableKeyValueStore<DynamoDbContextError> for DynamoDbStoreInternal {
    const MAX_VALUE_SIZE: usize = VISIBLE_MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), DynamoDbContextError> {
        let block_operations = DynamoDbBatch::from_batch(self, batch).await?;
        if block_operations.is_fastpath_feasible() {
            block_operations.write_fastpath_failsafe(self).await
        } else {
            let header = block_operations.write_journal(self, base_key).await?;
            header.coherently_resolve_journal(self, base_key).await
        }
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), DynamoDbContextError> {
        let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
        let value = self.read_value::<JournalHeader>(&key).await?;
        if let Some(header) = value {
            header.coherently_resolve_journal(self, base_key).await?;
        }
        Ok(())
    }
}

impl KeyValueStore for DynamoDbStoreInternal {
    type Error = DynamoDbContextError;
}

/// A shared DB client for DynamoDb implementing LruCaching
#[derive(Clone)]
pub struct DynamoDbStore {
    store: LruCachingStore<ValueSplittingStore<DynamoDbStoreInternal>>,
}

#[async_trait]
impl ReadableKeyValueStore<DynamoDbContextError> for DynamoDbStore {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE - 4;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DynamoDbContextError> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, DynamoDbContextError> {
        self.store.contains_key(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        key: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, DynamoDbContextError> {
        self.store.read_multi_values_bytes(key).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, DynamoDbContextError> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, DynamoDbContextError> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

#[async_trait]
impl WritableKeyValueStore<DynamoDbContextError> for DynamoDbStore {
    const MAX_VALUE_SIZE: usize = DynamoDbStoreInternal::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), DynamoDbContextError> {
        self.store.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), DynamoDbContextError> {
        self.store.clear_journal(base_key).await
    }
}

#[async_trait]
impl KeyValueStore for DynamoDbStore {
    type Error = DynamoDbContextError;
}

impl DynamoDbStore {
    /// Creates a `DynamoDbStore` from scratch with an LRU cache
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let (store, table_status) = DynamoDbStoreInternal::new_for_testing(store_config).await?;
        let store = ValueSplittingStore::new(store);
        let store = LruCachingStore::new(store, cache_size);
        let store = Self { store };
        Ok((store, table_status))
    }

    /// Initializes a `DynamoDbStore`.
    pub async fn initialize(
        store_config: DynamoDbStoreConfig,
    ) -> Result<Self, DynamoDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let store = DynamoDbStoreInternal::initialize(store_config).await?;
        let store = ValueSplittingStore::new(store);
        let store = LruCachingStore::new(store, cache_size);
        let store = Self { store };
        Ok(store)
    }

    /// Deletes all the tables from the database
    pub async fn test_existence(
        store_config: DynamoDbStoreConfig,
    ) -> Result<bool, DynamoDbContextError> {
        DynamoDbStoreInternal::test_existence(store_config).await
    }

    /// List all the tables of the database
    pub async fn list_tables(
        store_config: DynamoDbStoreConfig,
    ) -> Result<Vec<String>, DynamoDbContextError> {
        DynamoDbStoreInternal::list_tables(store_config).await
    }

    /// Deletes all the tables from the database
    pub async fn delete_all(store_config: DynamoDbStoreConfig) -> Result<(), DynamoDbContextError> {
        DynamoDbStoreInternal::delete_all(store_config).await
    }

    /// Deletes a single table from the database
    pub async fn delete_single(
        store_config: DynamoDbStoreConfig,
    ) -> Result<(), DynamoDbContextError> {
        DynamoDbStoreInternal::delete_single(store_config).await
    }

    /// Creates a `DynamoDbStore` with an LRU cache
    pub async fn new(
        store_config: DynamoDbStoreConfig,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let (store, table_name) = DynamoDbStoreInternal::new(store_config).await?;
        let store = ValueSplittingStore::new(store);
        let store = LruCachingStore::new(store, cache_size);
        let store = Self { store };
        Ok((store, table_name))
    }
}

/// An implementation of [`Context`][trait1] based on [`DynamoDbStore`].
///
/// [trait1]: crate::common::Context
pub type DynamoDbContext<E> = ContextFromStore<E, DynamoDbStore>;

impl<E> DynamoDbContext<E>
where
    E: Clone + Sync + Send,
{
    /// Creates a new [`DynamoDbContext`] instance from scratch from the given AWS configuration.
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
        base_key: Vec<u8>,
        extra: E,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) = DynamoDbStore::new_for_testing(store_config).await?;
        let storage = DynamoDbContext {
            store,
            base_key,
            extra,
        };
        Ok((storage, table_status))
    }

    /// Creates a new [`DynamoDbContext`] instance from the given AWS configuration.
    pub async fn new(
        store_config: DynamoDbStoreConfig,
        base_key: Vec<u8>,
        extra: E,
    ) -> Result<(Self, TableStatus), DynamoDbContextError> {
        let (store, table_status) = DynamoDbStore::new(store_config).await?;
        let storage = DynamoDbContext {
            store,
            base_key,
            extra,
        };
        Ok((storage, table_status))
    }
}

/// A DynamoDB table name.
///
/// Table names must follow some [naming
/// rules](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules),
/// so this type ensures that they are properly validated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableName(String);

impl FromStr for TableName {
    type Err = InvalidTableName;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        if string.len() < 3 {
            return Err(InvalidTableName::TooShort);
        }
        if string.len() > 255 {
            return Err(InvalidTableName::TooLong);
        }
        if !string.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || character == '.'
                || character == '-'
                || character == '_'
        }) {
            return Err(InvalidTableName::InvalidCharacter);
        }
        Ok(TableName(string.to_owned()))
    }
}

impl AsRef<String> for TableName {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

/// Error when validating a table name.
#[derive(Debug, Error)]
pub enum InvalidTableName {
    /// The table name should be at least 3 characters.
    #[error("Table name must have at least 3 characters")]
    TooShort,

    /// The table name should be at most 63 characters.
    #[error("Table name must be at most 63 characters")]
    TooLong,

    /// allowed characters are lowercase letters, numbers, periods and hyphens
    #[error("Table name must only contain lowercase letters, numbers, periods and hyphens")]
    InvalidCharacter,
}

/// Errors that occur when using [`DynamoDbContext`].
#[derive(Debug, Error)]
pub enum DynamoDbContextError {
    /// An error occurred while getting the item.
    #[error(transparent)]
    Get(#[from] Box<SdkError<GetItemError>>),

    /// An error occurred while writing a batch of items.
    #[error(transparent)]
    BatchWriteItem(#[from] Box<SdkError<BatchWriteItemError>>),

    /// An error occurred while writing a transaction of items.
    #[error(transparent)]
    TransactWriteItem(#[from] Box<SdkError<TransactWriteItemsError>>),

    /// An error occurred while doing a Query.
    #[error(transparent)]
    Query(#[from] Box<SdkError<QueryError>>),

    /// An error occurred while deleting a table
    #[error(transparent)]
    DeleteTable(#[from] Box<SdkError<DeleteTableError>>),

    /// An error occurred while listing tables
    #[error(transparent)]
    ListTables(#[from] Box<SdkError<ListTablesError>>),

    /// The transact maximum size is MAX_TRANSACT_WRITE_ITEM_SIZE.
    #[error("The transact must have length at most MAX_TRANSACT_WRITE_ITEM_SIZE")]
    TransactUpperLimitSize,

    /// Keys have to be of non-zero length.
    #[error("The key must be of strictly positive length")]
    ZeroLengthKey,

    /// The key must have at most 1024 bytes
    #[error("The key must have at most 1024 bytes")]
    KeyTooLong,

    /// The key prefix must have at most 1024 bytes
    #[error("The key prefix must have at most 1024 bytes")]
    KeyPrefixTooLong,

    /// Key prefixes have to be of non-zero length.
    #[error("The key_prefix must be of strictly positive length")]
    ZeroLengthKeyPrefix,

    /// The recovery failed.
    #[error("The DynamoDB database recovery failed")]
    DatabaseRecoveryFailed,

    /// The database is not coherent
    #[error(transparent)]
    DatabaseConsistencyError(#[from] DatabaseConsistencyError),

    /// Missing database
    #[error("Missing database")]
    MissingDatabase(String),

    /// Already existing database
    #[error("Already existing database")]
    AlreadyExistingDatabase,

    /// The length of the value should be at most 400KB.
    #[error("The DynamoDB value should be less than 400KB")]
    ValueLengthTooLarge,

    /// The stored key is missing.
    #[error("The stored key attribute is missing")]
    MissingKey,

    /// The type of the keys was not correct (It should have been a binary blob).
    #[error("Key was stored as {0}, but it was expected to be stored as a binary blob")]
    WrongKeyType(String),

    /// The value attribute is missing.
    #[error("The stored value attribute is missing")]
    MissingValue,

    /// The value was stored as the wrong type (it should be a binary blob).
    #[error("Value was stored as {0}, but it was expected to be stored as a binary blob")]
    WrongValueType(String),

    /// A BCS error occurred.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    /// An error occurred while creating the table.
    #[error(transparent)]
    CreateTable(#[from] SdkError<CreateTableError>),

    /// An error occurred while building an object
    #[error(transparent)]
    Build(#[from] Box<BuildError>),
}

impl<InnerError> From<SdkError<InnerError>> for DynamoDbContextError
where
    DynamoDbContextError: From<Box<SdkError<InnerError>>>,
{
    fn from(error: SdkError<InnerError>) -> Self {
        Box::new(error).into()
    }
}

impl From<BuildError> for DynamoDbContextError {
    fn from(error: BuildError) -> Self {
        Box::new(error).into()
    }
}

impl DynamoDbContextError {
    /// Creates a [`DynamoDbContextError::WrongKeyType`] instance based on the returned value type.
    ///
    /// # Panics
    ///
    /// If the value type is in the correct type, a binary blob.
    pub fn wrong_key_type(value: &AttributeValue) -> Self {
        DynamoDbContextError::WrongKeyType(Self::type_description_of(value))
    }

    /// Creates a [`DynamoDbContextError::WrongValueType`] instance based on the returned value type.
    ///
    /// # Panics
    ///
    /// If the value type is in the correct type, a binary blob.
    pub fn wrong_value_type(value: &AttributeValue) -> Self {
        DynamoDbContextError::WrongValueType(Self::type_description_of(value))
    }

    fn type_description_of(value: &AttributeValue) -> String {
        match value {
            AttributeValue::B(_) => unreachable!("creating an error type for the correct type"),
            AttributeValue::Bool(_) => "a boolean",
            AttributeValue::Bs(_) => "a list of binary blobs",
            AttributeValue::L(_) => "a list",
            AttributeValue::M(_) => "a map",
            AttributeValue::N(_) => "a number",
            AttributeValue::Ns(_) => "a list of numbers",
            AttributeValue::Null(_) => "a null value",
            AttributeValue::S(_) => "a string",
            AttributeValue::Ss(_) => "a list of strings",
            _ => "an unknown type",
        }
        .to_owned()
    }
}

impl From<DynamoDbContextError> for crate::views::ViewError {
    fn from(error: DynamoDbContextError) -> Self {
        Self::ContextError {
            backend: "DynamoDB".to_string(),
            error: error.to_string(),
        }
    }
}

/// A static lock to prevent multiple tests from using the same LocalStack instance at the same
/// time.
#[cfg(any(test, feature = "test"))]
static LOCALSTACK_GUARD: Mutex<()> = Mutex::const_new(());

/// Creates the common initialization for RocksDB
#[cfg(any(test, feature = "test"))]
pub fn create_dynamo_db_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: Some(TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES),
        max_stream_queries: TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
        cache_size: TEST_CACHE_SIZE,
    }
}

/// Creates a basic client that can be used for tests.
#[cfg(any(test, feature = "test"))]
pub async fn create_dynamo_db_test_store() -> DynamoDbStore {
    let common_config = create_dynamo_db_common_config();
    let table = get_table_name();
    let table_name = table.parse().expect("Invalid table name");
    let use_localstack = true;
    let config = get_config(use_localstack).await.expect("config");
    let store_config = DynamoDbStoreConfig {
        config,
        table_name,
        common_config,
    };
    let (key_value_store, _) = DynamoDbStore::new_for_testing(store_config)
        .await
        .expect("key_value_store");
    key_value_store
}

/// Helper function to list the names of tables registered on DynamoDB.
pub async fn list_tables_from_client(
    client: &aws_sdk_dynamodb::Client,
) -> Result<Vec<String>, DynamoDbContextError> {
    Ok(client
        .list_tables()
        .send()
        .await?
        .table_names
        .expect("List of tables was not returned"))
}

/// Helper function to clear all the tables from the database
pub async fn clear_tables(client: &aws_sdk_dynamodb::Client) -> Result<(), DynamoDbContextError> {
    let tables = list_tables_from_client(client).await?;
    for table in tables {
        client.delete_table().table_name(&table).send().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        batch::SimpleUnorderedBatch,
        dynamo_db::{get_uleb128_size, MAX_KEY_SIZE, RAW_MAX_VALUE_SIZE, VISIBLE_MAX_VALUE_SIZE},
    };
    use bcs::serialized_size;

    #[test]
    fn test_serialization_len() {
        for n in [0, 10, 127, 128, 129, 16383, 16384, 20000] {
            let vec = vec![0u8; n];
            let est_size = get_uleb128_size(n) + n;
            let serial_size = serialized_size(&vec).unwrap();
            assert_eq!(est_size, serial_size);
        }
    }

    #[test]
    fn test_raw_visible_sizes() {
        let mut vis_computed = RAW_MAX_VALUE_SIZE - MAX_KEY_SIZE;
        vis_computed -= serialized_size(&SimpleUnorderedBatch::default()).unwrap();
        vis_computed -= get_uleb128_size(RAW_MAX_VALUE_SIZE) + get_uleb128_size(MAX_KEY_SIZE);
        assert_eq!(vis_computed, VISIBLE_MAX_VALUE_SIZE);
    }
}
