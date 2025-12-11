// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the DynamoDB database.

use std::{
    collections::HashMap,
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_lock::{Semaphore, SemaphoreGuard};
use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        batch_get_item::BatchGetItemError,
        create_table::CreateTableError,
        delete_item::DeleteItemError,
        delete_table::DeleteTableError,
        get_item::GetItemError,
        list_tables::ListTablesError,
        put_item::PutItemError,
        query::{QueryError, QueryOutput},
        transact_write_items::TransactWriteItemsError,
    },
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, Delete, KeySchemaElement, KeyType, KeysAndAttributes,
        ProvisionedThroughput, Put, ScalarAttributeType, TransactWriteItem,
    },
    Client,
};
use aws_smithy_types::error::operation::BuildError;
use futures::future::join_all;
use linera_base::{ensure, util::future::FutureSyncExt as _};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(with_metrics)]
use crate::metering::MeteredDatabase;
#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::SimpleUnorderedBatch,
    common::get_uleb128_size,
    journaling::{JournalConsistencyError, JournalingKeyValueDatabase},
    lru_caching::{LruCachingConfig, LruCachingDatabase},
    store::{
        DirectWritableKeyValueStore, KeyValueDatabase, KeyValueStoreError, LeaseConflict,
        ReadableKeyValueStore, WithError,
    },
    value_splitting::{ValueSplittingDatabase, ValueSplittingError},
};

/// Name of the environment variable with the address to a DynamoDB local instance.
const DYNAMODB_LOCAL_ENDPOINT: &str = "DYNAMODB_LOCAL_ENDPOINT";

/// Gets the AWS configuration from the environment
async fn get_base_config() -> Result<aws_sdk_dynamodb::Config, DynamoDbStoreInternalError> {
    let base_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest())
        .boxed_sync()
        .await;
    Ok((&base_config).into())
}

fn get_endpoint_address() -> Option<String> {
    env::var(DYNAMODB_LOCAL_ENDPOINT).ok()
}

/// Gets the DynamoDB local config
async fn get_dynamodb_local_config() -> Result<aws_sdk_dynamodb::Config, DynamoDbStoreInternalError>
{
    let base_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest())
        .boxed_sync()
        .await;
    let endpoint_address = get_endpoint_address().unwrap();
    let config = aws_sdk_dynamodb::config::Builder::from(&base_config)
        .endpoint_url(endpoint_address)
        .build();
    Ok(config)
}

/// DynamoDB forbids the iteration over the partition keys.
/// Therefore we use a special partition key named `[1]` for storing
/// the root keys. For normal root keys, we simply put a `[0]` in
/// front therefore no intersection is possible.
const PARTITION_KEY_ROOT_KEY: &[u8] = &[1];

/// The attribute name of the partition key.
const PARTITION_ATTRIBUTE: &str = "item_partition";

/// A root key being used for testing existence of tables
const EMPTY_ROOT_KEY: &[u8] = &[0];

/// A key being used for testing existence of tables
const DB_KEY: &[u8] = &[0];

/// The attribute name of the primary key (used as a sort key).
const KEY_ATTRIBUTE: &str = "item_key";

/// The attribute name of the table value blob.
const VALUE_ATTRIBUTE: &str = "item_value";

/// The attribute for obtaining the primary key (used as a sort key) with the stored value.
const KEY_VALUE_ATTRIBUTE: &str = "item_key, item_value";

/// The attribute name for lease UUID (stored as a string).
const LEASE_UUID_ATTRIBUTE: &str = "lease_uuid";

/// The attribute name for lease expiration timestamp.
const LEASE_EXPIRATION_ATTRIBUTE: &str = "lease_expiration";

/// TODO(#1084): The scheme below with the `MAX_VALUE_SIZE` has to be checked
/// This is the maximum size of a raw value in DynamoDB.
const RAW_MAX_VALUE_SIZE: usize = 409600;

/// Fundamental constants in DynamoDB: The maximum size of a value is 400 KB
/// See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html
/// However, the value being written can also be the serialization of a `SimpleUnorderedBatch`
/// Therefore the actual `MAX_VALUE_SIZE` might be lower.
/// At the maximum key size is 1024 bytes (see below) and we pack just one entry.
/// So if the key has 1024 bytes this gets us the inequality
/// `1 + 1 + serialized_size(1024)? + serialized_size(x)? <= 400*1024`
/// and so this simplifies to `1 + 1 + (2 + 1024) + (3 + x) <= 400 * 1024`
/// Note on the following formula:
/// * We write 3 because `get_uleb128_size(400*1024) == 3`
/// * We write `1 + 1` because the `SimpleUnorderedBatch` has two entries
///
/// This gets us a maximal value of 408569;
const VISIBLE_MAX_VALUE_SIZE: usize = RAW_MAX_VALUE_SIZE
    - MAX_KEY_SIZE
    - get_uleb128_size(RAW_MAX_VALUE_SIZE)
    - get_uleb128_size(MAX_KEY_SIZE)
    - 1
    - 1;

/// Fundamental constant in DynamoDB: The maximum size of a key is 1024 bytes.
/// We decrease by 1 because we append a [1] as prefix.
/// See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
const MAX_KEY_SIZE: usize = 1023;

/// Fundamental constants in DynamoDB: The maximum size of a [`TransactWriteItem`] is 4 MB.
/// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
/// We're taking a conservative value because the mode of computation is unclear.
const MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE: usize = 4000000;

/// The DynamoDB database is potentially handling an infinite number of connections.
/// However, for testing or some other purpose we really need to decrease the number of
/// connections.
#[cfg(with_testing)]
const TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of entries in a stream of the tests can be controlled by this parameter for tests.
#[cfg(with_testing)]
const TEST_DYNAMO_DB_MAX_STREAM_QUERIES: usize = 10;

/// Fundamental constants in DynamoDB: The maximum size of a [`TransactWriteItem`] is 100.
/// See <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html>
const MAX_TRANSACT_WRITE_ITEM_SIZE: usize = 100;

/// Maximum number of entries that can be obtained in a [`BatchGetItem`] operation.
/// The two constraints are at most 100 operations and at most 16M in total.
/// Since the maximum size of a value is 400K, this gets us 40 as upper limit
const MAX_BATCH_GET_ITEM_SIZE: usize = 40;

/// Builds the key attributes for a table item.
///
/// The key is composed of two attributes that are both binary blobs. The first attribute is a
/// partition key and is currently just a dummy value that ensures all items are in the same
/// partition. This is necessary for range queries to work correctly.
///
/// The second attribute is the actual key value, which is generated by concatenating the
/// context prefix. `The Vec<u8>` expression is obtained from `self.derive_key`.
fn build_key(start_key: &[u8], key: Vec<u8>) -> HashMap<String, AttributeValue> {
    let mut prefixed_key = vec![1];
    prefixed_key.extend(key);
    [
        (
            PARTITION_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(start_key.to_vec())),
        ),
        (
            KEY_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(prefixed_key)),
        ),
    ]
    .into()
}

/// Builds the value attribute for storing a table item.
fn build_key_value(
    start_key: &[u8],
    key: Vec<u8>,
    value: Vec<u8>,
) -> HashMap<String, AttributeValue> {
    let mut prefixed_key = vec![1];
    prefixed_key.extend(key);
    [
        (
            PARTITION_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(start_key.to_vec())),
        ),
        (
            KEY_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(prefixed_key)),
        ),
        (
            VALUE_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(value)),
        ),
    ]
    .into()
}

/// Builds a lease item with UUID and expiration as separate attributes for condition expressions.
fn build_lease_item(
    start_key: &[u8],
    key: Vec<u8>,
    uuid: u64,
    expiration: u64,
) -> HashMap<String, AttributeValue> {
    let mut prefixed_key = vec![1];
    prefixed_key.extend(key);

    [
        (
            PARTITION_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(start_key.to_vec())),
        ),
        (
            KEY_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(prefixed_key)),
        ),
        (
            LEASE_UUID_ATTRIBUTE.to_owned(),
            AttributeValue::N(uuid.to_string()),
        ),
        (
            LEASE_EXPIRATION_ATTRIBUTE.to_owned(),
            AttributeValue::N(expiration.to_string()),
        ),
    ]
    .into()
}

/// Checks that a key is of the correct size
fn check_key_size(key: &[u8]) -> Result<(), DynamoDbStoreInternalError> {
    ensure!(
        key.len() <= MAX_KEY_SIZE,
        DynamoDbStoreInternalError::KeyTooLong
    );
    Ok(())
}

/// Extracts the key attribute from an item.
fn extract_key(
    prefix_len: usize,
    attributes: &HashMap<String, AttributeValue>,
) -> Result<&[u8], DynamoDbStoreInternalError> {
    let key = attributes
        .get(KEY_ATTRIBUTE)
        .ok_or(DynamoDbStoreInternalError::MissingKey)?;
    match key {
        AttributeValue::B(blob) => Ok(&blob.as_ref()[1 + prefix_len..]),
        key => Err(DynamoDbStoreInternalError::wrong_key_type(key)),
    }
}

/// Extracts the value attribute from an item.
fn extract_value(
    attributes: &HashMap<String, AttributeValue>,
) -> Result<&[u8], DynamoDbStoreInternalError> {
    // According to the official AWS DynamoDB documentation:
    // "Binary must have a length greater than zero if the attribute is used as a key attribute for a table or index"
    let value = attributes
        .get(VALUE_ATTRIBUTE)
        .ok_or(DynamoDbStoreInternalError::MissingValue)?;
    match value {
        AttributeValue::B(blob) => Ok(blob.as_ref()),
        value => Err(DynamoDbStoreInternalError::wrong_value_type(value)),
    }
}

/// Extracts the value attribute from an item (returned by value).
fn extract_value_owned(
    attributes: &mut HashMap<String, AttributeValue>,
) -> Result<Vec<u8>, DynamoDbStoreInternalError> {
    let value = attributes
        .remove(VALUE_ATTRIBUTE)
        .ok_or(DynamoDbStoreInternalError::MissingValue)?;
    match value {
        AttributeValue::B(blob) => Ok(blob.into_inner()),
        value => Err(DynamoDbStoreInternalError::wrong_value_type(&value)),
    }
}

/// Extracts the key and value attributes from an item.
fn extract_key_value(
    prefix_len: usize,
    attributes: &HashMap<String, AttributeValue>,
) -> Result<(&[u8], &[u8]), DynamoDbStoreInternalError> {
    let key = extract_key(prefix_len, attributes)?;
    let value = extract_value(attributes)?;
    Ok((key, value))
}

struct TransactionBuilder {
    start_key: Vec<u8>,
    transactions: Vec<TransactWriteItem>,
}

impl TransactionBuilder {
    fn new(start_key: &[u8]) -> Self {
        Self {
            start_key: start_key.to_vec(),
            transactions: Vec::new(),
        }
    }

    fn insert_delete_request(
        &mut self,
        key: Vec<u8>,
        store: &DynamoDbStoreInternal,
    ) -> Result<(), DynamoDbStoreInternalError> {
        let transaction = store.build_delete_transaction(&self.start_key, key)?;
        self.transactions.push(transaction);
        Ok(())
    }

    fn insert_put_request(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        store: &DynamoDbStoreInternal,
    ) -> Result<(), DynamoDbStoreInternalError> {
        let transaction = store.build_put_transaction(&self.start_key, key, value)?;
        self.transactions.push(transaction);
        Ok(())
    }
}

/// A DynamoDB client.
#[derive(Clone, Debug)]
pub struct DynamoDbStoreInternal {
    client: Client,
    namespace: String,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    start_key: Vec<u8>,
    root_key_written: Arc<AtomicBool>,
}

/// Database-level connection to DynamoDB for managing namespaces and partitions.
#[derive(Clone)]
pub struct DynamoDbDatabaseInternal {
    client: Client,
    namespace: String,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
}

impl WithError for DynamoDbDatabaseInternal {
    type Error = DynamoDbStoreInternalError;
}

/// The initial configuration of the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbStoreInternalConfig {
    /// Whether to use DynamoDB local or not.
    pub use_dynamodb_local: bool,
    /// Maximum number of concurrent database queries allowed for this client.
    pub max_concurrent_queries: Option<usize>,
    /// Preferred buffer size for async streams.
    pub max_stream_queries: usize,
}

impl DynamoDbStoreInternalConfig {
    async fn client(&self) -> Result<Client, DynamoDbStoreInternalError> {
        let config = if self.use_dynamodb_local {
            get_dynamodb_local_config().await?
        } else {
            get_base_config().await?
        };
        Ok(Client::from_conf(config))
    }
}

impl KeyValueDatabase for DynamoDbDatabaseInternal {
    type Config = DynamoDbStoreInternalConfig;
    type Store = DynamoDbStoreInternal;

    fn get_name() -> String {
        "dynamodb internal".to_string()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Self, DynamoDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let client = config.client().await?;
        let semaphore = config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.max_stream_queries;
        let namespace = namespace.to_string();
        let store = Self {
            client,
            namespace,
            semaphore,
            max_stream_queries,
        };
        Ok(store)
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, DynamoDbStoreInternalError> {
        let mut start_key = EMPTY_ROOT_KEY.to_vec();
        start_key.extend(root_key);
        Ok(self.open_internal(start_key))
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, DynamoDbStoreInternalError> {
        self.open_shared(root_key)
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, DynamoDbStoreInternalError> {
        let client = config.client().await?;
        let mut namespaces = Vec::new();
        let mut start_table = None;
        loop {
            let response = client
                .list_tables()
                .set_exclusive_start_table_name(start_table)
                .send()
                .boxed_sync()
                .await?;
            if let Some(namespaces_blk) = response.table_names {
                namespaces.extend(namespaces_blk);
            }
            if response.last_evaluated_table_name.is_none() {
                break;
            } else {
                start_table = response.last_evaluated_table_name;
            }
        }
        Ok(namespaces)
    }

    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>, DynamoDbStoreInternalError> {
        let store = self.open_internal(PARTITION_KEY_ROOT_KEY.to_vec());
        store.find_keys_by_prefix(EMPTY_ROOT_KEY).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), DynamoDbStoreInternalError> {
        let client = config.client().await?;
        let tables = Self::list_all(config).await?;
        for table in tables {
            client
                .delete_table()
                .table_name(&table)
                .send()
                .boxed_sync()
                .await?;
        }
        Ok(())
    }

    async fn exists(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<bool, DynamoDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let client = config.client().await?;
        let key_db = build_key(EMPTY_ROOT_KEY, DB_KEY.to_vec());
        let response = client
            .get_item()
            .table_name(namespace)
            .set_key(Some(key_db))
            .send()
            .boxed_sync()
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

    async fn create(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), DynamoDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let client = config.client().await?;
        client
            .create_table()
            .table_name(namespace)
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
            .boxed_sync()
            .await?;
        Ok(())
    }

    async fn delete(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), DynamoDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let client = config.client().await?;
        client
            .delete_table()
            .table_name(namespace)
            .send()
            .boxed_sync()
            .await?;
        Ok(())
    }
}

impl DynamoDbDatabaseInternal {
    /// Namespaces are named table names in DynamoDB [naming
    /// rules](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules),
    /// so we need to check correctness of the namespace
    fn check_namespace(namespace: &str) -> Result<(), InvalidNamespace> {
        if namespace.len() < 3 {
            return Err(InvalidNamespace::TooShort);
        }
        if namespace.len() > 255 {
            return Err(InvalidNamespace::TooLong);
        }
        if !namespace.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || character == '.'
                || character == '-'
                || character == '_'
        }) {
            return Err(InvalidNamespace::InvalidCharacter);
        }
        Ok(())
    }

    fn open_internal(&self, start_key: Vec<u8>) -> DynamoDbStoreInternal {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        DynamoDbStoreInternal {
            client,
            namespace,
            semaphore,
            max_stream_queries,
            start_key,
            root_key_written: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl DynamoDbStoreInternal {
    fn build_delete_transaction(
        &self,
        start_key: &[u8],
        key: Vec<u8>,
    ) -> Result<TransactWriteItem, DynamoDbStoreInternalError> {
        check_key_size(&key)?;
        let request = Delete::builder()
            .table_name(&self.namespace)
            .set_key(Some(build_key(start_key, key)))
            .build()?;
        Ok(TransactWriteItem::builder().delete(request).build())
    }

    fn build_put_transaction(
        &self,
        start_key: &[u8],
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<TransactWriteItem, DynamoDbStoreInternalError> {
        check_key_size(&key)?;
        ensure!(
            value.len() <= RAW_MAX_VALUE_SIZE,
            DynamoDbStoreInternalError::ValueLengthTooLarge
        );
        let request = Put::builder()
            .table_name(&self.namespace)
            .set_item(Some(build_key_value(start_key, key, value)))
            .build()?;
        Ok(TransactWriteItem::builder().put(request).build())
    }

    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
        }
    }

    async fn get_query_output(
        &self,
        attribute_str: &str,
        start_key: &[u8],
        key_prefix: &[u8],
        start_key_map: Option<HashMap<String, AttributeValue>>,
    ) -> Result<QueryOutput, DynamoDbStoreInternalError> {
        let _guard = self.acquire().await;
        let start_key = start_key.to_vec();
        let mut prefixed_key_prefix = vec![1];
        prefixed_key_prefix.extend(key_prefix);
        let response = self
            .client
            .query()
            .table_name(&self.namespace)
            .projection_expression(attribute_str)
            .key_condition_expression(format!(
                "{PARTITION_ATTRIBUTE} = :partition and begins_with({KEY_ATTRIBUTE}, :prefix)"
            ))
            .expression_attribute_values(":partition", AttributeValue::B(Blob::new(start_key)))
            .expression_attribute_values(
                ":prefix",
                AttributeValue::B(Blob::new(prefixed_key_prefix)),
            )
            .set_exclusive_start_key(start_key_map)
            .send()
            .boxed_sync()
            .await?;
        Ok(response)
    }

    async fn read_value_bytes_general(
        &self,
        key_db: HashMap<String, AttributeValue>,
    ) -> Result<Option<Vec<u8>>, DynamoDbStoreInternalError> {
        let _guard = self.acquire().await;
        let response = self
            .client
            .get_item()
            .table_name(&self.namespace)
            .set_key(Some(key_db))
            .send()
            .boxed_sync()
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
    ) -> Result<bool, DynamoDbStoreInternalError> {
        let _guard = self.acquire().await;
        let response = self
            .client
            .get_item()
            .table_name(&self.namespace)
            .set_key(Some(key_db))
            .projection_expression(PARTITION_ATTRIBUTE)
            .send()
            .boxed_sync()
            .await?;

        Ok(response.item.is_some())
    }

    async fn get_list_responses(
        &self,
        attribute: &str,
        start_key: &[u8],
        key_prefix: &[u8],
    ) -> Result<QueryResponses, DynamoDbStoreInternalError> {
        check_key_size(key_prefix)?;
        let mut responses = Vec::new();
        let mut start_key_map = None;
        loop {
            let response = self
                .get_query_output(attribute, start_key, key_prefix, start_key_map)
                .await?;
            let last_evaluated = response.last_evaluated_key.clone();
            responses.push(response);
            match last_evaluated {
                None => {
                    break;
                }
                Some(value) => {
                    start_key_map = Some(value);
                }
            }
        }
        Ok(QueryResponses {
            prefix_len: key_prefix.len(),
            responses,
        })
    }

    async fn read_batch_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, DynamoDbStoreInternalError> {
        // Early return for empty keys
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut results = vec![None; keys.len()];

        // Build the request keys
        let mut request_keys = Vec::new();
        let mut key_to_index = HashMap::<Vec<u8>, Vec<usize>>::new();

        for (i, key) in keys.iter().enumerate() {
            check_key_size(key)?;
            let key_attrs = build_key(&self.start_key, key.clone());
            key_to_index.entry(key.clone()).or_default().push(i);
            request_keys.push(key_attrs);
        }

        let keys_and_attributes = KeysAndAttributes::builder()
            .set_keys(Some(request_keys))
            .build()?;

        let mut request_items = HashMap::new();
        request_items.insert(self.namespace.clone(), keys_and_attributes);

        // Execute batch get item request with retry for unprocessed keys
        let mut remaining_request_items = Some(request_items);

        while let Some(request_items) = remaining_request_items {
            // Skip if the request items are empty
            if request_items.is_empty() {
                break;
            }

            let _guard = self.acquire().await;
            let response = self
                .client
                .batch_get_item()
                .set_request_items(Some(request_items))
                .send()
                .boxed_sync()
                .await?;

            // Process returned items
            if let Some(mut responses) = response.responses {
                if let Some(items) = responses.remove(&self.namespace) {
                    for mut item in items {
                        // Extract key to find the original index
                        let key_attr = item
                            .get(KEY_ATTRIBUTE)
                            .ok_or(DynamoDbStoreInternalError::MissingKey)?;

                        if let AttributeValue::B(blob) = key_attr {
                            let prefixed_key = blob.as_ref();
                            let key = &prefixed_key[1..]; // Remove the [1] prefix
                            if let Some(indices) = key_to_index.get(key) {
                                if let Some((&last, rest)) = indices.split_last() {
                                    let value = extract_value_owned(&mut item)?;
                                    for index in rest {
                                        results[*index] = Some(value.clone());
                                    }
                                    results[last] = Some(value);
                                }
                            }
                        }
                    }
                }
            }

            // Handle unprocessed keys
            remaining_request_items = response.unprocessed_keys;
        }

        Ok(results)
    }
}

struct QueryResponses {
    prefix_len: usize,
    responses: Vec<QueryOutput>,
}

impl QueryResponses {
    fn keys(&self) -> impl Iterator<Item = Result<&[u8], DynamoDbStoreInternalError>> {
        self.responses
            .iter()
            .flat_map(|response| response.items.iter().flatten())
            .map(|item| extract_key(self.prefix_len, item))
    }

    fn key_values(
        &self,
    ) -> impl Iterator<Item = Result<(&[u8], &[u8]), DynamoDbStoreInternalError>> {
        self.responses
            .iter()
            .flat_map(|response| response.items.iter().flatten())
            .map(|item| extract_key_value(self.prefix_len, item))
    }
}

impl WithError for DynamoDbStoreInternal {
    type Error = DynamoDbStoreInternalError;
}

impl ReadableKeyValueStore for DynamoDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    fn root_key(&self) -> Result<Vec<u8>, DynamoDbStoreInternalError> {
        assert!(self.start_key.starts_with(EMPTY_ROOT_KEY));
        Ok(self.start_key[EMPTY_ROOT_KEY.len()..].to_vec())
    }

    async fn read_value_bytes(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, DynamoDbStoreInternalError> {
        check_key_size(key)?;
        let key_db = build_key(&self.start_key, key.to_vec());
        self.read_value_bytes_general(key_db).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, DynamoDbStoreInternalError> {
        check_key_size(key)?;
        let key_db = build_key(&self.start_key, key.to_vec());
        self.contains_key_general(key_db).await
    }

    async fn contains_keys(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<bool>, DynamoDbStoreInternalError> {
        let mut handles = Vec::new();
        for key in keys {
            check_key_size(key)?;
            let key_db = build_key(&self.start_key, key.clone());
            let handle = self.contains_key_general(key_db);
            handles.push(handle);
        }
        join_all(handles)
            .await
            .into_iter()
            .collect::<Result<_, _>>()
    }

    async fn read_multi_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, DynamoDbStoreInternalError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let handles = keys
            .chunks(MAX_BATCH_GET_ITEM_SIZE)
            .map(|key_batch| self.read_batch_values_bytes(key_batch));
        let results: Vec<_> = join_all(handles)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok(results.into_iter().flatten().collect())
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, DynamoDbStoreInternalError> {
        let result_queries = self
            .get_list_responses(KEY_ATTRIBUTE, &self.start_key, key_prefix)
            .await?;
        result_queries
            .keys()
            .map(|key| key.map(|k| k.to_vec()))
            .collect()
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DynamoDbStoreInternalError> {
        let result_queries = self
            .get_list_responses(KEY_VALUE_ATTRIBUTE, &self.start_key, key_prefix)
            .await?;
        result_queries
            .key_values()
            .map(|entry| entry.map(|(key, value)| (key.to_vec(), value.to_vec())))
            .collect()
    }
}

impl DirectWritableKeyValueStore for DynamoDbStoreInternal {
    const MAX_BATCH_SIZE: usize = MAX_TRANSACT_WRITE_ITEM_SIZE;
    const MAX_BATCH_TOTAL_SIZE: usize = MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE;
    const MAX_VALUE_SIZE: usize = VISIBLE_MAX_VALUE_SIZE;

    // DynamoDB does not support the `DeletePrefix` operation.
    type Batch = SimpleUnorderedBatch;

    async fn write_batch(&self, batch: Self::Batch) -> Result<(), DynamoDbStoreInternalError> {
        if !self.root_key_written.fetch_or(true, Ordering::SeqCst) {
            let mut builder = TransactionBuilder::new(PARTITION_KEY_ROOT_KEY);
            builder.insert_put_request(self.start_key.clone(), vec![], self)?;
            self.client
                .transact_write_items()
                .set_transact_items(Some(builder.transactions))
                .send()
                .boxed_sync()
                .await?;
        }
        let mut builder = TransactionBuilder::new(&self.start_key);
        for key in batch.deletions {
            builder.insert_delete_request(key, self)?;
        }
        for (key, value) in batch.insertions {
            builder.insert_put_request(key, value, self)?;
        }
        if !builder.transactions.is_empty() {
            let _guard = self.acquire().await;
            self.client
                .transact_write_items()
                .set_transact_items(Some(builder.transactions))
                .send()
                .boxed_sync()
                .await?;
        }
        Ok(())
    }

    async fn obtain_lease(
        &self,
        key: Vec<u8>,
        uuid: u64,
        ttl: std::time::Duration,
    ) -> Result<Result<(), LeaseConflict>, DynamoDbStoreInternalError> {
        check_key_size(&key)?;

        // Calculate expiration timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expiration = now + ttl.as_secs();

        // Build the key_db for potential error handling
        let key_db = build_key(&self.start_key, key.clone());

        // Build the lease item with separate UUID and expiration attributes
        let item = build_lease_item(&self.start_key, key, uuid, expiration);

        // Use condition expression to ensure atomic operation:
        // Allow write if:
        // 1. Item doesn't exist (attribute_not_exists), OR
        // 2. UUID matches (:uuid), OR
        // 3. Expiration has passed (< :now)
        let condition = format!(
            "attribute_not_exists({}) OR {} = :uuid OR {} < :now",
            LEASE_UUID_ATTRIBUTE, LEASE_UUID_ATTRIBUTE, LEASE_EXPIRATION_ATTRIBUTE
        );

        let result = self
            .client
            .put_item()
            .table_name(&self.namespace)
            .set_item(Some(item))
            .condition_expression(condition)
            .expression_attribute_values(":uuid", AttributeValue::N(uuid.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
            .send()
            .boxed_sync()
            .await;

        // Handle conditional check failure
        match result {
            Ok(_) => Ok(Ok(())),
            Err(err) => {
                // Check if this is a conditional check failure
                if let SdkError::ServiceError(ref service_err) = err {
                    if service_err.err().is_conditional_check_failed_exception() {
                        // Read the existing lease UUID and expiration for the error message
                        let response = self
                            .client
                            .get_item()
                            .table_name(&self.namespace)
                            .set_key(Some(key_db))
                            .projection_expression(format!(
                                "{}, {}",
                                LEASE_UUID_ATTRIBUTE, LEASE_EXPIRATION_ATTRIBUTE
                            ))
                            .send()
                            .boxed_sync()
                            .await?;

                        let (existing_uuid, expiration) = if let Some(item) = response.item {
                            let uuid = item
                                .get(LEASE_UUID_ATTRIBUTE)
                                .and_then(|attr| {
                                    if let AttributeValue::N(n) = attr {
                                        n.parse::<u64>().ok()
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(0);

                            let exp = item
                                .get(LEASE_EXPIRATION_ATTRIBUTE)
                                .and_then(|attr| {
                                    if let AttributeValue::N(n) = attr {
                                        n.parse::<u64>().ok()
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(0);

                            (uuid, exp)
                        } else {
                            (0, 0)
                        };

                        return Ok(Err(LeaseConflict {
                            uuid: existing_uuid,
                            timestamp: expiration,
                        }));
                    }
                }
                Err(err.into())
            }
        }
    }

    async fn discard_lease(
        &self,
        key: Vec<u8>,
        uuid: u64,
    ) -> Result<(), DynamoDbStoreInternalError> {
        check_key_size(&key)?;

        // Build the key for deletion
        let key_db = build_key(&self.start_key, key);

        // Use condition expression to ensure atomic operation:
        // Only delete if the UUID matches
        let condition = format!("{} = :uuid", LEASE_UUID_ATTRIBUTE);

        let result = self
            .client
            .delete_item()
            .table_name(&self.namespace)
            .set_key(Some(key_db.clone()))
            .condition_expression(condition)
            .expression_attribute_values(":uuid", AttributeValue::N(uuid.to_string()))
            .send()
            .boxed_sync()
            .await;

        // Handle conditional check failure
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                // Check if this is a conditional check failure
                if let SdkError::ServiceError(ref service_err) = err {
                    if service_err.err().is_conditional_check_failed_exception() {
                        tracing::warn!("Failed to discard lease");
                        return Ok(());
                    }
                }
                Err(err.into())
            }
        }
    }
}

/// Error when validating a namespace
#[derive(Debug, Error)]
pub enum InvalidNamespace {
    /// The namespace should be at least 3 characters.
    #[error("Namespace must have at least 3 characters")]
    TooShort,

    /// The namespace should be at most 63 characters.
    #[error("Namespace must be at most 63 characters")]
    TooLong,

    /// allowed characters are lowercase letters, numbers, periods and hyphens
    #[error("Namespace must only contain lowercase letters, numbers, periods and hyphens")]
    InvalidCharacter,
}

/// Errors that occur when using [`DynamoDbStoreInternal`].
#[derive(Debug, Error)]
pub enum DynamoDbStoreInternalError {
    /// An error occurred while getting the item.
    #[error(transparent)]
    Get(#[from] Box<SdkError<GetItemError>>),

    /// An error occurred while batch getting items.
    #[error(transparent)]
    BatchGet(#[from] Box<SdkError<BatchGetItemError>>),

    /// An error occurred while writing a transaction of items.
    #[error(transparent)]
    TransactWriteItem(#[from] Box<SdkError<TransactWriteItemsError>>),

    /// An error occurred while putting an item.
    #[error(transparent)]
    PutItem(#[from] Box<SdkError<PutItemError>>),

    /// An error occurred while deleting an item.
    #[error(transparent)]
    DeleteItem(#[from] Box<SdkError<DeleteItemError>>),

    /// An error occurred while doing a Query.
    #[error(transparent)]
    Query(#[from] Box<SdkError<QueryError>>),

    /// An error occurred while deleting a table
    #[error(transparent)]
    DeleteTable(#[from] Box<SdkError<DeleteTableError>>),

    /// An error occurred while listing tables
    #[error(transparent)]
    ListTables(#[from] Box<SdkError<ListTablesError>>),

    /// The transact maximum size is `MAX_TRANSACT_WRITE_ITEM_SIZE`.
    #[error("The transact must have length at most MAX_TRANSACT_WRITE_ITEM_SIZE")]
    TransactUpperLimitSize,

    /// The key must have at most 1024 bytes
    #[error("The key must have at most 1024 bytes")]
    KeyTooLong,

    /// The key prefix must have at most 1024 bytes
    #[error("The key prefix must have at most 1024 bytes")]
    KeyPrefixTooLong,

    /// The journal is not coherent
    #[error(transparent)]
    JournalConsistencyError(#[from] JournalConsistencyError),

    /// The length of the value should be at most 400 KB.
    #[error("The DynamoDB value should be less than 400 KB")]
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

    /// A wrong namespace error occurred
    #[error(transparent)]
    InvalidNamespace(#[from] InvalidNamespace),

    /// An error occurred while creating the table.
    #[error(transparent)]
    CreateTable(#[from] Box<SdkError<CreateTableError>>),

    /// An error occurred while building an object
    #[error(transparent)]
    Build(#[from] Box<BuildError>),
}

impl<InnerError> From<SdkError<InnerError>> for DynamoDbStoreInternalError
where
    DynamoDbStoreInternalError: From<Box<SdkError<InnerError>>>,
{
    fn from(error: SdkError<InnerError>) -> Self {
        Box::new(error).into()
    }
}

impl From<BuildError> for DynamoDbStoreInternalError {
    fn from(error: BuildError) -> Self {
        Box::new(error).into()
    }
}

impl DynamoDbStoreInternalError {
    /// Creates a [`DynamoDbStoreInternalError::WrongKeyType`] instance based on the returned value type.
    ///
    /// # Panics
    ///
    /// If the value type is in the correct type, a binary blob.
    pub fn wrong_key_type(value: &AttributeValue) -> Self {
        DynamoDbStoreInternalError::WrongKeyType(Self::type_description_of(value))
    }

    /// Creates a [`DynamoDbStoreInternalError::WrongValueType`] instance based on the returned value type.
    ///
    /// # Panics
    ///
    /// If the value type is in the correct type, a binary blob.
    pub fn wrong_value_type(value: &AttributeValue) -> Self {
        DynamoDbStoreInternalError::WrongValueType(Self::type_description_of(value))
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

impl KeyValueStoreError for DynamoDbStoreInternalError {
    const BACKEND: &'static str = "dynamo_db";
}

#[cfg(with_testing)]
impl TestKeyValueDatabase for JournalingKeyValueDatabase<DynamoDbDatabaseInternal> {
    async fn new_test_config() -> Result<DynamoDbStoreInternalConfig, DynamoDbStoreInternalError> {
        Ok(DynamoDbStoreInternalConfig {
            use_dynamodb_local: true,
            max_concurrent_queries: Some(TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES),
            max_stream_queries: TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
        })
    }
}

/// The combined error type for [`DynamoDbDatabase`].
pub type DynamoDbStoreError = ValueSplittingError<DynamoDbStoreInternalError>;

/// The config type for [`DynamoDbDatabase`]`
pub type DynamoDbStoreConfig = LruCachingConfig<DynamoDbStoreInternalConfig>;

/// A shared DB client for DynamoDB with metrics
#[cfg(with_metrics)]
pub type DynamoDbDatabase = MeteredDatabase<
    LruCachingDatabase<
        MeteredDatabase<
            ValueSplittingDatabase<
                MeteredDatabase<JournalingKeyValueDatabase<DynamoDbDatabaseInternal>>,
            >,
        >,
    >,
>;
/// A shared DB client for DynamoDB
#[cfg(not(with_metrics))]
pub type DynamoDbDatabase = LruCachingDatabase<
    ValueSplittingDatabase<JournalingKeyValueDatabase<DynamoDbDatabaseInternal>>,
>;

#[cfg(test)]
mod tests {
    use bcs::serialized_size;

    use crate::common::get_uleb128_size;

    #[test]
    fn test_serialization_len() {
        for n in [0, 10, 127, 128, 129, 16383, 16384, 20000] {
            let vec = vec![0u8; n];
            let est_size = get_uleb128_size(n) + n;
            let serial_size = serialized_size(&vec).unwrap();
            assert_eq!(est_size, serial_size);
        }
    }
}
