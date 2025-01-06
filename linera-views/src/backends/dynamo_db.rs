// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the DynamoDB database.

use std::{collections::HashMap, env, sync::Arc};

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
use futures::future::{join_all, FutureExt as _};
use linera_base::ensure;
use thiserror::Error;

#[cfg(with_metrics)]
use crate::metering::MeteredStore;
#[cfg(with_testing)]
use crate::store::TestKeyValueStore;
use crate::{
    batch::SimpleUnorderedBatch,
    common::get_uleb128_size,
    journaling::{DirectWritableKeyValueStore, JournalConsistencyError, JournalingKeyValueStore},
    lru_caching::{LruCachingConfig, LruCachingStore},
    store::{
        AdminKeyValueStore, CommonStoreInternalConfig, KeyIterable, KeyValueIterable,
        KeyValueStoreError, ReadableKeyValueStore, WithError,
    },
    value_splitting::{ValueSplittingError, ValueSplittingStore},
};

/// Name of the environment variable with the address to a LocalStack instance.
const LOCALSTACK_ENDPOINT: &str = "LOCALSTACK_ENDPOINT";

/// The configuration to connect to DynamoDB.
pub type Config = aws_sdk_dynamodb::Config;

/// Gets the AWS configuration from the environment
async fn get_base_config() -> Result<aws_sdk_dynamodb::Config, DynamoDbStoreInternalError> {
    let base_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest())
        .boxed()
        .await;
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
async fn get_localstack_config() -> Result<aws_sdk_dynamodb::Config, DynamoDbStoreInternalError> {
    let base_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest())
        .boxed()
        .await;
    let endpoint_address = get_endpoint_address().unwrap();
    let config = aws_sdk_dynamodb::config::Builder::from(&base_config)
        .endpoint_url(endpoint_address)
        .build();
    Ok(config)
}

/// Getting a configuration for the system
async fn get_config_internal(
    use_localstack: bool,
) -> Result<aws_sdk_dynamodb::Config, DynamoDbStoreInternalError> {
    if use_localstack {
        get_localstack_config().await
    } else {
        get_base_config().await
    }
}

/// The attribute name of the partition key.
const PARTITION_ATTRIBUTE: &str = "item_partition";

/// A root key being used for testing existence of tables
const EMPTY_ROOT_KEY: &[u8] = &[];

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
/// `1 + 1 + serialized_size(1024)? + serialized_size(x)? <= 400*1024`
/// and so this simplifies to `1 + 1 + (2 + 1024) + (3 + x) <= 400 * 1024`
/// Note on the following formula:
/// * We write 3 because get_uleb128_size(400*1024) = 3
/// * We write `1 + 1` because the `SimpleUnorderedBatch` has two entries
///
/// This gets us a maximal value of 408569;
const VISIBLE_MAX_VALUE_SIZE: usize = RAW_MAX_VALUE_SIZE
    - MAX_KEY_SIZE
    - get_uleb128_size(RAW_MAX_VALUE_SIZE)
    - get_uleb128_size(MAX_KEY_SIZE)
    - 1
    - 1;

/// Fundamental constant in DynamoDB: The maximum size of a key is 1024 bytes
/// See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
const MAX_KEY_SIZE: usize = 1024;

/// Fundamental constants in DynamoDB: The maximum size of a TransactWriteItem is 4M.
/// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
/// We're taking a conservative value because the mode of computation is unclear.
const MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE: usize = 4000000;

/// The DynamoDb database is potentially handling an infinite number of connections.
/// However, for testing or some other purpose we really need to decrease the number of
/// connections.
#[cfg(with_testing)]
const TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of entries in a stream of the tests can be controlled by this parameter for tests.
#[cfg(with_testing)]
const TEST_DYNAMO_DB_MAX_STREAM_QUERIES: usize = 10;

/// Fundamental constants in DynamoDB: The maximum size of a TransactWriteItem is 100.
/// See <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html>
const MAX_TRANSACT_WRITE_ITEM_SIZE: usize = 100;

/// Keys of length 0 are not allowed, so we extend by having a prefix on start
fn extend_root_key(root_key: &[u8]) -> Vec<u8> {
    let mut vec = vec![0];
    vec.extend(root_key);
    vec
}

/// Builds the key attributes for a table item.
///
/// The key is composed of two attributes that are both binary blobs. The first attribute is a
/// partition key and is currently just a dummy value that ensures all items are in the same
/// partion. This is necessary for range queries to work correctly.
///
/// The second attribute is the actual key value, which is generated by concatenating the
/// context prefix. The Vec<u8> expression is obtained from self.derive_key.
fn build_key(root_key: &[u8], key: Vec<u8>) -> HashMap<String, AttributeValue> {
    let big_root = extend_root_key(root_key);
    [
        (
            PARTITION_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(big_root)),
        ),
        (KEY_ATTRIBUTE.to_owned(), AttributeValue::B(Blob::new(key))),
    ]
    .into()
}

/// Builds the value attribute for storing a table item.
fn build_key_value(
    root_key: &[u8],
    key: Vec<u8>,
    value: Vec<u8>,
) -> HashMap<String, AttributeValue> {
    let big_root = extend_root_key(root_key);
    [
        (
            PARTITION_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(big_root)),
        ),
        (KEY_ATTRIBUTE.to_owned(), AttributeValue::B(Blob::new(key))),
        (
            VALUE_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(value)),
        ),
    ]
    .into()
}

/// Checks that a key is of the correct size
fn check_key_size(key: &[u8]) -> Result<(), DynamoDbStoreInternalError> {
    ensure!(!key.is_empty(), DynamoDbStoreInternalError::ZeroLengthKey);
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
        AttributeValue::B(blob) => Ok(&blob.as_ref()[prefix_len..]),
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

/// Extracts the `(key, value)` pair attributes from an item (returned by value).
fn extract_key_value_owned(
    prefix_len: usize,
    attributes: &mut HashMap<String, AttributeValue>,
) -> Result<(Vec<u8>, Vec<u8>), DynamoDbStoreInternalError> {
    let key = extract_key(prefix_len, attributes)?.to_vec();
    let value = extract_value_owned(attributes)?;
    Ok((key, value))
}

struct TransactionBuilder {
    root_key: Vec<u8>,
    transacts: Vec<TransactWriteItem>,
}

impl TransactionBuilder {
    fn new(root_key: &[u8]) -> Self {
        let root_key = root_key.to_vec();
        let transacts = Vec::new();
        Self {
            root_key,
            transacts,
        }
    }

    fn insert_delete_request(
        &mut self,
        key: Vec<u8>,
        store: &DynamoDbStoreInternal,
    ) -> Result<(), DynamoDbStoreInternalError> {
        let transact = store.build_delete_transact(&self.root_key, key)?;
        self.transacts.push(transact);
        Ok(())
    }

    fn insert_put_request(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        store: &DynamoDbStoreInternal,
    ) -> Result<(), DynamoDbStoreInternalError> {
        let transact = store.build_put_transact(&self.root_key, key, value)?;
        self.transacts.push(transact);
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
    root_key: Vec<u8>,
}

/// The initial configuration of the system
#[derive(Debug)]
pub struct DynamoDbStoreInternalConfig {
    /// The AWS configuration
    config: aws_sdk_dynamodb::Config,
    /// The common configuration of the key value store
    common_config: CommonStoreInternalConfig,
}

impl AdminKeyValueStore for DynamoDbStoreInternal {
    type Config = DynamoDbStoreInternalConfig;

    fn get_name() -> String {
        "dynamodb internal".to_string()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, DynamoDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let client = Client::from_conf(config.config.clone());
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.common_config.max_stream_queries;
        let namespace = namespace.to_string();
        let root_key = root_key.to_vec();
        Ok(Self {
            client,
            namespace,
            semaphore,
            max_stream_queries,
            root_key,
        })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, DynamoDbStoreInternalError> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        let root_key = root_key.to_vec();
        Ok(Self {
            client,
            namespace,
            semaphore,
            max_stream_queries,
            root_key,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, DynamoDbStoreInternalError> {
        let client = Client::from_conf(config.config.clone());
        let mut namespaces = Vec::new();
        let mut start_table = None;
        loop {
            let response = client
                .list_tables()
                .set_exclusive_start_table_name(start_table)
                .send()
                .boxed()
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

    async fn delete_all(config: &Self::Config) -> Result<(), DynamoDbStoreInternalError> {
        let client = Client::from_conf(config.config.clone());
        let tables = Self::list_all(config).await?;
        for table in tables {
            client
                .delete_table()
                .table_name(&table)
                .send()
                .boxed()
                .await?;
        }
        Ok(())
    }

    async fn exists(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<bool, DynamoDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let client = Client::from_conf(config.config.clone());
        let key_db = build_key(EMPTY_ROOT_KEY, DB_KEY.to_vec());
        let response = client
            .get_item()
            .table_name(namespace)
            .set_key(Some(key_db))
            .send()
            .boxed()
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
        let client = Client::from_conf(config.config.clone());
        let _result = client
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
            .boxed()
            .await?;
        Ok(())
    }

    async fn delete(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), DynamoDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let client = Client::from_conf(config.config.clone());
        client
            .delete_table()
            .table_name(namespace)
            .send()
            .boxed()
            .await?;
        Ok(())
    }
}

impl DynamoDbStoreInternal {
    /// Namespaces are named table names in DynamoDb [naming
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

    fn build_delete_transact(
        &self,
        root_key: &[u8],
        key: Vec<u8>,
    ) -> Result<TransactWriteItem, DynamoDbStoreInternalError> {
        check_key_size(&key)?;
        let request = Delete::builder()
            .table_name(&self.namespace)
            .set_key(Some(build_key(root_key, key)))
            .build()?;
        Ok(TransactWriteItem::builder().delete(request).build())
    }

    fn build_put_transact(
        &self,
        root_key: &[u8],
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
            .set_item(Some(build_key_value(root_key, key, value)))
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
        root_key: &[u8],
        key_prefix: &[u8],
        start_key_map: Option<HashMap<String, AttributeValue>>,
    ) -> Result<QueryOutput, DynamoDbStoreInternalError> {
        let _guard = self.acquire().await;
        let big_root = extend_root_key(root_key);
        let response = self
            .client
            .query()
            .table_name(&self.namespace)
            .projection_expression(attribute_str)
            .key_condition_expression(format!(
                "{PARTITION_ATTRIBUTE} = :partition and begins_with({KEY_ATTRIBUTE}, :prefix)"
            ))
            .expression_attribute_values(":partition", AttributeValue::B(Blob::new(big_root)))
            .expression_attribute_values(":prefix", AttributeValue::B(Blob::new(key_prefix)))
            .set_exclusive_start_key(start_key_map)
            .send()
            .boxed()
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
            .boxed()
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
            .boxed()
            .await?;

        Ok(response.item.is_some())
    }

    async fn get_list_responses(
        &self,
        attribute: &str,
        root_key: &[u8],
        key_prefix: &[u8],
    ) -> Result<QueryResponses, DynamoDbStoreInternalError> {
        check_key_size(key_prefix)?;
        let mut responses = Vec::new();
        let mut start_key = None;
        loop {
            let response = self
                .get_query_output(attribute, root_key, key_prefix, start_key)
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
        Ok(QueryResponses {
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
#[expect(clippy::type_complexity)]
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
    type Item = Result<&'a [u8], DynamoDbStoreInternalError>;

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

impl KeyIterable<DynamoDbStoreInternalError> for DynamoDbKeys {
    type Iterator<'a>
        = DynamoDbKeyBlockIterator<'a>
    where
        Self: 'a;

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
#[expect(clippy::type_complexity)]
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
    type Item = Result<(&'a [u8], &'a [u8]), DynamoDbStoreInternalError>;

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
#[expect(clippy::type_complexity)]
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
    type Item = Result<(Vec<u8>, Vec<u8>), DynamoDbStoreInternalError>;

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

impl KeyValueIterable<DynamoDbStoreInternalError> for DynamoDbKeyValues {
    type Iterator<'a>
        = DynamoDbKeyValueIterator<'a>
    where
        Self: 'a;
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

impl WithError for DynamoDbStoreInternal {
    type Error = DynamoDbStoreInternalError;
}

impl ReadableKeyValueStore for DynamoDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = DynamoDbKeys;
    type KeyValues = DynamoDbKeyValues;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, DynamoDbStoreInternalError> {
        check_key_size(key)?;
        let key_db = build_key(&self.root_key, key.to_vec());
        self.read_value_bytes_general(key_db).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, DynamoDbStoreInternalError> {
        check_key_size(key)?;
        let key_db = build_key(&self.root_key, key.to_vec());
        self.contains_key_general(key_db).await
    }

    async fn contains_keys(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, DynamoDbStoreInternalError> {
        let mut handles = Vec::new();
        for key in keys {
            check_key_size(&key)?;
            let key_db = build_key(&self.root_key, key);
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
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, DynamoDbStoreInternalError> {
        let mut handles = Vec::new();
        for key in keys {
            check_key_size(&key)?;
            let key_db = build_key(&self.root_key, key);
            let handle = self.read_value_bytes_general(key_db);
            handles.push(handle);
        }
        join_all(handles)
            .await
            .into_iter()
            .collect::<Result<_, _>>()
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<DynamoDbKeys, DynamoDbStoreInternalError> {
        let result_queries = self
            .get_list_responses(KEY_ATTRIBUTE, &self.root_key, key_prefix)
            .await?;
        Ok(DynamoDbKeys { result_queries })
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<DynamoDbKeyValues, DynamoDbStoreInternalError> {
        let result_queries = self
            .get_list_responses(KEY_VALUE_ATTRIBUTE, &self.root_key, key_prefix)
            .await?;
        Ok(DynamoDbKeyValues { result_queries })
    }
}

#[async_trait]
impl DirectWritableKeyValueStore for DynamoDbStoreInternal {
    const MAX_BATCH_SIZE: usize = MAX_TRANSACT_WRITE_ITEM_SIZE;
    const MAX_BATCH_TOTAL_SIZE: usize = MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE;
    const MAX_VALUE_SIZE: usize = VISIBLE_MAX_VALUE_SIZE;

    // DynamoDB does not support the `DeletePrefix` operation.
    type Batch = SimpleUnorderedBatch;

    async fn write_batch(&self, batch: Self::Batch) -> Result<(), DynamoDbStoreInternalError> {
        let mut builder = TransactionBuilder::new(&self.root_key);
        for key in batch.deletions {
            builder.insert_delete_request(key, self)?;
        }
        for (key, value) in batch.insertions {
            builder.insert_put_request(key, value, self)?;
        }
        if !builder.transacts.is_empty() {
            let _guard = self.acquire().await;
            self.client
                .transact_write_items()
                .set_transact_items(Some(builder.transacts))
                .send()
                .boxed()
                .await?;
        }
        Ok(())
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

    /// The journal is not coherent
    #[error(transparent)]
    JournalConsistencyError(#[from] JournalConsistencyError),

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

    /// A wrong namespace error occurred
    #[error(transparent)]
    InvalidNamespace(#[from] InvalidNamespace),

    /// An error occurred while creating the table.
    #[error(transparent)]
    CreateTable(#[from] SdkError<CreateTableError>),

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
impl TestKeyValueStore for JournalingKeyValueStore<DynamoDbStoreInternal> {
    async fn new_test_config() -> Result<DynamoDbStoreInternalConfig, DynamoDbStoreInternalError> {
        let common_config = CommonStoreInternalConfig {
            max_concurrent_queries: Some(TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES),
            max_stream_queries: TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
        };
        let use_localstack = true;
        let config = get_config_internal(use_localstack).await?;
        Ok(DynamoDbStoreInternalConfig {
            config,
            common_config,
        })
    }
}

/// A shared DB client for DynamoDb implementing LruCaching and metrics
#[cfg(with_metrics)]
pub type DynamoDbStore = MeteredStore<
    LruCachingStore<
        MeteredStore<
            ValueSplittingStore<MeteredStore<JournalingKeyValueStore<DynamoDbStoreInternal>>>,
        >,
    >,
>;

/// A shared DB client for DynamoDb implementing LruCaching
#[cfg(not(with_metrics))]
pub type DynamoDbStore =
    LruCachingStore<ValueSplittingStore<JournalingKeyValueStore<DynamoDbStoreInternal>>>;

/// The combined error type for the `DynamoDbStore`.
pub type DynamoDbStoreError = ValueSplittingError<DynamoDbStoreInternalError>;

/// The config type for DynamoDbStore
pub type DynamoDbStoreConfig = LruCachingConfig<DynamoDbStoreInternalConfig>;

/// Getting a configuration for the system
pub async fn get_config(use_localstack: bool) -> Result<Config, DynamoDbStoreError> {
    Ok(get_config_internal(use_localstack).await?)
}

impl DynamoDbStoreConfig {
    /// Creates a `DynamoDbStoreConfig` from the input.
    pub fn new(
        config: Config,
        common_config: crate::store::CommonStoreConfig,
    ) -> DynamoDbStoreConfig {
        let inner_config = DynamoDbStoreInternalConfig {
            config,
            common_config: common_config.reduced(),
        };
        DynamoDbStoreConfig {
            inner_config,
            cache_size: common_config.cache_size,
        }
    }
}

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
