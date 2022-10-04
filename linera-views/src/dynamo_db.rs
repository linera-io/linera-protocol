// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    hash::HashingContext,
    localstack,
    views::{
        CollectionOperations, Context, LogOperations, MapOperations, QueueOperations,
        RegisterOperations, ScopedOperations, ViewError,
    },
};
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    model::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    },
    types::{Blob, SdkError},
    Client,
};
use linera_base::ensure;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, io, ops::Range, str::FromStr, sync::Arc};
use thiserror::Error;
use tokio::sync::OwnedMutexGuard;

/// The configuration to connect to DynamoDB.
pub use aws_sdk_dynamodb::Config;

#[cfg(test)]
#[path = "unit_tests/dynamo_db_context_tests.rs"]
pub mod dynamo_db_context_tests;

/// The attribute name of the partition key.
const PARTITION_ATTRIBUTE: &str = "item_partition";

/// A dummy value to use as the partition key.
const DUMMY_PARTITION_KEY: &[u8] = &[0];

/// The attribute name of the primary key (used as a sort key).
const KEY_ATTRIBUTE: &str = "item_key";

/// The attribute name of the table value blob.
const VALUE_ATTRIBUTE: &str = "item_value";

/// A implementation of [`Context`] based on DynamoDB.
#[derive(Debug, Clone)]
pub struct DynamoDbContext<E> {
    client: Client,
    table: TableName,
    lock: Arc<OwnedMutexGuard<()>>,
    key_prefix: Vec<u8>,
    extra: E,
}

impl<E> DynamoDbContext<E> {
    /// Create a new [`DynamoDbContext`] instance.
    pub async fn new(
        table: TableName,
        lock: OwnedMutexGuard<()>,
        key_prefix: Vec<u8>,
        extra: E,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        let config = aws_config::load_from_env().await;

        DynamoDbContext::from_config(&config, table, lock, key_prefix, extra).await
    }

    /// Create a new [`DynamoDbContext`] instance using the provided `config` parameters.
    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
        lock: OwnedMutexGuard<()>,
        key_prefix: Vec<u8>,
        extra: E,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        let storage = DynamoDbContext {
            client: Client::from_conf(config.into()),
            table,
            lock: Arc::new(lock),
            key_prefix,
            extra,
        };

        let table_status = storage.create_table_if_needed().await?;

        Ok((storage, table_status))
    }

    /// Create a new [`DynamoDbContext`] instance using a LocalStack endpoint.
    ///
    /// Requires a [`LOCALSTACK_ENDPOINT`] environment variable with the endpoint address to connect
    /// to the LocalStack instance. Creates the table if it doesn't exist yet, reporting a
    /// [`TableStatus`] to indicate if the table was created or if it already exists.
    pub async fn with_localstack(
        table: TableName,
        lock: OwnedMutexGuard<()>,
        key_prefix: Vec<u8>,
        extra: E,
    ) -> Result<(Self, TableStatus), LocalStackError> {
        let base_config = aws_config::load_from_env().await;
        let config = aws_sdk_dynamodb::config::Builder::from(&base_config)
            .endpoint_resolver(localstack::get_endpoint()?)
            .build();

        Ok(DynamoDbContext::from_config(config, table, lock, key_prefix, extra).await?)
    }

    /// Clone this [`DynamoDbContext`] while entering a sub-scope.
    ///
    /// The return context uses the `new_lock` instead of the current internal lock, and has its key
    /// prefix extended with `scope_prefix` and uses the `new_extra` instead of cloning the current
    /// extra data.
    pub fn clone_with_sub_scope<NewE>(
        &self,
        new_lock: OwnedMutexGuard<()>,
        scope_prefix: &impl Serialize,
        new_extra: NewE,
    ) -> DynamoDbContext<NewE> {
        DynamoDbContext {
            client: self.client.clone(),
            table: self.table.clone(),
            lock: Arc::new(new_lock),
            key_prefix: self.extend_prefix(scope_prefix),
            extra: new_extra,
        }
    }

    /// Create the storage table if it doesn't exist.
    ///
    /// Attempts to create the table and ignores errors that indicate that it already exists.
    async fn create_table_if_needed(&self) -> Result<TableStatus, CreateTableError> {
        let result = self
            .client
            .create_table()
            .table_name(self.table.as_ref())
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(PARTITION_ATTRIBUTE)
                    .attribute_type(ScalarAttributeType::B)
                    .build(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(KEY_ATTRIBUTE)
                    .attribute_type(ScalarAttributeType::B)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(PARTITION_ATTRIBUTE)
                    .key_type(KeyType::Hash)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(KEY_ATTRIBUTE)
                    .key_type(KeyType::Range)
                    .build(),
            )
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(10)
                    .write_capacity_units(10)
                    .build(),
            )
            .send()
            .await;

        match result {
            Ok(_) => Ok(TableStatus::New),
            Err(error) if error.is_resource_in_use_exception() => Ok(TableStatus::Existing),
            Err(error) => Err(error.into()),
        }
    }

    /// Extend the current key prefix with the provided marker, returning a new key prefix.
    ///
    /// This is used to enter a sub-view's scope.
    fn extend_prefix(&self, marker: &impl Serialize) -> Vec<u8> {
        [
            self.key_prefix.as_slice(),
            &bcs::to_bytes(marker).expect("Failed to serialize marker to extend key prefix"),
        ]
        .concat()
    }

    /// Build the key attributes for a table item.
    ///
    /// The key is composed of two attributes that are both binary blobs. The first attribute is a
    /// partition key and is currently just a dummy value that ensures all items are in the same
    /// partion. This is necessary for range queries to work correctly.
    ///
    /// The second attribute is the actual key value, which is generated by concatenating the
    /// context prefix with the bytes obtained from serializing `key` using [`bcs`].
    fn build_key(&self, key: &impl Serialize) -> HashMap<String, AttributeValue> {
        [
            (
                PARTITION_ATTRIBUTE.to_owned(),
                AttributeValue::B(Blob::new(DUMMY_PARTITION_KEY)),
            ),
            (
                KEY_ATTRIBUTE.to_owned(),
                AttributeValue::B(Blob::new(self.extend_prefix(key))),
            ),
        ]
        .into()
    }

    /// Build the value attribute for storing a table item.
    fn build_value(&self, value: &impl Serialize) -> (String, AttributeValue) {
        (
            VALUE_ATTRIBUTE.to_owned(),
            AttributeValue::B(Blob::new(
                bcs::to_bytes(value).expect("Serialization failed"),
            )),
        )
    }

    /// Retrieve a generic `Item` from the table using the provided `key` prefixed by the current
    /// context.
    ///
    /// The `Item` is deserialized using [`bcs`].
    async fn get_item<Item>(
        &mut self,
        key: &impl Serialize,
    ) -> Result<Option<Item>, DynamoDbContextError>
    where
        Item: DeserializeOwned,
    {
        let response = self
            .client
            .get_item()
            .table_name(self.table.as_ref())
            .set_key(Some(self.build_key(key)))
            .send()
            .await?;

        match response.item() {
            Some(item) => Ok(Some(Self::extract_value(item)?)),
            None => Ok(None),
        }
    }

    /// Extract the key attribute from an item and deserialize it into the `Key` type.
    fn extract_key<Key>(
        &self,
        attributes: &HashMap<String, AttributeValue>,
        extra_bytes_to_skip: Option<usize>,
    ) -> Result<Key, DynamoDbContextError>
    where
        Key: DeserializeOwned,
    {
        Self::extract_attribute(
            attributes,
            KEY_ATTRIBUTE,
            Some(self.key_prefix.len() + extra_bytes_to_skip.unwrap_or(0)),
            DynamoDbContextError::MissingKey,
            DynamoDbContextError::wrong_key_type,
            DynamoDbContextError::KeyDeserialization,
        )
    }

    /// Extract the value attribute from an item and deserialize it into the `Value` type.
    fn extract_value<Value>(
        attributes: &HashMap<String, AttributeValue>,
    ) -> Result<Value, DynamoDbContextError>
    where
        Value: DeserializeOwned,
    {
        Self::extract_attribute(
            attributes,
            VALUE_ATTRIBUTE,
            None,
            DynamoDbContextError::MissingValue,
            DynamoDbContextError::wrong_value_type,
            DynamoDbContextError::ValueDeserialization,
        )
    }

    /// Extract the requested `attribute` from an item and deserialize it into the `Data` type.
    ///
    /// # Parameters
    ///
    /// - `attributes`: the attributes of the item
    /// - `attribute`: the attribute to extract
    /// - `bytes_to_skip`: the number of bytes from the value blob to ignore before attempting to
    ///   deserialize it
    /// - `missing_error`: error to return if the requested attribute is missing
    /// - `type_error`: error to return if the attribute value is not a binary blob
    /// - `deserialization_error`: error to return if the attribute value blob can't be
    ///   deserialized into a `Data` instance
    fn extract_attribute<Data>(
        attributes: &HashMap<String, AttributeValue>,
        attribute: &str,
        bytes_to_skip: Option<usize>,
        missing_error: DynamoDbContextError,
        type_error: impl FnOnce(&AttributeValue) -> DynamoDbContextError,
        deserialization_error: impl FnOnce(bcs::Error) -> DynamoDbContextError,
    ) -> Result<Data, DynamoDbContextError>
    where
        Data: DeserializeOwned,
    {
        let bytes = attributes
            .get(attribute)
            .ok_or(missing_error)?
            .as_b()
            .map_err(type_error)?
            .as_ref()
            .to_owned();
        let data_start = bytes_to_skip.unwrap_or(0);
        let data_bytes = &bytes[data_start..];

        bcs::from_bytes(data_bytes).map_err(deserialization_error)
    }

    /// Store a generic `value` into the table using the provided `key` prefixed by the current
    /// context.
    ///
    /// The value is serialized using [`bcs`].
    async fn put_item(
        &self,
        key: &impl Serialize,
        value: &impl Serialize,
    ) -> Result<(), DynamoDbContextError> {
        let mut item = self.build_key(key);
        item.extend([self.build_value(value)]);

        self.client
            .put_item()
            .table_name(self.table.as_ref())
            .set_item(Some(item))
            .send()
            .await?;

        Ok(())
    }

    /// Remove an item with the provided `key` prefixed with `prefix` from the table.
    async fn remove_item(&self, key: &impl Serialize) -> Result<(), DynamoDbContextError> {
        self.client
            .delete_item()
            .table_name(self.table.as_ref())
            .set_key(Some(self.build_key(key)))
            .send()
            .await?;

        Ok(())
    }

    /// Query the table for the keys that are prefixed by the current context.
    ///
    /// # Panics
    ///
    /// If the raw key bytes can't be deserialized into a `Key`.
    async fn get_sub_keys<Key, ExtraSuffix>(
        &mut self,
        extra_suffix_for_key_prefix: &ExtraSuffix,
    ) -> Result<Vec<Key>, DynamoDbContextError>
    where
        Key: DeserializeOwned,
        ExtraSuffix: Serialize,
    {
        let extra_prefix_bytes =
            bcs::to_bytes(extra_suffix_for_key_prefix).expect("Failed to serialize suffix");
        let extra_prefix_bytes_count = extra_prefix_bytes.len();

        let mut prefix_bytes = self.key_prefix.clone();
        prefix_bytes.extend(extra_prefix_bytes);

        let response = self
            .client
            .query()
            .table_name(self.table.as_ref())
            .projection_expression(KEY_ATTRIBUTE)
            .key_condition_expression(format!(
                "{PARTITION_ATTRIBUTE} = :partition and begins_with({KEY_ATTRIBUTE}, :prefix)"
            ))
            .expression_attribute_values(
                ":partition",
                AttributeValue::B(Blob::new(DUMMY_PARTITION_KEY)),
            )
            .expression_attribute_values(":prefix", AttributeValue::B(Blob::new(prefix_bytes)))
            .send()
            .await?;

        response
            .items()
            .into_iter()
            .flatten()
            .map(|item| self.extract_key(item, Some(extra_prefix_bytes_count)))
            .collect::<Result<_, _>>()
    }
}

#[async_trait]
impl<E> Context for DynamoDbContext<E>
where
    E: Clone + Send + Sync,
{
    type Batch = ();
    type Extra = E;
    type Error = DynamoDbContextError;

    fn extra(&self) -> &E {
        &self.extra
    }

    async fn run_with_batch<F>(&self, builder: F) -> Result<(), Self::Error>
    where
        F: FnOnce(&mut Self::Batch) -> futures::future::BoxFuture<Result<(), Self::Error>>
            + Send
            + Sync,
    {
        builder(&mut ()).await
    }
}

#[async_trait]
impl<E> ScopedOperations for DynamoDbContext<E>
where
    E: Clone + Send + Sync,
{
    fn clone_with_scope(&self, index: u64) -> Self {
        DynamoDbContext {
            client: self.client.clone(),
            table: self.table.clone(),
            lock: self.lock.clone(),
            key_prefix: self.extend_prefix(&index),
            extra: self.extra.clone(),
        }
    }
}

#[async_trait]
impl<E, T> RegisterOperations<T> for DynamoDbContext<E>
where
    T: Default + Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    async fn get(&mut self) -> Result<T, Self::Error> {
        let value = self.get_item(&()).await?.unwrap_or_default();
        Ok(value)
    }

    async fn set(&mut self, _batch: &mut Self::Batch, value: T) -> Result<(), Self::Error> {
        self.put_item(&(), &value).await?;
        Ok(())
    }

    async fn delete(&mut self, _batch: &mut Self::Batch) -> Result<(), Self::Error> {
        self.remove_item(&()).await?;
        Ok(())
    }
}

#[async_trait]
impl<E, T> LogOperations<T> for DynamoDbContext<E>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    async fn count(&mut self) -> Result<usize, Self::Error> {
        let count = self.get_item(&()).await?.unwrap_or_default();
        Ok(count)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        self.get_item(&index).await
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut items = Vec::with_capacity(range.len());
        for index in range {
            let item = match self.get_item(&index).await? {
                Some(item) => item,
                None => return Ok(items),
            };
            items.push(item);
        }
        Ok(items)
    }

    async fn append(
        &mut self,
        stored_count: usize,
        _batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        let mut count = stored_count;
        for value in values {
            self.put_item(&count, &value).await?;
            count += 1;
        }
        self.put_item(&(), &count).await?;
        Ok(())
    }

    async fn delete(
        &mut self,
        stored_count: usize,
        _batch: &mut Self::Batch,
    ) -> Result<(), Self::Error> {
        self.remove_item(&()).await?;
        for index in 0..stored_count {
            self.remove_item(&index).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<E, T> QueueOperations<T> for DynamoDbContext<E>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error> {
        let range = self.get_item(&()).await?.unwrap_or_default();
        Ok(range)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        Ok(self.get_item(&index).await?)
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::new();
        for index in range {
            match self.get_item(&index).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    async fn delete_front(
        &mut self,
        stored_indices: &mut Range<usize>,
        _batch: &mut Self::Batch,
        count: usize,
    ) -> Result<(), Self::Error> {
        let deletion_range = stored_indices.clone().take(count);
        stored_indices.start += count;
        self.put_item(&(), &stored_indices).await?;
        for index in deletion_range {
            self.remove_item(&index).await?;
        }
        Ok(())
    }

    async fn append_back(
        &mut self,
        stored_indices: &mut Range<usize>,
        _batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        for value in values {
            self.put_item(&stored_indices.end, &value).await?;
            stored_indices.end += 1;
        }
        self.put_item(&(), &stored_indices).await
    }

    async fn delete(
        &mut self,
        stored_indices: Range<usize>,
        _batch: &mut Self::Batch,
    ) -> Result<(), Self::Error> {
        self.remove_item(&()).await?;
        for index in stored_indices {
            self.remove_item(&index).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<E, I, V> MapOperations<I, V> for DynamoDbContext<E>
where
    I: Eq + Ord + Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    async fn get(&mut self, index: &I) -> Result<Option<V>, Self::Error> {
        Ok(self.get_item(&index).await?)
    }

    async fn insert(
        &mut self,
        _batch: &mut Self::Batch,
        index: I,
        value: V,
    ) -> Result<(), Self::Error> {
        self.put_item(&index, &value).await?;
        Ok(())
    }

    async fn remove(&mut self, _batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        self.remove_item(&index).await?;
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        self.get_sub_keys(&()).await
    }

    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send,
    {
        for index in self.get_sub_keys(&()).await? {
            f(index);
        }
        Ok(())
    }

    async fn delete(&mut self, _batch: &mut Self::Batch) -> Result<(), Self::Error> {
        for key in self.get_sub_keys::<I, _>(&()).await? {
            self.remove_item(&key).await?;
        }

        Ok(())
    }
}

impl<E> HashingContext for DynamoDbContext<E>
where
    E: Clone + Send + Sync,
{
    type Hasher = sha2::Sha512;
}

/// A marker type used to distinguish keys from the current scope from the keys of sub-views.
///
/// Sub-views in a collection share a common key prefix, like in other view types. However,
/// just concatenating the shared prefix with sub-view keys makes it impossible to distinguish if a
/// given key belongs to child sub-view or a grandchild sub-view (consider for example if a
/// collection is stored inside the collection).
///
/// The solution to this is to use a marker type to have two sets of keys, where
/// [`CollectionKey::Index`] serves to indicate the existence of an entry in the collection, and
/// [`CollectionKey::Subvie`] serves as the prefix for the sub-view.
#[derive(Serialize)]
enum CollectionKey<I> {
    Index(I),
    Subview(I),
}

#[async_trait]
impl<E, I> CollectionOperations<I> for DynamoDbContext<E>
where
    I: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    fn clone_with_scope(&self, index: &I) -> Self {
        DynamoDbContext {
            client: self.client.clone(),
            table: self.table.clone(),
            lock: self.lock.clone(),
            key_prefix: self.extend_prefix(&CollectionKey::Subview(index)),
            extra: self.extra.clone(),
        }
    }

    async fn add_index(&mut self, _batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        self.put_item(&CollectionKey::Index(index), &()).await
    }

    async fn remove_index(
        &mut self,
        _batch: &mut Self::Batch,
        index: I,
    ) -> Result<(), Self::Error> {
        self.remove_item(&CollectionKey::Index(index)).await
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        self.get_sub_keys(&CollectionKey::Index(())).await
    }
}

/// Status of a table at the creation time of a [`DynamoDbContext`] instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableStatus {
    /// Table was created during the construction of the [`DynamoDbContext`] instance.
    New,
    /// Table already existed when the [`DynamoDbContext`] instance was created.
    Existing,
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
        ensure!(string.len() >= 3, InvalidTableName::TooShort);
        ensure!(string.len() <= 255, InvalidTableName::TooLong);
        ensure!(
            string
                .chars()
                .all(|character| character.is_ascii_alphanumeric()
                    || character == '.'
                    || character == '-'
                    || character == '_'),
            InvalidTableName::InvalidCharacter
        );

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
    #[error("Table name must have at least 3 characters")]
    TooShort,

    #[error("Table name must be at most 63 characters")]
    TooLong,

    #[error("Table name must only contain lowercase letters, numbers, periods and hyphens")]
    InvalidCharacter,
}

/// Errors that occur when using [`DynamoDbContext`].
#[derive(Debug, Error)]
pub enum DynamoDbContextError {
    #[error(transparent)]
    Put(#[from] Box<SdkError<aws_sdk_dynamodb::error::PutItemError>>),

    #[error(transparent)]
    Get(#[from] Box<SdkError<aws_sdk_dynamodb::error::GetItemError>>),

    #[error(transparent)]
    Delete(#[from] Box<SdkError<aws_sdk_dynamodb::error::DeleteItemError>>),

    #[error(transparent)]
    Query(#[from] Box<SdkError<aws_sdk_dynamodb::error::QueryError>>),

    #[error("The stored key attribute is missing")]
    MissingKey,

    #[error("Key was stored as {0}, but it was expected to be stored as a binary blob")]
    WrongKeyType(String),

    #[error("The stored value attribute is missing")]
    MissingValue,

    #[error("Value was stored as {0}, but it was expected to be stored as a binary blob")]
    WrongValueType(String),

    #[error("Failed to deserialize key")]
    KeyDeserialization(#[source] bcs::Error),

    #[error("Failed to deserialize value")]
    ValueDeserialization(#[source] bcs::Error),

    #[error("IO error")]
    Io(#[from] io::Error),

    #[error(transparent)]
    View(#[from] ViewError),

    // TODO: Remove the following variants
    #[error("Unknown BCS serialization/deserialization error")]
    UnknownBcsError(#[from] bcs::Error),

    #[error(transparent)]
    CreateTable(#[from] Box<CreateTableError>),

    #[error("Item not found in DynamoDB table: {0}")]
    NotFound(String),
}

impl<InnerError> From<SdkError<InnerError>> for DynamoDbContextError
where
    DynamoDbContextError: From<Box<SdkError<InnerError>>>,
{
    fn from(error: SdkError<InnerError>) -> Self {
        Box::new(error).into()
    }
}

impl From<CreateTableError> for DynamoDbContextError {
    fn from(error: CreateTableError) -> Self {
        Box::new(error).into()
    }
}

impl DynamoDbContextError {
    /// Create a [`DynamoDbContextError::WrongKeyType`] instance based on the returned value type.
    ///
    /// # Panics
    ///
    /// If the value type is in the correct type, a binary blob.
    pub fn wrong_key_type(value: &AttributeValue) -> Self {
        DynamoDbContextError::WrongKeyType(Self::type_description_of(value))
    }

    /// Create a [`DynamoDbContextError::WrongValueType`] instance based on the returned value type.
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

impl From<DynamoDbContextError> for linera_base::error::Error {
    fn from(error: DynamoDbContextError) -> Self {
        Self::StorageError {
            backend: "DynamoDB".to_string(),
            error: error.to_string(),
        }
    }
}

/// Error when creating a table for a new [`DynamoDbContext`] instance.
#[derive(Debug, Error)]
pub enum CreateTableError {
    #[error(transparent)]
    CreateTable(#[from] SdkError<aws_sdk_dynamodb::error::CreateTableError>),
}

/// Error when creating a [`DynamoDbContext`] instance using a LocalStack instance.
#[derive(Debug, Error)]
pub enum LocalStackError {
    #[error(transparent)]
    Endpoint(#[from] localstack::EndpointError),

    #[error(transparent)]
    CreateTable(#[from] Box<CreateTableError>),
}

impl From<CreateTableError> for LocalStackError {
    fn from(error: CreateTableError) -> Self {
        Box::new(error).into()
    }
}

/// A helper trait to add a `SdkError<CreateTableError>::is_resource_in_use_exception()` method.
trait IsResourceInUseException {
    /// Check if the error is a resource is in use exception.
    fn is_resource_in_use_exception(&self) -> bool;
}

impl IsResourceInUseException for SdkError<aws_sdk_dynamodb::error::CreateTableError> {
    fn is_resource_in_use_exception(&self) -> bool {
        matches!(
            self,
            SdkError::ServiceError {
                err: aws_sdk_dynamodb::error::CreateTableError {
                    kind: aws_sdk_dynamodb::error::CreateTableErrorKind::ResourceInUseException(_),
                    ..
                },
                ..
            }
        )
    }
}
