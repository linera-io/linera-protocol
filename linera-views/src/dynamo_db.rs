// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    localstack,
    views::{
        AppendOnlyLogOperations, Context, QueueOperations, RegisterOperations, ScopedOperations,
        ViewError,
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
        config: impl Into<aws_sdk_dynamodb::Config>,
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

    /// Build the key attributes for a table item.
    ///
    /// The key is composed of two attributes that are both binary blobs. The first attribute is a
    /// partition key and is currently just a dummy value that ensures all items are in the same
    /// partion. This is necessary for range queries to work correctly.
    ///
    /// The second attribute is the actual key value, which is generated by concatenating the
    /// context prefix with the bytes obtained from serializing `key` using [`bcs`].
    fn build_key(&self, key: &impl Serialize) -> HashMap<String, AttributeValue> {
        let key_bytes = [
            self.key_prefix.as_slice(),
            &bcs::to_bytes(key).expect("Serialization of key failed"),
        ]
        .concat();

        [
            (
                PARTITION_ATTRIBUTE.to_owned(),
                AttributeValue::B(Blob::new(DUMMY_PARTITION_KEY)),
            ),
            (
                KEY_ATTRIBUTE.to_owned(),
                AttributeValue::B(Blob::new(key_bytes)),
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
}

#[async_trait]
impl<E> Context for DynamoDbContext<E>
where
    E: Clone + Send + Sync,
{
    type Extra = E;
    type Error = DynamoDbContextError;

    fn extra(&self) -> &E {
        &self.extra
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
            key_prefix: [self.key_prefix.as_slice(), &index.to_le_bytes()].concat(),
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

    async fn set(&mut self, value: T) -> Result<(), Self::Error> {
        self.put_item(&(), &value).await?;
        Ok(())
    }

    async fn delete(&mut self) -> Result<(), Self::Error> {
        self.remove_item(&()).await?;
        Ok(())
    }
}

#[async_trait]
impl<E, T> AppendOnlyLogOperations<T> for DynamoDbContext<E>
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

    async fn read(&mut self, range: std::ops::Range<usize>) -> Result<Vec<T>, Self::Error> {
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

    async fn append(&mut self, values: Vec<T>) -> Result<(), Self::Error> {
        let mut count = AppendOnlyLogOperations::<T>::count(self).await?;
        for value in values {
            self.put_item(&count, &value).await?;
            count += 1;
        }
        self.put_item(&(), &count).await?;
        Ok(())
    }

    async fn delete(&mut self) -> Result<(), Self::Error> {
        let count = AppendOnlyLogOperations::<T>::count(self).await?;
        self.remove_item(&()).await?;
        for index in 0..count {
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

    async fn delete_front(&mut self, count: usize) -> Result<(), Self::Error> {
        let mut range: Range<usize> = self.get_item(&()).await?.unwrap_or_default();
        range.start += count;
        self.put_item(&(), &range).await?;
        for index in 0..count {
            self.remove_item(&index).await?;
        }
        Ok(())
    }

    async fn append_back(&mut self, values: Vec<T>) -> Result<(), Self::Error> {
        let mut range: Range<usize> = self.get_item(&()).await?.unwrap_or_default();
        for value in values {
            self.put_item(&range.end, &value).await?;
            range.end += 1;
        }
        self.put_item(&(), &range).await
    }

    async fn delete(&mut self) -> Result<(), Self::Error> {
        let range: Range<usize> = self.get_item(&()).await?.unwrap_or_default();
        self.remove_item(&()).await?;
        for index in range {
            self.remove_item(&index).await?;
        }
        Ok(())
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
#[derive(Clone, Debug)]
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
}

impl<InnerError> From<SdkError<InnerError>> for DynamoDbContextError
where
    DynamoDbContextError: From<Box<SdkError<InnerError>>>,
{
    fn from(error: SdkError<InnerError>) -> Self {
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
