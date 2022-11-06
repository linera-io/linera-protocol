// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    hash::HashingContext,
    localstack,
    views::{Context, ViewError},
    common::{WriteOperation, Batch, simplify_batch},
};
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    model::{
        AttributeDefinition, AttributeValue, DeleteRequest, KeySchemaElement, KeyType,
        ProvisionedThroughput, PutRequest, ScalarAttributeType, WriteRequest,
    },
    types::{Blob, SdkError},
    Client,
};
use linera_base::ensure;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, str::FromStr};
use thiserror::Error;

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

#[derive(Debug, Clone)]
pub struct DynamoPair { pub client: Client, pub table: TableName }

/// A implementation of [`Context`] based on DynamoDB.
#[derive(Debug, Clone)]
pub struct DynamoDbContext<E>
where
    E: Clone + Sync + Send,
{
    db: DynamoPair,
    base_key: Vec<u8>,
    extra: E,
}

impl<E> DynamoDbContext<E>
where
    E: Clone + Sync + Send,
{
    /// Create a new [`DynamoDbContext`] instance.
    pub async fn new(
        table: TableName,
        base_key: Vec<u8>,
        extra: E,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        let config = aws_config::load_from_env().await;

        DynamoDbContext::from_config(&config, table, base_key, extra).await
    }

    /// Create a new [`DynamoDbContext`] instance using the provided `config` parameters.
    pub async fn from_config(
        config: impl Into<Config>,
        table: TableName,
        base_key: Vec<u8>,
        extra: E,
    ) -> Result<(Self, TableStatus), CreateTableError> {
        let db = DynamoPair { client: Client::from_conf(config.into()), table };
        let storage = DynamoDbContext {
            db,
            base_key,
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
        base_key: Vec<u8>,
        extra: E,
    ) -> Result<(Self, TableStatus), LocalStackError> {
        let base_config = aws_config::load_from_env().await;
        let config = aws_sdk_dynamodb::config::Builder::from(&base_config)
            .endpoint_resolver(localstack::get_endpoint()?)
            .build();

        Ok(DynamoDbContext::from_config(config, table, base_key, extra).await?)
    }

    /// Clone this [`DynamoDbContext`] while entering a sub-scope.
    ///
    /// The return context has its key prefix extended with `scope_prefix` and uses the
    /// `new_extra` instead of cloning the current extra data.
    pub fn clone_with_sub_scope<NewE: Clone + Send + Sync>(
        &self,
        scope_prefix: &impl Serialize,
        new_extra: NewE,
    ) -> DynamoDbContext<NewE> {
        DynamoDbContext {
            db: self.db.clone(),
            base_key: self.derive_key(scope_prefix).expect("derive_key should not fail"),
            extra: new_extra,
        }
    }

    /// Create the storage table if it doesn't exist.
    ///
    /// Attempts to create the table and ignores errors that indicate that it already exists.
    async fn create_table_if_needed(&self) -> Result<TableStatus, CreateTableError> {
        let result = self
            .db.client
            .create_table()
            .table_name(self.db.table.as_ref())
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
    /// context prefix. The Vec<u8> expression is obtaind from self.derive_key
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

    /// Build the value attribute for storing a table item.
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

    /// Extract the key attribute from an item and deserialize it into the `Key` type.
    fn extract_raw_key(
        &self,
        attributes: &HashMap<String, AttributeValue>,
    ) -> Result<Vec<u8>, DynamoDbContextError> {
        Ok(attributes
            .get(KEY_ATTRIBUTE)
            .ok_or(DynamoDbContextError::MissingKey)?
            .as_b()
            .map_err(DynamoDbContextError::wrong_key_type)?
            .as_ref()
            .to_owned())
    }
    /// Extract the key attribute from an item and deserialize it into the `Key` type.
    fn extract_sub_key<Key>(
        &self,
        attributes: &HashMap<String, AttributeValue>,
        extra_bytes_to_skip: usize,
    ) -> Result<Key, DynamoDbContextError>
    where
        Key: DeserializeOwned,
    {
        Self::extract_attribute(
            attributes,
            KEY_ATTRIBUTE,
            Some(self.base_key.len() + extra_bytes_to_skip),
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

    /// We put submit the transaction in blocks of at most 25 so as to decrease the
    /// number of needed transactions.
    async fn process_batch(&self, batch: Batch) -> Result<(), DynamoDbContextError> {
        for batch_chunk in simplify_batch(batch).operations.chunks(25) {
            let requests = batch_chunk
                .iter()
                .map(|operation| match operation {
                    WriteOperation::Delete { key } => {
                        let request = DeleteRequest::builder()
                            .set_key(Some(Self::build_key(key.to_vec())))
                            .build();
                        WriteRequest::builder().delete_request(request).build()
                    }
                    WriteOperation::Put { key, value } => {
                        let request = PutRequest::builder()
                            .set_item(Some(Self::build_key_value(key.to_vec(), value.to_vec())))
                            .build();
                        WriteRequest::builder().put_request(request).build()
                    }
                })
                .collect();

            self.db.client
                .batch_write_item()
                .set_request_items(Some(HashMap::from([(self.db.table.0.clone(), requests)])))
                .send()
                .await?;
        }
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

    fn base_key(&self) -> Vec<u8> {
        self.base_key.clone()
    }

    fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>,DynamoDbContextError> {
        let mut key = self.base_key.clone();
        bcs::serialize_into(&mut key, index)?;
        assert!(
            key.len() > self.base_key.len(),
            "Empty indices are not allowed"
        );
        Ok(key)
    }

    /// Retrieve a generic `Item` from the table using the provided `key` prefixed by the current
    /// context.
    ///
    /// The `Item` is deserialized using [`bcs`].
    async fn read_key<Item>(&mut self, key: &[u8]) -> Result<Option<Item>, DynamoDbContextError>
    where
        Item: DeserializeOwned,
    {
        let response = self
            .db.client
            .get_item()
            .table_name(self.db.table.as_ref())
            .set_key(Some(Self::build_key(key.to_vec())))
            .send()
            .await?;

        match response.item() {
            Some(item) => Ok(Some(Self::extract_value(item)?)),
            None => Ok(None),
        }
    }

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, DynamoDbContextError> {
        let response = self
            .db.client
            .query()
            .table_name(self.db.table.as_ref())
            .projection_expression(KEY_ATTRIBUTE)
            .key_condition_expression(format!(
                "{PARTITION_ATTRIBUTE} = :partition and begins_with({KEY_ATTRIBUTE}, :prefix)"
            ))
            .expression_attribute_values(
                ":partition",
                AttributeValue::B(Blob::new(DUMMY_PARTITION_KEY)),
            )
            .expression_attribute_values(
                ":prefix",
                AttributeValue::B(Blob::new(key_prefix.to_vec())),
            )
            .send()
            .await?;

        response
            .items()
            .into_iter()
            .flatten()
            .map(|item| self.extract_raw_key(item))
            .collect()
    }

    /// Query the table for the keys that are prefixed by the current context.
    ///
    /// # Panics
    ///
    /// If the raw key bytes can't be deserialized into a `Key`.
    async fn get_sub_keys<Key>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, DynamoDbContextError>
    where
        Key: DeserializeOwned + Send,
    {
        let extra_prefix_bytes_count = key_prefix.len() - self.base_key.len();
        let response = self
            .db.client
            .query()
            .table_name(self.db.table.as_ref())
            .projection_expression(KEY_ATTRIBUTE)
            .key_condition_expression(format!(
                "{PARTITION_ATTRIBUTE} = :partition and begins_with({KEY_ATTRIBUTE}, :prefix)"
            ))
            .expression_attribute_values(
                ":partition",
                AttributeValue::B(Blob::new(DUMMY_PARTITION_KEY)),
            )
            .expression_attribute_values(
                ":prefix",
                AttributeValue::B(Blob::new(key_prefix.to_vec())),
            )
            .send()
            .await?;

        response
            .items()
            .into_iter()
            .flatten()
            .map(|item| self.extract_sub_key(item, extra_prefix_bytes_count))
            .collect()
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        self.process_batch(batch).await?;
        Ok(())
    }

    fn clone_self(&self, base_key: Vec<u8>) -> Self {
        DynamoDbContext {
            db: self.db.clone(),
            base_key,
            extra: self.extra.clone(),
        }
    }
}

impl<E> HashingContext for DynamoDbContext<E>
where
    E: Clone + Send + Sync,
{
    type Hasher = sha2::Sha512;
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
    BatchWriteItem(#[from] Box<SdkError<aws_sdk_dynamodb::error::BatchWriteItemError>>),

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

impl From<DynamoDbContextError> for crate::views::ViewError {
    fn from(error: DynamoDbContextError) -> Self {
        Self::ContextError {
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
