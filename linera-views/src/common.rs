// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{hash::HashingContext, views::ViewError};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, fmt::Debug};

pub enum WriteOperation {
    Delete { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

/// A batch of writes inside a transaction;
#[derive(Default)]
pub struct Batch {
    pub(crate) operations: Vec<WriteOperation>,
}

impl Batch {
    /// building a batch from a function
    pub async fn build<F>(builder: F) -> Result<Self, ViewError>
    where
        F: FnOnce(&mut Batch) -> futures::future::BoxFuture<Result<(), ViewError>> + Send + Sync,
    {
        let mut batch = Batch::default();
        builder(&mut batch).await?;
        Ok(batch)
    }

    /// A key may appear multiple times in the batch
    /// The construction of BatchWriteItem and TransactWriteItem for DynamoDb does
    /// not allow this to happen.
    pub fn simplify(self) -> Self {
        let mut map = HashMap::new();
        for op in self.operations {
            match op {
                WriteOperation::Delete { key } => map.insert(key, None),
                WriteOperation::Put { key, value } => map.insert(key, Some(value)),
            };
        }
        let mut operations = Vec::with_capacity(map.len());
        for (key, val) in map {
            match val {
                Some(value) => operations.push(WriteOperation::Put { key, value }),
                None => operations.push(WriteOperation::Delete { key }),
            }
        }
        Self { operations }
    }

    /// Insert a put a key/value in the batch
    pub fn put_key_value(
        &mut self,
        key: Vec<u8>,
        value: &impl Serialize,
    ) -> Result<(), bcs::Error> {
        let bytes = bcs::to_bytes(value)?;
        self.operations
            .push(WriteOperation::Put { key, value: bytes });
        Ok(())
    }

    /// Insert a put a key/value in the batch
    pub fn put_key_value_u8(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(WriteOperation::Put { key, value });
    }

    /// Delete a key and put in the batch
    pub fn delete_key(&mut self, key: Vec<u8>) {
        self.operations.push(WriteOperation::Delete { key });
    }
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueOperations {
    type Error;

    async fn read_key<V: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<V>, Self::Error>;

    async fn find_keys_with_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error>;

    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, Self::Error>;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error>;
}

/// The context in which a view is operated. Typically, this includes the client to
/// connect to the database and the address of the current entry.
#[async_trait]
pub trait Context {
    /// User provided data to be carried along.
    type Extra: Clone + Send + Sync;

    /// The error type in use by internal operations.
    /// In practice, we always want `ViewError: From<Self::Error>` here.
    type Error: std::error::Error + Debug + Send + Sync + 'static + From<bcs::Error>;

    /// Getter for the user provided data.
    fn extra(&self) -> &Self::Extra;

    /// Getter for the address of the current entry (aka the base_key)
    fn base_key(&self) -> Vec<u8>;

    /// Obtain the Vec<u8> key from the key by serialization and using the base_key
    fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>, Self::Error>;

    /// Obtain the Vec<u8> key from the key by appending to the base_key
    fn derive_key_bytes(&self, index: &[u8]) -> Vec<u8>;

    /// Retrieve a generic `Item` from the table using the provided `key` prefixed by the current
    /// context.
    /// The `Item` is deserialized using [`bcs`].
    async fn read_key<Item: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<Item>, Self::Error>;

    /// Find keys matching the prefix. The full keys are returned, that is including the prefix.
    async fn find_keys_with_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Find the keys matching the prefix. The remainder of the key are parsed back into elements.
    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, Self::Error>;

    /// Apply the operations from the `batch`, persisting the changes.
    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError>;

    fn clone_self(&self, base_key: Vec<u8>) -> Self;
}

#[derive(Debug, Clone)]
pub struct ContextFromDb<E, DB> {
    pub db: DB,
    pub base_key: Vec<u8>,
    pub extra: E,
}

#[async_trait]
impl<E, DB> Context for ContextFromDb<E, DB>
where
    E: Clone + Send + Sync,
    DB: KeyValueOperations + Clone + Send + Sync,
    DB::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
    ViewError: From<DB::Error>,
{
    type Extra = E;
    type Error = DB::Error;

    fn extra(&self) -> &E {
        &self.extra
    }

    fn base_key(&self) -> Vec<u8> {
        self.base_key.clone()
    }

    fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>, Self::Error> {
        let mut key = self.base_key.clone();
        bcs::serialize_into(&mut key, index)?;
        assert!(
            key.len() > self.base_key.len(),
            "Empty indices are not allowed"
        );
        Ok(key)
    }

    fn derive_key_bytes(&self, index: &[u8]) -> Vec<u8> {
        let mut key = self.base_key.clone();
        key.extend_from_slice(index);
        key
    }

    async fn read_key<Item>(&self, key: &[u8]) -> Result<Option<Item>, Self::Error>
    where
        Item: DeserializeOwned,
    {
        self.db.read_key(key).await
    }

    async fn find_keys_with_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.db.find_keys_with_prefix(key_prefix).await
    }

    async fn get_sub_keys<Key>(&mut self, key_prefix: &[u8]) -> Result<Vec<Key>, Self::Error>
    where
        Key: DeserializeOwned + Send,
    {
        self.db.get_sub_keys(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        self.db.write_batch(batch).await?;
        Ok(())
    }

    fn clone_self(&self, base_key: Vec<u8>) -> Self {
        Self {
            db: self.db.clone(),
            base_key,
            extra: self.extra.clone(),
        }
    }
}

impl<E, DB> HashingContext for ContextFromDb<E, DB>
where
    E: Clone + Send + Sync,
    DB: KeyValueOperations + Clone + Send + Sync,
    DB::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
    ViewError: From<DB::Error>,
{
    type Hasher = sha2::Sha512;
}
