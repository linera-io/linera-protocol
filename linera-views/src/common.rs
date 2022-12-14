// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::views::ViewError;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
    },
};

#[derive(Debug)]
pub enum WriteOperation {
    Delete { key: Vec<u8> },
    DeletePrefix { key_prefix: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

/// A batch of writes inside a transaction;
#[derive(Default)]
pub struct Batch {
    pub(crate) operations: Vec<WriteOperation>,
}

/// When wanting to find the entries in a BTreeMap with a specific prefix,
/// one option is to iterate over all keys. Another is to select an interval
/// that represents exactly the keys having that prefix. Which fortunately
/// is possible with the way the comparison operators for vectors is built.
///
/// The statement is that p is a prefix of v if and only if p <= v < upper_bound(p).
pub fn get_upper_bound(key_prefix: &[u8]) -> Option<Vec<u8>> {
    let len = key_prefix.len();
    for i in (0..len).rev() {
        let val = key_prefix[i];
        if val < u8::MAX {
            let mut upper_bound = key_prefix[0..i + 1].to_vec();
            upper_bound[i] += 1;
            return Some(upper_bound);
        }
    }
    None
}

pub fn get_interval(key_prefix: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let upper_bound = match get_upper_bound(&key_prefix) {
        None => Unbounded,
        Some(val) => Excluded(val),
    };
    (Included(key_prefix), upper_bound)
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
        let mut map_delete_insert = BTreeMap::new();
        let mut set_key_prefix = BTreeSet::new();
        for op in self.operations {
            match op {
                WriteOperation::Delete { key } => {
                    map_delete_insert.insert(key, None);
                }
                WriteOperation::Put { key, value } => {
                    map_delete_insert.insert(key, Some(value));
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    let key_list: Vec<Vec<u8>> = map_delete_insert
                        .range(get_interval(key_prefix.clone()))
                        .map(|x| x.0.to_vec())
                        .collect();
                    for key in key_list {
                        map_delete_insert.remove(&key);
                    }
                    let key_prefix_list: Vec<Vec<u8>> = set_key_prefix
                        .range(get_interval(key_prefix.clone()))
                        .map(|x: &Vec<u8>| x.to_vec())
                        .collect();
                    for key_prefix in key_prefix_list {
                        set_key_prefix.remove(&key_prefix);
                    }
                    set_key_prefix.insert(key_prefix);
                }
            }
        }
        let mut operations = Vec::with_capacity(set_key_prefix.len() + map_delete_insert.len());
        // It is important to note that DeletePrefix operations have to be done before other
        // insert operations.
        for key_prefix in set_key_prefix {
            operations.push(WriteOperation::DeletePrefix { key_prefix });
        }
        for (key, val) in map_delete_insert {
            match val {
                Some(value) => operations.push(WriteOperation::Put { key, value }),
                None => operations.push(WriteOperation::Delete { key }),
            }
        }
        Self { operations }
    }

    /// Insert a Put { key, value } into the batch
    #[inline]
    pub fn put_key_value(
        &mut self,
        key: Vec<u8>,
        value: &impl Serialize,
    ) -> Result<(), bcs::Error> {
        let bytes = bcs::to_bytes(value)?;
        self.put_key_value_bytes(key, bytes);
        Ok(())
    }

    /// Insert a Put { key, value } into the batch
    #[inline]
    pub fn put_key_value_bytes(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(WriteOperation::Put { key, value });
    }

    /// Insert a Delete { key } into the batch
    #[inline]
    pub fn delete_key(&mut self, key: Vec<u8>) {
        self.operations.push(WriteOperation::Delete { key });
    }

    /// Insert a DeletePrefix { key_prefix } into the batch
    #[inline]
    pub fn delete_key_prefix(&mut self, key_prefix: Vec<u8>) {
        self.operations
            .push(WriteOperation::DeletePrefix { key_prefix });
    }
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueOperations {
    type Error: Debug;
    type KeyIterator: Iterator<Item = Result<Vec<u8>, Self::Error>>;
    type KeyValueIterator: Iterator<Item = Result<(Vec<u8>, Vec<u8>), Self::Error>>;

    /// Retrieve a Vec<u8> from the database using the provided `key`
    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Find keys matching the prefix. The stripped keys are returned, that is excluding the prefix.
    async fn find_stripped_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyIterator, Self::Error>;

    /// Find (key,value) matching the prefix. The stripped keys are returned, that is excluding the prefix.
    async fn find_stripped_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValueIterator, Self::Error>;

    /// Find keys matching the prefix. The full keys are returned, that is including the prefix.
    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<PrefixAppendIterator<Self::KeyIterator, Self::Error>, Self::Error> {
        let iter = self.find_stripped_keys_by_prefix(key_prefix).await?;
        let key_prefix = key_prefix.to_vec();
        Ok(PrefixAppendIterator::new(key_prefix, iter))
    }

    /// Write the batch in the database.
    async fn write_batch(&self, mut batch: Batch) -> Result<(), Self::Error>;

    /// Read a single key and deserialize the result if present.
    async fn read_key<V: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<V>, Self::Error>
    where
        Self::Error: From<bcs::Error>,
    {
        match self.read_key_bytes(key).await? {
            Some(bytes) => {
                let value = bcs::from_bytes(&bytes)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Get the vector of deserialized keys matching a prefix.
    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, Self::Error>
    where
        Self::Error: From<bcs::Error>,
    {
        let mut keys = Vec::new();
        for key in self.find_stripped_keys_by_prefix(key_prefix).await? {
            let key = key?;
            keys.push(bcs::from_bytes(&key)?);
        }
        Ok(keys)
    }
}

/// An iterator that wraps another one, prefixing the items with a key prefix.
pub struct PrefixAppendIterator<IT, E> {
    key_prefix: Vec<u8>,
    iter: IT,
    _error_type: std::marker::PhantomData<E>,
}

impl<IT, E> PrefixAppendIterator<IT, E> {
    pub(crate) fn new(key_prefix: Vec<u8>, iter: IT) -> Self {
        Self {
            key_prefix,
            iter,
            _error_type: std::marker::PhantomData,
        }
    }
}

impl<IT, E> Iterator for PrefixAppendIterator<IT, E>
where
    IT: Iterator<Item = Result<Vec<u8>, E>>,
{
    type Item = Result<Vec<u8>, E>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next()? {
            Ok(val) => {
                let mut key = self.key_prefix.clone();
                key.extend_from_slice(&val);
                Some(Ok(key))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

// A non-optimized iterator for simple DB implementations.
// Inspired by https://depth-first.com/articles/2020/06/22/returning-rust-iterators/
pub struct SimpleTypeIterator<T, E> {
    iter: std::vec::IntoIter<T>,
    _error_type: std::marker::PhantomData<E>,
}

impl<T, E> SimpleTypeIterator<T, E> {
    pub(crate) fn new(values: Vec<T>) -> Self {
        Self {
            iter: values.into_iter(),
            _error_type: std::marker::PhantomData,
        }
    }
}

impl<T, E> Iterator for SimpleTypeIterator<T, E> {
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Result::Ok)
    }
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

    /// Obtain the short Vec<u8> key from the key by serialization
    fn derive_short_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>, Self::Error>;

    /// Obtain the Vec<u8> key from the key by appending to the base_key
    fn derive_key_bytes(&self, index: &[u8]) -> Vec<u8>;

    /// Deserialize `value_byte`.
    fn deserialize_value<Item: DeserializeOwned>(bytes: &[u8]) -> Result<Item, Self::Error>;

    /// Retrieve a generic `Item` from the database using the provided `key` prefixed by the current
    /// context.
    /// The `Item` is deserialized using [`bcs`].
    async fn read_key<Item: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<Item>, Self::Error>;

    /// Retrieve a Vec<u8> from the database using the provided `key` prefixed by the current
    /// context.
    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Find keys matching the prefix. The stripped keys are returned, that is excluding the prefix.
    async fn find_stripped_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Find (key,value) matching the prefix. The stripped keys are returned, that is excluding the prefix.
    async fn find_stripped_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error>;

    /// Find the keys matching the prefix. The remainder of the key are parsed back into elements.
    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, Self::Error>;

    /// Apply the operations from the `batch`, persisting the changes.
    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error>;

    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self;
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

    fn derive_short_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>, Self::Error> {
        let mut key = Vec::new();
        bcs::serialize_into(&mut key, index)?;
        Ok(key)
    }

    fn derive_key_bytes(&self, index: &[u8]) -> Vec<u8> {
        let mut key = self.base_key.clone();
        key.extend_from_slice(index);
        key
    }

    fn deserialize_value<Item: DeserializeOwned>(bytes: &[u8]) -> Result<Item, Self::Error> {
        let value = bcs::from_bytes(bytes)?;
        Ok(value)
    }

    async fn read_key<Item>(&self, key: &[u8]) -> Result<Option<Item>, Self::Error>
    where
        Item: DeserializeOwned,
    {
        self.db.read_key(key).await
    }

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.db.read_key_bytes(key).await
    }

    async fn find_stripped_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.db
            .find_stripped_keys_by_prefix(key_prefix)
            .await?
            .collect()
    }

    async fn find_stripped_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        self.db
            .find_stripped_key_values_by_prefix(key_prefix)
            .await?
            .collect()
    }

    async fn get_sub_keys<Key>(&mut self, key_prefix: &[u8]) -> Result<Vec<Key>, Self::Error>
    where
        Key: DeserializeOwned + Send,
    {
        self.db.get_sub_keys(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        self.db.write_batch(batch).await?;
        Ok(())
    }

    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self {
        Self {
            db: self.db.clone(),
            base_key,
            extra: self.extra.clone(),
        }
    }
}
