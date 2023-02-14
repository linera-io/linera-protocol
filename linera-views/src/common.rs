// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{batch::Batch, views::ViewError};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::Debug,
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
    },
};

#[cfg(test)]
#[path = "unit_tests/common_tests.rs"]
mod common_tests;

/// This is the standard hash used by sha2
pub type HashOutput = generic_array::GenericArray<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>;

#[derive(Debug)]
pub(crate) enum Update<T> {
    Removed,
    Set(T),
}

/// The minimum value for the view tags. values in 0..MIN_VIEW_TAG are used for other purposes
pub const MIN_VIEW_TAG: u8 = 1;

/// When wanting to find the entries in a BTreeMap with a specific prefix,
/// one option is to iterate over all keys. Another is to select an interval
/// that represents exactly the keys having that prefix. Which fortunately
/// is possible with the way the comparison operators for vectors is built.
///
/// The statement is that p is a prefix of v if and only if p <= v < upper_bound(p).
pub(crate) fn get_upper_bound(key_prefix: &[u8]) -> Bound<Vec<u8>> {
    let len = key_prefix.len();
    for i in (0..len).rev() {
        let val = key_prefix[i];
        if val < u8::MAX {
            let mut upper_bound = key_prefix[0..i + 1].to_vec();
            upper_bound[i] += 1;
            return Excluded(upper_bound);
        }
    }
    Unbounded
}

pub(crate) fn get_interval(key_prefix: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let upper_bound = get_upper_bound(&key_prefix);
    (Included(key_prefix), upper_bound)
}

/// How to iterate over the keys returned by a search query.
pub trait KeyIterable<Error> {
    /// The iterator returning keys by reference.
    type Iterator<'a>: Iterator<Item = Result<&'a [u8], Error>>
    where
        Self: 'a;

    /// Iterate keys by reference.
    fn iterator(&self) -> Self::Iterator<'_>;
}

/// How to iterate over the key-value pairs returned by a search query.
pub trait KeyValueIterable<Error> {
    /// The iterator returning key-value pairs by reference.
    type Iterator<'a>: Iterator<Item = Result<(&'a [u8], &'a [u8]), Error>>
    where
        Self: 'a;

    /// The iterator returning key-value pairs by value.
    type IteratorOwned: Iterator<Item = Result<(Vec<u8>, Vec<u8>), Error>>;

    /// Iterate keys and values by reference.
    fn iterator(&self) -> Self::Iterator<'_>;

    /// Iterate keys and values by value.
    fn into_iterator_owned(self) -> Self::IteratorOwned;
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueStoreClient {
    /// The error type.
    type Error: Debug;

    /// Return type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Return type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Retrieve a `Vec<u8>` from the database using the provided `key`
    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Find the keys matching the prefix. The prefix is not included in the returned keys.
    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error>;

    /// Find the key-value pairs matching the prefix. The prefix is not included in the returned keys.
    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error>;

    /// Write the batch in the database.
    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Self::Error>;

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

    /// Clearing any journal entry that may remain.
    /// The journal located at the base_key will be cleared if existing.
    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error>;
}

#[doc(hidden)]
/// Iterates keys by reference in a vector of keys.
/// Inspired by https://depth-first.com/articles/2020/06/22/returning-rust-iterators/
pub struct SimpleKeyIterator<'a, E> {
    iter: std::slice::Iter<'a, Vec<u8>>,
    _error_type: std::marker::PhantomData<E>,
}

impl<'a, E> Iterator for SimpleKeyIterator<'a, E> {
    type Item = Result<&'a [u8], E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|key| Result::Ok(key.as_ref()))
    }
}

impl<E> KeyIterable<E> for Vec<Vec<u8>> {
    type Iterator<'a> = SimpleKeyIterator<'a, E>;

    fn iterator(&self) -> Self::Iterator<'_> {
        SimpleKeyIterator {
            iter: self.iter(),
            _error_type: std::marker::PhantomData,
        }
    }
}

#[doc(hidden)]
/// Same as `SimpleKeyIterator` but for key-value pairs.
pub struct SimpleKeyValueIterator<'a, E> {
    iter: std::slice::Iter<'a, (Vec<u8>, Vec<u8>)>,
    _error_type: std::marker::PhantomData<E>,
}

impl<'a, E> Iterator for SimpleKeyValueIterator<'a, E> {
    type Item = Result<(&'a [u8], &'a [u8]), E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|entry| Ok((&entry.0[..], &entry.1[..])))
    }
}

#[doc(hidden)]
/// Same as `SimpleKeyValueIterator` but key-value pairs are passed by value.
pub struct SimpleKeyValueIteratorOwned<E> {
    iter: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    _error_type: std::marker::PhantomData<E>,
}

impl<E> Iterator for SimpleKeyValueIteratorOwned<E> {
    type Item = Result<(Vec<u8>, Vec<u8>), E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Result::Ok)
    }
}

impl<E> KeyValueIterable<E> for Vec<(Vec<u8>, Vec<u8>)> {
    type Iterator<'a> = SimpleKeyValueIterator<'a, E>;
    type IteratorOwned = SimpleKeyValueIteratorOwned<E>;

    fn iterator(&self) -> Self::Iterator<'_> {
        SimpleKeyValueIterator {
            iter: self.iter(),
            _error_type: std::marker::PhantomData,
        }
    }

    fn into_iterator_owned(self) -> Self::IteratorOwned {
        SimpleKeyValueIteratorOwned {
            iter: self.into_iter(),
            _error_type: std::marker::PhantomData,
        }
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

    /// Return type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Return type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Getter for the user provided data.
    fn extra(&self) -> &Self::Extra;

    /// Getter for the address of the current entry (aka the base_key)
    fn base_key(&self) -> Vec<u8>;

    /// Concatenate the base_key and tag
    fn base_tag(&self, tag: u8) -> Vec<u8>;

    /// Concatenate the base_key, tag and index
    fn base_tag_index(&self, tag: u8, index: &[u8]) -> Vec<u8>;

    /// Obtain the `Vec<u8>` key from the key by serialization and using the base_key
    fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>, Self::Error>;

    /// Obtain the `Vec<u8>` key from the key by serialization and using the base_key
    fn derive_tag_key<I: Serialize>(&self, tag: u8, index: &I) -> Result<Vec<u8>, Self::Error>;

    /// Obtain the short `Vec<u8>` key from the key by serialization
    fn derive_short_key<I: Serialize + ?Sized>(index: &I) -> Result<Vec<u8>, Self::Error>;

    /// Obtain the `Vec<u8>` key from the key by appending to the base_key
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

    /// Retrieve a `Vec<u8>` from the database using the provided `key` prefixed by the current
    /// context.
    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Find keys matching the prefix. The prefix is not included in the returned keys.
    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error>;

    /// Find the key-value pairs matching the prefix. The prefix is not included in the returned keys.
    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error>;

    /// Apply the operations from the `batch`, persisting the changes.
    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error>;

    /// Obtain a similar [`Context`] implementation with a different base key.
    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self;
}

/// Implementation of the [`Context`] trait on top of a DB client implementing
/// [`KeyValueStoreClient`].
#[derive(Debug, Clone)]
pub struct ContextFromDb<E, DB> {
    /// The DB client, usually shared between views.
    pub db: DB,
    /// The key prefix for the current view.
    pub base_key: Vec<u8>,
    /// User-defined data attached to the view.
    pub extra: E,
}

#[async_trait]
impl<E, DB> Context for ContextFromDb<E, DB>
where
    E: Clone + Send + Sync,
    DB: KeyValueStoreClient + Clone + Send + Sync,
    DB::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
    ViewError: From<DB::Error>,
{
    type Extra = E;
    type Error = DB::Error;
    type Keys = DB::Keys;
    type KeyValues = DB::KeyValues;

    fn extra(&self) -> &E {
        &self.extra
    }

    fn base_key(&self) -> Vec<u8> {
        self.base_key.clone()
    }

    fn base_tag(&self, tag: u8) -> Vec<u8> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_key.clone();
        key.extend_from_slice(&[tag]);
        key
    }

    fn base_tag_index(&self, tag: u8, index: &[u8]) -> Vec<u8> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_key.clone();
        key.extend_from_slice(&[tag]);
        key.extend_from_slice(index);
        key
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

    fn derive_tag_key<I: Serialize>(&self, tag: u8, index: &I) -> Result<Vec<u8>, Self::Error> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_key.clone();
        key.extend_from_slice(&[tag]);
        bcs::serialize_into(&mut key, index)?;
        Ok(key)
    }

    fn derive_short_key<I: Serialize + ?Sized>(index: &I) -> Result<Vec<u8>, Self::Error> {
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

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.db.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.db.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        self.db.write_batch(batch, &self.base_key).await?;
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
