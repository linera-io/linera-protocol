// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides several functionalities for the handling of data.
//! The most important traits are:
//! * [`KeyValueStoreClient`][trait1] which manages the access to a database and is clonable. It has a minimal interface
//! * [`Context`][trait2] which provides the access to a database plus a `base_key` and some extra type `E` which is carried along
//! and has no impact on the running of the system. There is also a bunch of other helper functions.
//!
//! [trait1]: common::KeyValueStoreClient
//! [trait2]: common::Context

use crate::{
    batch::{Batch, WriteOperation},
    views::ViewError,
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::{Debug, Display},
    future::Future,
    mem,
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
    },
    time::{Duration, Instant},
};
use thiserror::Error;

#[cfg(test)]
#[path = "unit_tests/common_tests.rs"]
mod common_tests;

#[doc(hidden)]
pub type HasherOutputSize = <sha3::Sha3_256 as sha3::digest::OutputSizeUser>::OutputSize;
#[doc(hidden)]
pub type HasherOutput = generic_array::GenericArray<u8, HasherOutputSize>;

#[derive(Debug)]
pub(crate) enum Update<T> {
    Removed,
    Set(T),
}

/// The minimum value for the view tags. Values in 0..MIN_VIEW_TAG are used for other purposes.
pub(crate) const MIN_VIEW_TAG: u8 = 1;

/// Data type indicating that the database is not consistent
#[derive(Error, Debug)]
pub enum DatabaseConsistencyError {
    /// The key is of length less than 4 so we cannot extract the first byte
    #[error("key of length less than 4, so we cannot extract the first byte")]
    TooShortKey,

    /// In the splitting scheme a certain number of segments was specified, but some are missing
    #[error("segment is missing from the database")]
    MissingSegment,

    /// We expect that the segments arrive sequentially
    #[error("segment index is not what was expected")]
    IncorrectIndexEntry,

    /// In the value, we should have a count entry.
    #[error("no count of size u32 is available in the value")]
    NoCountAvailable,
}

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

/// Compute an interval so that a vector has key_prefix as a prefix
/// if and only if it belongs to the range.
pub fn get_interval(key_prefix: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let upper_bound = get_upper_bound(&key_prefix);
    (Included(key_prefix), upper_bound)
}

pub(crate) fn from_bytes_opt<V: DeserializeOwned, E>(
    key_opt: Option<Vec<u8>>,
) -> Result<Option<V>, E>
where
    E: From<bcs::Error>,
{
    match key_opt {
        Some(bytes) => {
            let value = bcs::from_bytes(&bytes)?;
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

/// How to iterate over the keys returned by a search query.
pub trait KeyIterable<Error> {
    /// The iterator returning keys by reference.
    type Iterator<'a>: Iterator<Item = Result<&'a [u8], Error>>
    where
        Self: 'a;

    /// Iterates keys by reference.
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

    /// Iterates keys and values by reference.
    fn iterator(&self) -> Self::Iterator<'_>;

    /// Iterates keys and values by value.
    fn into_iterator_owned(self) -> Self::IteratorOwned;
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueStoreClient {
    /// Maximum number of simultaneous connections
    const MAX_CONNECTIONS: usize;

    /// The maximal size of values that can be stored
    const MAX_VALUE_SIZE: usize;

    /// The error type.
    type Error: Debug;

    /// Returns type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Returns type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Retrieves a `Vec<u8>` from the database using the provided `key`.
    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Retrieves multiple `Vec<u8>` from the database using the provided `keys`.
    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

    /// Finds the `key` matching the prefix. The prefix is not included in the returned keys.
    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error>;

    /// Finds the `(key,value)` pairs matching the prefix. The prefix is not included in the returned keys.
    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error>;

    /// Writes the `batch` in the database with `base_key` the base key of the entries for the journal.
    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Self::Error>;

    /// Clears any journal entry that may remain.
    /// The journal is located at the `base_key`.
    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error>;

    /// Reads a single `key` and deserializes the result if present.
    async fn read_key<V: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<V>, Self::Error>
    where
        Self::Error: From<bcs::Error>,
    {
        from_bytes_opt(self.read_key_bytes(key).await?)
    }

    /// Reads multiple `keys` and deserializes the results if present.
    async fn read_multi_key<V: DeserializeOwned + Send>(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<V>>, Self::Error>
    where
        Self::Error: From<bcs::Error>,
    {
        let mut values = Vec::with_capacity(keys.len());
        for entry in self.read_multi_key_bytes(keys).await? {
            values.push(from_bytes_opt(entry)?);
        }
        Ok(values)
    }
}

/// Implement a reader of data that splits the big values into multiple keys.
/// Some databases have a design with relatively small size for values (like 400 kB)
/// which forces us to split the value into several small block.
/// Design is the following:
/// * For every `key` we build several corresponding keys [key * * * *] that contain
///   each a piece of the full data set.
///   The [ * * * *] is actually the serialization of the index which is u32 so that
///   the order of the serialization is the same as the order of the u32 entries.
/// * For the first key, that is  [key 0 0 0 0] the information is [* * * * value0]
///   with [* * * *] being the serialization of the u32 count of the number of blocks
///   and value0 the first block of the value. For the other keys [key * * * *] the
///   corresponding value is valueK with valueK the K-th entry in the value. The full
///   value is reconstructed as value = [value0 value1 .... valueN]
///
/// The effective working of the system relies on a number of design choices.
/// * In the retrieval by the "find_keys_by_prefix" and similar, the keys are
///   returned in lexicographic order.
/// * The count and indices are serialized as u32 and the serialization is done
///   so that the u32 ordering is the same as the lexicographic ordering. So, we
///   get first the first key (and so the count) and then the segment keys one by
///   one in the correct order.
/// * Suppose that we have written a (key,value1) with several segments and then
///   we replace by a (key,value2) which has less segment or uses just a logical key.
///   Our choice is to leave the old segments in the database. Same with delete:
///   we just delete the first key.
/// * We avoid the overflowing of the system by the following trick: When we delete
///   a view, we do a delete by prefix, which thus deletes the old segments if present.
/// * In order to have a segment that deletes all the unused segment, every write has
///   to have a read just before which is expensive.
#[derive(Clone)]
pub struct KeyValueStoreClientBigValue<K> {
    client: K,
}

#[async_trait]
impl<K> KeyValueStoreClient for KeyValueStoreClientBigValue<K>
where
    K: KeyValueStoreClient + Send + Sync,
    K::Error: From<bcs::Error> + From<DatabaseConsistencyError>,
{
    const MAX_CONNECTIONS: usize = K::MAX_CONNECTIONS;
    const MAX_VALUE_SIZE: usize = usize::MAX;
    type Error = K::Error;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let mut big_key = key.to_vec();
        big_key.extend(&[0, 0, 0, 0]);
        let value = self.client.read_key_bytes(&big_key).await?;
        let Some(value) = value else {
            return Ok(None);
        };
        let count = Self::read_count_from_value(&value)?;
        let mut big_value = value[4..].to_vec();
        if count == 1 {
            return Ok(Some(big_value));
        }
        let mut big_keys = Vec::new();
        for i in 1..count {
            let big_key_segment = Self::get_segment_key(key, i)?;
            big_keys.push(big_key_segment);
        }
        let segments = self.client.read_multi_key_bytes(big_keys).await?;
        for segment in segments {
            match segment {
                None => {
                    return Err(DatabaseConsistencyError::MissingSegment.into());
                }
                Some(segment) => {
                    big_value.extend(segment);
                }
            }
        }
        Ok(Some(big_value))
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut big_keys = Vec::new();
        for key in &keys {
            let mut big_key = key.clone();
            big_key.extend(&[0, 0, 0, 0]);
            big_keys.push(big_key);
        }
        let values = self.client.read_multi_key_bytes(big_keys).await?;
        let mut big_values = Vec::<Option<Vec<u8>>>::new();
        let mut keys_add = Vec::new();
        let mut n_blocks = Vec::new();
        for (key, value) in keys.iter().zip(values) {
            match value {
                None => {
                    n_blocks.push(0);
                    big_values.push(None);
                }
                Some(value) => {
                    let count = Self::read_count_from_value(&value)?;
                    let big_value = value[4..].to_vec();
                    for i in 1..count {
                        let big_key_segment = Self::get_segment_key(key, i)?;
                        keys_add.push(big_key_segment);
                    }
                    n_blocks.push(count);
                    big_values.push(Some(big_value));
                }
            }
        }
        if !keys_add.is_empty() {
            let segments: Vec<Option<Vec<u8>>> = self.client.read_multi_key_bytes(keys_add).await?;
            let mut pos = 0;
            for (idx, count) in n_blocks.iter().enumerate() {
                if count > &1 {
                    let value: &mut Option<Vec<u8>> = big_values.get_mut(idx).unwrap();
                    if let Some(ref mut value) = value {
                        for _ in 1..*count {
                            let segment: Vec<u8> = segments.get(pos).unwrap().clone().unwrap();
                            value.extend(segment);
                            pos += 1;
                        }
                    }
                }
            }
        }
        Ok(big_values)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        let mut keys = Vec::new();
        for big_key in self
            .client
            .find_keys_by_prefix(key_prefix)
            .await?
            .iterator()
        {
            let big_key = big_key?;
            let len = big_key.len();
            if Self::read_index_from_key(big_key)? == 0 {
                let key = big_key[0..len - 4].to_vec();
                keys.push(key);
            }
        }
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let mut key_values = Vec::new();
        let mut key = Vec::new();
        let mut big_value = Vec::new();
        let mut count = 0;
        let mut pos = 0;
        for result in self
            .client
            .find_key_values_by_prefix(key_prefix)
            .await?
            .iterator()
        {
            let (big_key, value) = result?;
            let len = big_key.len();
            let idx = Self::read_index_from_key(big_key)?;
            if idx == 0 {
                count = Self::read_count_from_value(value)?;
                key = big_key[..len - 4].to_vec();
                big_value.extend(value[4..].to_vec());
                pos = 1;
            } else if big_key[..len - 4] == key {
                if idx == pos && idx < count {
                    big_value.extend(value);
                    pos += 1;
                }
            } else {
                pos = 0;
            }
            if pos == count {
                key_values.push((mem::take(&mut key), mem::take(&mut big_value)));
            }
        }
        Ok(key_values)
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Self::Error> {
        let mut batch_new = Batch::new();
        for operation in batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    let mut big_key = key.to_vec();
                    big_key.extend(&[0, 0, 0, 0]);
                    batch_new.delete_key(big_key);
                }
                WriteOperation::Put { key, value } => {
                    let mut big_key = key.clone();
                    big_key.extend(&[0, 0, 0, 0]);
                    let mut first_chunk = Vec::new();
                    let mut pos: u32 = 0;
                    for value_chunk in value.chunks(K::MAX_VALUE_SIZE) {
                        let big_key_segment = Self::get_segment_key(&key, pos)?;
                        if pos == 0 {
                            first_chunk = value_chunk.to_vec();
                        } else {
                            batch_new.put_key_value_bytes(big_key_segment, value_chunk.to_vec());
                        }
                        pos += 1;
                    }
                    let value_ext = Self::get_initial_count_first_chunk(pos, &first_chunk)?;
                    batch_new.put_key_value_bytes(big_key, value_ext);
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    batch_new.delete_key_prefix(key_prefix);
                }
            }
        }
        self.client.write_batch(batch_new, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.client.clear_journal(base_key).await
    }
}

impl<K> KeyValueStoreClientBigValue<K>
where
    K: KeyValueStoreClient + Send + Sync,
    K::Error: From<bcs::Error> + From<DatabaseConsistencyError>,
{
    /// Create a new client that deals with big values from one that does not.
    pub fn new(client: K) -> Self {
        KeyValueStoreClientBigValue { client }
    }

    fn read_count_from_value(value: &[u8]) -> Result<u32, K::Error> {
        if value.len() < 4 {
            return Err(DatabaseConsistencyError::NoCountAvailable.into());
        }
        let mut bytes = value[0..4].to_vec();
        bytes.reverse();
        Ok(bcs::from_bytes::<u32>(&bytes)?)
    }

    fn get_segment_key(key: &[u8], index: u32) -> Result<Vec<u8>, K::Error> {
        let mut big_key_segment = key.to_vec();
        let mut bytes = bcs::to_bytes(&index)?;
        bytes.reverse();
        big_key_segment.extend(bytes);
        Ok(big_key_segment)
    }

    fn read_index_from_key(key: &[u8]) -> Result<u32, K::Error> {
        let len = key.len();
        if len < 4 {
            return Err(DatabaseConsistencyError::TooShortKey.into());
        }
        let mut bytes = key[len - 4..len].to_vec();
        bytes.reverse();
        Ok(bcs::from_bytes::<u32>(&bytes)?)
    }

    fn get_initial_count_first_chunk(count: u32, first_chunk: &[u8]) -> Result<Vec<u8>, K::Error> {
        let mut bytes = bcs::to_bytes(&count)?;
        bytes.reverse();
        let mut value_ext = Vec::new();
        value_ext.extend(bytes);
        value_ext.extend(first_chunk);
        Ok(value_ext)
    }
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
    /// Maximum number of simultaneous connections
    const MAX_CONNECTIONS: usize;

    /// User provided data to be carried along.
    type Extra: Clone + Send + Sync;

    /// The error type in use by internal operations.
    /// In practice, we always want `ViewError: From<Self::Error>` here.
    type Error: std::error::Error + Debug + Send + Sync + 'static + From<bcs::Error>;

    /// Returns type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Returns type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Retrieves a `Vec<u8>` from the database using the provided `key` prefixed by the current
    /// context.
    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Retrieves multiple `Vec<u8>` from the database using the provided `keys`.
    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

    /// Finds the keys matching the `key_prefix`. The `key_prefix` is not included in the returned keys.
    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error>;

    /// Finds the `(key,value)` pairs matching the `key_prefix`. The `key_prefix` is not included in the returned keys.
    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error>;

    /// Applies the operations from the `batch`, persisting the changes.
    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error>;

    /// Getter for the user provided data.
    fn extra(&self) -> &Self::Extra;

    /// Obtains a similar [`Context`] implementation with a different base key.
    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self;

    /// Getter for the address of the current entry (aka the base_key).
    fn base_key(&self) -> Vec<u8>;

    /// Concatenates the base_key and tag.
    fn base_tag(&self, tag: u8) -> Vec<u8> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_key();
        key.extend([tag]);
        key
    }

    /// Concatenates the base_key, tag and index.
    fn base_tag_index(&self, tag: u8, index: &[u8]) -> Vec<u8> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_key();
        key.extend([tag]);
        key.extend_from_slice(index);
        key
    }

    /// Obtains the `Vec<u8>` key from the key by serialization and using the base_key.
    fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>, Self::Error> {
        let mut key = self.base_key();
        bcs::serialize_into(&mut key, index)?;
        assert!(
            key.len() > self.base_key().len(),
            "Empty indices are not allowed"
        );
        Ok(key)
    }

    /// Obtains the `Vec<u8>` key from the key by serialization and using the `base_key`.
    fn derive_tag_key<I: Serialize>(&self, tag: u8, index: &I) -> Result<Vec<u8>, Self::Error> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_key();
        key.extend([tag]);
        bcs::serialize_into(&mut key, index)?;
        Ok(key)
    }

    /// Obtains the short `Vec<u8>` key from the key by serialization.
    fn derive_short_key<I: Serialize + ?Sized>(index: &I) -> Result<Vec<u8>, Self::Error> {
        Ok(bcs::to_bytes(index)?)
    }

    /// Deserialize `bytes` into type `Item`.
    fn deserialize_value<Item: DeserializeOwned>(bytes: &[u8]) -> Result<Item, Self::Error> {
        let value = bcs::from_bytes(bytes)?;
        Ok(value)
    }

    /// Retrieves a generic `Item` from the database using the provided `key` prefixed by the current
    /// context.
    /// The `Item` is deserialized using [`bcs`].
    async fn read_key<Item>(&self, key: &[u8]) -> Result<Option<Item>, Self::Error>
    where
        Item: DeserializeOwned,
    {
        from_bytes_opt(self.read_key_bytes(key).await?)
    }

    /// Reads multiple `keys` and deserializes the results if present.
    async fn read_multi_key<V: DeserializeOwned + Send>(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<V>>, Self::Error>
    where
        Self::Error: From<bcs::Error>,
    {
        let mut values = Vec::with_capacity(keys.len());
        for entry in self.read_multi_key_bytes(keys).await? {
            values.push(from_bytes_opt(entry)?);
        }
        Ok(values)
    }
}

/// Implementation of the [`Context`] trait on top of a DB client implementing
/// [`KeyValueStoreClient`].
#[derive(Debug, Default, Clone)]
pub struct ContextFromDb<E, DB> {
    /// The DB client, usually shared between views.
    pub db: DB,
    /// The base key for the current view.
    pub base_key: Vec<u8>,
    /// User-defined data attached to the view.
    pub extra: E,
}

impl<E, DB> ContextFromDb<E, DB>
where
    E: Clone + Send + Sync,
    DB: KeyValueStoreClient + Clone + Send + Sync,
    DB::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
    ViewError: From<DB::Error>,
{
    /// Creates a context from db that also clears the journal before making it available.
    pub async fn create(
        db: DB,
        base_key: Vec<u8>,
        extra: E,
    ) -> Result<Self, <ContextFromDb<E, DB> as Context>::Error> {
        db.clear_journal(&base_key).await?;
        Ok(ContextFromDb {
            db,
            base_key,
            extra,
        })
    }
}

async fn time_async<F, O>(f: F) -> (O, Duration)
where
    F: Future<Output = O>,
{
    let start = Instant::now();
    let out = f.await;
    let duration = start.elapsed();
    (out, duration)
}

async fn log_time_async<F, D, O>(f: F, name: D) -> O
where
    F: Future<Output = O>,
    D: Display,
{
    if cfg!(feature = "db_timings") {
        let (out, duration) = time_async(f).await;
        let duration = duration.as_nanos();
        println!("|{name}|={duration:?}");
        out
    } else {
        f.await
    }
}

#[async_trait]
impl<E, DB> Context for ContextFromDb<E, DB>
where
    E: Clone + Send + Sync,
    DB: KeyValueStoreClient + Clone + Send + Sync,
    DB::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
    ViewError: From<DB::Error>,
{
    const MAX_CONNECTIONS: usize = DB::MAX_CONNECTIONS;
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

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        log_time_async(self.db.read_key_bytes(key), "read_key_bytes").await
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        log_time_async(self.db.read_multi_key_bytes(keys), "read_multi_key_bytes").await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        log_time_async(
            self.db.find_keys_by_prefix(key_prefix),
            "find_keys_by_prefix",
        )
        .await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        log_time_async(
            self.db.find_key_values_by_prefix(key_prefix),
            "find_key_values_by_prefix",
        )
        .await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        log_time_async(self.db.write_batch(batch, &self.base_key), "write_batch").await
    }

    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self {
        Self {
            db: self.db.clone(),
            base_key,
            extra: self.extra.clone(),
        }
    }
}

/// Sometimes we need a serialization that is different from the usual one and
/// for example preserves order.
/// The {to/from}_custom_bytes has to be coherent with the Borrow trait.
pub trait CustomSerialize: Sized {
    /// Serializes the value
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError>;

    /// Deserialize the vector
    fn from_custom_bytes(short_key: &[u8]) -> Result<Self, ViewError>;
}

impl CustomSerialize for u128 {
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError> {
        let mut bytes = bcs::to_bytes(&self)?;
        bytes.reverse();
        Ok(bytes)
    }

    fn from_custom_bytes(bytes: &[u8]) -> Result<Self, ViewError> {
        let mut bytes = bytes.to_vec();
        bytes.reverse();
        let value = bcs::from_bytes(&bytes)?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use linera_views::common::CustomSerialize;
    use rand::{Rng, SeedableRng};
    use std::collections::BTreeSet;

    #[test]
    fn test_ordering_serialization() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(2);
        let n = 1000;
        let mut set = BTreeSet::new();
        for _ in 0..n {
            let val = rng.gen::<u128>();
            set.insert(val);
        }
        let mut vec = Vec::new();
        for val in set {
            vec.push(val);
        }
        for i in 1..vec.len() {
            let val1 = vec[i - 1];
            let val2 = vec[i];
            assert!(val1 < val2);
            let vec1 = val1.to_custom_bytes().unwrap();
            let vec2 = val2.to_custom_bytes().unwrap();
            assert!(vec1 < vec2);
            let val_ret1 = u128::from_custom_bytes(&vec1).unwrap();
            let val_ret2 = u128::from_custom_bytes(&vec2).unwrap();
            assert_eq!(val1, val_ret1);
            assert_eq!(val2, val_ret2);
        }
    }
}
