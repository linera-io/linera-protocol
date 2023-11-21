// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides several functionalities for the handling of data.
//! The most important traits are:
//! * [`KeyValueStoreClient`][trait1] which manages the access to a database and is clonable. It has a minimal interface
//! * [`Context`][trait2] which provides access to a database plus a `base_key` and some extra type `E` which is carried along
//! and has no impact on the running of the system. There is also a bunch of other helper functions.
//!
//! [trait1]: common::KeyValueStoreClient
//! [trait2]: common::Context

use crate::{batch::Batch, views::ViewError};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::{Debug, Display},
    future::Future,
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
    },
    time::{Duration, Instant},
};

#[cfg(test)]
use std::collections::BTreeSet;

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

/// Status of a table at the creation time of a [`KeyValueStoreClient`] instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableStatus {
    /// Table was created during the construction of the [`KeyValueStoreClient`] instance.
    New,
    /// Table already existed when the [`KeyValueStoreClient`] instance was created.
    Existing,
}

/// The common initialization parameters for the `KeyValueStore`
#[derive(Debug, Clone)]
pub struct CommonStoreConfig {
    /// The number of concurrent to a database
    pub max_concurrent_queries: Option<usize>,
    /// The number of streams used for the async streams.
    pub max_stream_queries: usize,
    /// The cache size being used.
    pub cache_size: usize,
}

impl Default for CommonStoreConfig {
    fn default() -> Self {
        CommonStoreConfig {
            max_concurrent_queries: None,
            max_stream_queries: 10,
            cache_size: 1000,
        }
    }
}

/// The minimum value for the view tags. Values in 0..MIN_VIEW_TAG are used for other purposes.
pub const MIN_VIEW_TAG: u8 = 1;

/// When wanting to find the entries in a BTreeMap with a specific prefix,
/// one option is to iterate over all keys. Another is to select an interval
/// that represents exactly the keys having that prefix. Which fortunately
/// is possible with the way the comparison operators for vectors are built.
///
/// The statement is that p is a prefix of v if and only if p <= v < upper_bound(p).
pub(crate) fn get_upper_bound_option(key_prefix: &[u8]) -> Option<Vec<u8>> {
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

/// The upper bound that can be used in ranges when accessing
/// a container. That is a vector v is a prefix of p if and only if
/// v belongs to the interval (Included(p), get_upper_bound(p)).
pub(crate) fn get_upper_bound(key_prefix: &[u8]) -> Bound<Vec<u8>> {
    match get_upper_bound_option(key_prefix) {
        None => Unbounded,
        Some(upper_bound) => Excluded(upper_bound),
    }
}

/// Computes an interval so that a vector has `key_prefix` as a prefix
/// if and only if it belongs to the range.
pub fn get_interval(key_prefix: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let upper_bound = get_upper_bound(&key_prefix);
    (Included(key_prefix), upper_bound)
}

pub(crate) fn from_bytes_opt<V: DeserializeOwned, E>(
    key_opt: &Option<Vec<u8>>,
) -> Result<Option<V>, E>
where
    E: From<bcs::Error>,
{
    match key_opt {
        Some(bytes) => {
            let value = bcs::from_bytes(bytes)?;
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

/// GreatestLowerBoundIterator iterates over the entries of a BTreeSet.
/// The function call `get_lower_bound(val)` returns a `Some(x)` where `x` is the highest
/// entry such that `x <= val`. If none exists then None is returned.
///
/// The function calls `is_index_absent` have to be done with increasing
/// values in order to get correct results.
pub(crate) struct GreatestLowerBoundIterator<'a, IT> {
    prefix_len: usize,
    prec1: Option<&'a Vec<u8>>,
    prec2: Option<&'a Vec<u8>>,
    iter: IT,
}

impl<'a, IT> GreatestLowerBoundIterator<'a, IT>
where
    IT: Iterator<Item = &'a Vec<u8>>,
{
    pub(crate) fn new(prefix_len: usize, mut iter: IT) -> Self {
        let prec1 = None;
        let prec2 = iter.next();
        Self {
            prefix_len,
            prec1,
            prec2,
            iter,
        }
    }

    fn compar(&self, x: &'a Vec<u8>, val: &[u8]) -> bool {
        let len1 = x.len() - self.prefix_len;
        let len2 = val.len();
        let min_len = len1.min(len2);
        for u in 0..min_len {
            if x[self.prefix_len + u] > val[u] {
                return true;
            }
        }
        len1 > len2
    }

    fn get_lower_bound(&mut self, val: &[u8]) -> Option<&'a Vec<u8>> {
        loop {
            match &self.prec2 {
                None => {
                    return self.prec1;
                }
                Some(x) => {
                    if self.compar(x, val) {
                        return self.prec1;
                    }
                }
            }
            let prec2 = self.iter.next();
            self.prec1 = std::mem::replace(&mut self.prec2, prec2);
        }
    }

    pub(crate) fn is_index_absent(&mut self, index: &[u8]) -> bool {
        let lower_bound = self.get_lower_bound(index);
        match lower_bound {
            None => true,
            Some(key_prefix) => !index.starts_with(&key_prefix[self.prefix_len..]),
        }
    }
}

#[test]
fn lower_bound_test1_the_lower_bound() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![4]);
    set.insert(vec![7]);
    set.insert(vec![8]);
    set.insert(vec![10]);
    set.insert(vec![24]);
    set.insert(vec![40]);

    let mut lower_bound = GreatestLowerBoundIterator::new(0, set.iter());
    assert_eq!(lower_bound.get_lower_bound(&[3]), None);
    assert_eq!(lower_bound.get_lower_bound(&[15]), Some(vec!(10)).as_ref());
    assert_eq!(lower_bound.get_lower_bound(&[17]), Some(vec!(10)).as_ref());
    assert_eq!(lower_bound.get_lower_bound(&[25]), Some(vec!(24)).as_ref());
    assert_eq!(lower_bound.get_lower_bound(&[27]), Some(vec!(24)).as_ref());
    assert_eq!(lower_bound.get_lower_bound(&[42]), Some(vec!(40)).as_ref());
}

#[test]
fn lower_bound_test2_is_index_absent() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![4]);
    set.insert(vec![0, 3]);
    set.insert(vec![5]);

    let mut lower_bound = GreatestLowerBoundIterator::new(0, set.iter());
    assert!(lower_bound.is_index_absent(&[0]));
    assert!(!lower_bound.is_index_absent(&[0, 3]));
    assert!(!lower_bound.is_index_absent(&[0, 3, 4]));
    assert!(lower_bound.is_index_absent(&[1]));
    assert!(!lower_bound.is_index_absent(&[4]));
}

#[test]
fn lower_bound_test3_is_index_absent_prefix_len() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![0, 4]);
    set.insert(vec![0, 3]);
    set.insert(vec![0, 0, 1]);

    let mut lower_bound = GreatestLowerBoundIterator::new(1, set.iter());
    assert!(lower_bound.is_index_absent(&[0]));
    assert!(!lower_bound.is_index_absent(&[0, 1]));
    assert!(!lower_bound.is_index_absent(&[0, 1, 4]));
    assert!(!lower_bound.is_index_absent(&[3]));
    assert!(lower_bound.is_index_absent(&[5]));
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
    /// The iterator that returns key-value pairs by reference.
    type Iterator<'a>: Iterator<Item = Result<(&'a [u8], &'a [u8]), Error>>
    where
        Self: 'a;

    /// The iterator that returns key-value pairs by value.
    type IteratorOwned: Iterator<Item = Result<(Vec<u8>, Vec<u8>), Error>>;

    /// Iterates keys and values by reference.
    fn iterator(&self) -> Self::Iterator<'_>;

    /// Iterates keys and values by value.
    fn into_iterator_owned(self) -> Self::IteratorOwned;
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueStoreClient {
    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// The error type.
    type Error: Debug;

    /// Returns type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Returns type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Retrieve the number of stream queries.
    fn max_stream_queries(&self) -> usize;

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
        from_bytes_opt(&self.read_key_bytes(key).await?)
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
            values.push(from_bytes_opt(&entry)?);
        }
        Ok(values)
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
    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// User-provided data to be carried along.
    type Extra: Clone + Send + Sync;

    /// The error type in use by internal operations.
    /// In practice, we always want `ViewError: From<Self::Error>` here.
    type Error: std::error::Error + Debug + Send + Sync + 'static + From<bcs::Error>;

    /// Returns type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Returns type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Retrieve the number of stream queries.
    fn max_stream_queries(&self) -> usize;

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

    /// Getter for the user-provided data.
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
        from_bytes_opt(&self.read_key_bytes(key).await?)
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
            values.push(from_bytes_opt(&entry)?);
        }
        Ok(values)
    }
}

/// Implementation of the [`Context`] trait on top of a DB client implementing
/// [`KeyValueStoreClient`].
#[derive(Debug, Default, Clone)]
pub struct ContextFromDb<E, DB> {
    /// The DB client that is shared between views.
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
    const MAX_VALUE_SIZE: usize = DB::MAX_VALUE_SIZE;
    type Extra = E;
    type Error = DB::Error;
    type Keys = DB::Keys;
    type KeyValues = DB::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.db.max_stream_queries()
    }

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
