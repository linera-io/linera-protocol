// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides several functionalities for the handling of data.
//! The most important traits are:
//! * [`KeyValueStore`][trait1] which manages the access to a database and is clonable. It has a minimal interface
//! * [`Context`][trait2] which provides access to a database plus a `base_key` and some extra type `E` which is carried along
//! and has no impact on the running of the system. There is also a bunch of other helper functions.
//!
//! [trait1]: common::KeyValueStore
//! [trait2]: context::Context

use std::{
    collections::BTreeSet,
    fmt::Debug,
    future::Future,
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
    },
};

use serde::de::DeserializeOwned;

use crate::{batch::Batch, views::ViewError};

#[doc(hidden)]
pub type HasherOutputSize = <sha3::Sha3_256 as sha3::digest::OutputSizeUser>::OutputSize;
#[doc(hidden)]
pub type HasherOutput = generic_array::GenericArray<u8, HasherOutputSize>;

#[derive(Clone, Debug)]
pub(crate) enum Update<T> {
    Removed,
    Set(T),
}

#[derive(Clone, Debug)]
pub(crate) struct DeletionSet {
    pub(crate) delete_storage_first: bool,
    pub(crate) deleted_prefixes: BTreeSet<Vec<u8>>,
}

impl DeletionSet {
    pub(crate) fn new() -> Self {
        Self {
            delete_storage_first: false,
            deleted_prefixes: BTreeSet::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.delete_storage_first = true;
        self.deleted_prefixes.clear();
    }

    pub(crate) fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.deleted_prefixes.clear();
    }

    pub(crate) fn contains_prefix_of(&self, index: &[u8]) -> bool {
        self.delete_storage_first || contains_prefix_of(&self.deleted_prefixes, index)
    }

    pub(crate) fn has_pending_changes(&self) -> bool {
        self.delete_storage_first || !self.deleted_prefixes.is_empty()
    }

    pub(crate) fn insert_key_prefix(&mut self, key_prefix: Vec<u8>) {
        if !self.delete_storage_first {
            insert_key_prefix(&mut self.deleted_prefixes, key_prefix);
        }
    }
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
pub(crate) fn get_interval(key_prefix: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let upper_bound = get_upper_bound(&key_prefix);
    (Included(key_prefix), upper_bound)
}

/// Deserializes an Optional vector of u8
pub(crate) fn from_bytes_option<V: DeserializeOwned, E>(
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

pub(crate) fn from_bytes_option_or_default<V: DeserializeOwned + Default, E>(
    key_opt: &Option<Vec<u8>>,
) -> Result<V, E>
where
    E: From<bcs::Error>,
{
    match key_opt {
        Some(bytes) => Ok(bcs::from_bytes(bytes)?),
        None => Ok(V::default()),
    }
}

/// `SuffixClosedSetIterator` iterates over the entries of a container ordered
/// lexicographically.
///
/// The function call `find_lower_bound(val)` returns a `Some(x)` where `x` is the highest
/// entry such that `x <= val` for the lexicographic order. If none exists then None is
/// returned. The function calls have to be done with increasing `val`.
///
/// The function call `find_key(val)` tests whether there exists a prefix p in the
/// set of vectors such that p is a prefix of val.
pub(crate) struct SuffixClosedSetIterator<'a, I> {
    prefix_len: usize,
    previous: Option<&'a Vec<u8>>,
    current: Option<&'a Vec<u8>>,
    iter: I,
}

impl<'a, I> SuffixClosedSetIterator<'a, I>
where
    I: Iterator<Item = &'a Vec<u8>>,
{
    pub(crate) fn new(prefix_len: usize, mut iter: I) -> Self {
        let previous = None;
        let current = iter.next();
        Self {
            prefix_len,
            previous,
            current,
            iter,
        }
    }

    pub(crate) fn find_lower_bound(&mut self, val: &[u8]) -> Option<&'a Vec<u8>> {
        loop {
            match &self.current {
                None => {
                    return self.previous;
                }
                Some(x) => {
                    if &x[self.prefix_len..] > val {
                        return self.previous;
                    }
                }
            }
            let current = self.iter.next();
            self.previous = std::mem::replace(&mut self.current, current);
        }
    }

    pub(crate) fn find_key(&mut self, index: &[u8]) -> bool {
        let lower_bound = self.find_lower_bound(index);
        match lower_bound {
            None => false,
            Some(key_prefix) => index.starts_with(&key_prefix[self.prefix_len..]),
        }
    }
}

pub(crate) fn contains_prefix_of(prefixes: &BTreeSet<Vec<u8>>, key: &[u8]) -> bool {
    let iter = prefixes.iter();
    let mut suffix_closed_set = SuffixClosedSetIterator::new(0, iter);
    suffix_closed_set.find_key(key)
}

pub(crate) fn insert_key_prefix(prefixes: &mut BTreeSet<Vec<u8>>, prefix: Vec<u8>) {
    if !contains_prefix_of(prefixes, &prefix) {
        let key_prefix_list = prefixes
            .range(get_interval(prefix.clone()))
            .map(|x| x.to_vec())
            .collect::<Vec<_>>();
        for key in key_prefix_list {
            prefixes.remove(&key);
        }
        prefixes.insert(prefix);
    }
}

#[test]
fn suffix_closed_set_test1_the_lower_bound() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![4]);
    set.insert(vec![7]);
    set.insert(vec![8]);
    set.insert(vec![10]);
    set.insert(vec![24]);
    set.insert(vec![40]);

    let mut suffix_closed_set = SuffixClosedSetIterator::new(0, set.iter());
    assert_eq!(suffix_closed_set.find_lower_bound(&[3]), None);
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[15]),
        Some(vec![10]).as_ref()
    );
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[17]),
        Some(vec![10]).as_ref()
    );
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[25]),
        Some(vec![24]).as_ref()
    );
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[27]),
        Some(vec![24]).as_ref()
    );
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[42]),
        Some(vec![40]).as_ref()
    );
}

#[test]
fn suffix_closed_set_test2_find_key() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![4]);
    set.insert(vec![0, 3]);
    set.insert(vec![5]);

    let mut suffix_closed_set = SuffixClosedSetIterator::new(0, set.iter());
    assert!(!suffix_closed_set.find_key(&[0]));
    assert!(suffix_closed_set.find_key(&[0, 3]));
    assert!(suffix_closed_set.find_key(&[0, 3, 4]));
    assert!(!suffix_closed_set.find_key(&[1]));
    assert!(suffix_closed_set.find_key(&[4]));
}

#[test]
fn suffix_closed_set_test3_find_key_prefix_len() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![0, 4]);
    set.insert(vec![0, 3]);
    set.insert(vec![0, 0, 1]);

    let mut suffix_closed_set = SuffixClosedSetIterator::new(1, set.iter());
    assert!(!suffix_closed_set.find_key(&[0]));
    assert!(suffix_closed_set.find_key(&[0, 1]));
    assert!(suffix_closed_set.find_key(&[0, 1, 4]));
    assert!(suffix_closed_set.find_key(&[3]));
    assert!(!suffix_closed_set.find_key(&[5]));
}

#[test]
fn insert_key_prefix_test1() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![0, 4]);

    insert_key_prefix(&mut set, vec![0, 4, 5]);
    let keys = set.iter().cloned().collect::<Vec<_>>();
    assert_eq!(keys, vec![vec![0, 4]]);
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

/// The error type for the key-value stores.
pub trait KeyValueStoreError: std::error::Error + Debug + From<bcs::Error> {
    /// The name of the backend.
    const BACKEND: &'static str;
}

impl<E: KeyValueStoreError> From<E> for ViewError {
    fn from(error: E) -> Self {
        Self::StoreError {
            backend: E::BACKEND.to_string(),
            error: error.to_string(),
        }
    }
}

/// Define an associated [`KeyValueStoreError`].
pub trait WithError {
    /// The error type.
    type Error: KeyValueStoreError;
}

/// Low-level, asynchronous read key-value operations. Useful for storage APIs not based on views.
#[trait_variant::make(ReadableKeyValueStore: Send)]
pub trait LocalReadableKeyValueStore: WithError {
    /// The maximal size of keys that can be stored.
    const MAX_KEY_SIZE: usize;

    /// Returns type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Returns type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Retrieve the number of stream queries.
    fn max_stream_queries(&self) -> usize;

    /// Retrieves a `Vec<u8>` from the database using the provided `key`.
    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Tests whether a key exists in the database
    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error>;

    /// Tests whether a list of keys exist in the database
    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error>;

    /// Retrieves multiple `Vec<u8>` from the database using the provided `keys`.
    async fn read_multi_values_bytes(
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

    // We can't use `async fn` here in the below implementations due to
    // https://github.com/rust-lang/impl-trait-utils/issues/17, but once that bug is fixed
    // we can revert them to `async fn` syntax, which is neater.

    /// Reads a single `key` and deserializes the result if present.
    fn read_value<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<V>, Self::Error>>
    where
        Self: Sync,
    {
        async { from_bytes_option(&self.read_value_bytes(key).await?) }
    }

    /// Reads multiple `keys` and deserializes the results if present.
    fn read_multi_values<V: DeserializeOwned + Send>(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> impl Future<Output = Result<Vec<Option<V>>, Self::Error>>
    where
        Self: Sync,
    {
        async {
            let mut values = Vec::with_capacity(keys.len());
            for entry in self.read_multi_values_bytes(keys).await? {
                values.push(from_bytes_option(&entry)?);
            }
            Ok(values)
        }
    }
}

/// Low-level, asynchronous write key-value operations. Useful for storage APIs not based on views.
#[trait_variant::make(WritableKeyValueStore: Send)]
pub trait LocalWritableKeyValueStore: WithError {
    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// Writes the `batch` in the database.
    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error>;

    /// Clears any journal entry that may remain.
    /// The journal is located at the `root_key`.
    async fn clear_journal(&self) -> Result<(), Self::Error>;
}

/// Low-level trait for the administration of stores and their namespaces.
#[trait_variant::make(AdminKeyValueStore: Send)]
pub trait LocalAdminKeyValueStore: WithError + Sized {
    /// The configuration needed to interact with a new store.
    type Config: Send + Sync;

    /// Obtains a test config
    async fn new_test_config() -> Result<Self::Config, Self::Error>;

    /// Connects to an existing namespace using the given configuration.
    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, Self::Error>;

    /// Takes a connection and creates a new one with a different `root_key`.
    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, Self::Error>;

    /// Obtains the list of existing namespaces.
    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error>;

    /// Deletes all the existing namespaces.
    fn delete_all(config: &Self::Config) -> impl Future<Output = Result<(), Self::Error>> {
        async {
            let namespaces = Self::list_all(config).await?;
            for namespace in namespaces {
                Self::delete(config, &namespace).await?;
            }
            Ok(())
        }
    }

    /// Tests if a given namespace exists.
    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error>;

    /// Creates a namespace. Returns an error if the namespace exists.
    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error>;

    /// Deletes the given namespace.
    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error>;

    /// Initializes a storage if missing and provides it.
    fn maybe_create_and_connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> impl Future<Output = Result<Self, Self::Error>> {
        async {
            if !Self::exists(config, namespace).await? {
                Self::create(config, namespace).await?;
            }
            Self::connect(config, namespace, root_key).await
        }
    }

    /// Creates a new storage. Overwrites it if this namespace already exists.
    fn recreate_and_connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> impl Future<Output = Result<Self, Self::Error>> {
        async {
            if Self::exists(config, namespace).await? {
                Self::delete(config, namespace).await?;
            }
            Self::create(config, namespace).await?;
            Self::connect(config, namespace, root_key).await
        }
    }
}

/// Low-level, asynchronous write and read key-value operations. Useful for storage APIs not based on views.
pub trait RestrictedKeyValueStore: ReadableKeyValueStore + WritableKeyValueStore {}

impl<T> RestrictedKeyValueStore for T where T: ReadableKeyValueStore + WritableKeyValueStore {}

/// Low-level, asynchronous write and read key-value operations, without a `Send` bound. Useful for storage APIs not based on views.
pub trait LocalRestrictedKeyValueStore:
    LocalReadableKeyValueStore + LocalWritableKeyValueStore
{
}

impl<T> LocalRestrictedKeyValueStore for T where
    T: LocalReadableKeyValueStore + LocalWritableKeyValueStore
{
}

/// Low-level, asynchronous write and read key-value operations. Useful for storage APIs not based on views.
pub trait KeyValueStore:
    ReadableKeyValueStore + WritableKeyValueStore + AdminKeyValueStore
{
}

impl<T> KeyValueStore for T where
    T: ReadableKeyValueStore + WritableKeyValueStore + AdminKeyValueStore
{
}

/// Low-level, asynchronous write and read key-value operations, without a `Send` bound. Useful for storage APIs not based on views.
pub trait LocalKeyValueStore:
    LocalReadableKeyValueStore + LocalWritableKeyValueStore + LocalAdminKeyValueStore
{
}

impl<T> LocalKeyValueStore for T where
    T: LocalReadableKeyValueStore + LocalWritableKeyValueStore + LocalAdminKeyValueStore
{
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

/// This computes the offset of the BCS serialization of a vector.
/// The formula that should be satisfied is
/// serialized_size(vec![v_1, ...., v_n]) = get_uleb128_size(n)
///  + serialized_size(v_1)? + .... serialized_size(v_n)?
pub(crate) fn get_uleb128_size(len: usize) -> usize {
    let mut power = 128;
    let mut expo = 1;
    while len >= power {
        power *= 128;
        expo += 1;
    }
    expo
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use linera_views::common::CustomSerialize;
    use rand::Rng;

    #[test]
    fn test_ordering_serialization() {
        let mut rng = crate::test_utils::make_deterministic_rng();
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

#[test]
fn test_upper_bound() {
    assert_eq!(get_upper_bound(&[255]), Unbounded);
    assert_eq!(get_upper_bound(&[255, 255, 255, 255]), Unbounded);
    assert_eq!(get_upper_bound(&[0, 2]), Excluded(vec![0, 3]));
    assert_eq!(get_upper_bound(&[0, 255]), Excluded(vec![1]));
    assert_eq!(get_upper_bound(&[255, 0]), Excluded(vec![255, 1]));
}
