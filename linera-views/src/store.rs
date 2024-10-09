// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides the trait definitions for the stores.

use std::{fmt::Debug, future::Future};

use serde::de::DeserializeOwned;

#[cfg(with_testing)]
use crate::random::generate_test_namespace;
use crate::{batch::Batch, common::from_bytes_option, views::ViewError};

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
                values.push(from_bytes_option::<_, bcs::Error>(&entry)?);
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

/// The functions needed for testing purposes
#[cfg(with_testing)]
pub trait TestKeyValueStore: KeyValueStore {
    /// Obtains a test config
    fn new_test_config(
    ) -> impl std::future::Future<Output = Result<Self::Config, Self::Error>> + Send;

    /// Creates a store for testing purposes
    fn new_test_store() -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send {
        async {
            let config = Self::new_test_config().await?;
            let namespace = generate_test_namespace();
            let root_key = &[];
            Self::recreate_and_connect(&config, &namespace, root_key).await
        }
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
