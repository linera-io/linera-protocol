// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides the trait definitions for the stores.

use std::{fmt::Debug, future::Future};

use serde::{de::DeserializeOwned, Serialize};

#[cfg(with_testing)]
use crate::random::generate_test_namespace;
use crate::{
    batch::{Batch, SimplifiedBatch},
    common::from_bytes_option,
    ViewError,
};

/// The error type for the key-value stores.
pub trait KeyValueStoreError:
    std::error::Error + From<bcs::Error> + Debug + Send + Sync + 'static
{
    /// The name of the backend.
    const BACKEND: &'static str;
}

impl<E: KeyValueStoreError> From<E> for ViewError {
    fn from(error: E) -> Self {
        Self::StoreError {
            backend: E::BACKEND,
            error: Box::new(error),
        }
    }
}

/// Define an associated [`KeyValueStoreError`].
pub trait WithError {
    /// The error type.
    type Error: KeyValueStoreError;
}

/// Asynchronous read key-value operations.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
pub trait ReadableKeyValueStore: WithError {
    /// The maximal size of keys that can be stored.
    const MAX_KEY_SIZE: usize;

    /// Retrieve the number of stream queries.
    fn max_stream_queries(&self) -> usize;

    /// Gets the root key of the store.
    fn root_key(&self) -> Result<Vec<u8>, Self::Error>;

    /// Retrieves a `Vec<u8>` from the database using the provided `key`.
    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Tests whether a key exists in the database
    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error>;

    /// Tests whether a list of keys exist in the database
    async fn contains_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>, Self::Error>;

    /// Retrieves multiple `Vec<u8>` from the database using the provided `keys`.
    async fn read_multi_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

    /// Finds the `key` matching the prefix. The prefix is not included in the returned keys.
    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Finds the `(key,value)` pairs matching the prefix. The prefix is not included in the returned keys.
    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error>;

    // We can't use `async fn` here in the below implementations due to
    // https://github.com/rust-lang/impl-trait-utils/issues/17, but once that bug is fixed
    // we can revert them to `async fn` syntax, which is neater.

    /// Reads a single `key` and deserializes the result if present.
    fn read_value<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<V>, Self::Error>> {
        async { Ok(from_bytes_option(&self.read_value_bytes(key).await?)?) }
    }

    /// Reads multiple `keys` and deserializes the results if present.
    fn read_multi_values<V: DeserializeOwned + Send + Sync>(
        &self,
        keys: &[Vec<u8>],
    ) -> impl Future<Output = Result<Vec<Option<V>>, Self::Error>> {
        async {
            let mut values = Vec::with_capacity(keys.len());
            for entry in self.read_multi_values_bytes(keys).await? {
                values.push(from_bytes_option(&entry)?);
            }
            Ok(values)
        }
    }
}

/// Asynchronous write key-value operations.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
pub trait WritableKeyValueStore: WithError {
    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// Writes the `batch` in the database.
    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error>;

    /// Clears any journal entry that may remain.
    /// The journal is located at the `root_key`.
    async fn clear_journal(&self) -> Result<(), Self::Error>;
}

/// Asynchronous direct write key-value operations with simplified batch.
///
/// Some backend cannot implement `WritableKeyValueStore` directly and will require
/// journaling.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
pub trait DirectWritableKeyValueStore: WithError {
    /// The maximal number of items in a batch.
    const MAX_BATCH_SIZE: usize;

    /// The maximal number of bytes of a batch.
    const MAX_BATCH_TOTAL_SIZE: usize;

    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// The batch type.
    type Batch: SimplifiedBatch + Serialize + DeserializeOwned + Default;

    /// Writes the batch to the database.
    async fn write_batch(&self, batch: Self::Batch) -> Result<(), Self::Error>;
}

/// The definition of a key-value database.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
pub trait KeyValueDatabase: WithError + linera_base::util::traits::AutoTraits + Sized {
    /// The configuration needed to interact with a new backend.
    type Config: Send + Sync;

    /// The result of opening a partition.
    type Store;

    /// The name of this database.
    fn get_name() -> String;

    /// Connects to an existing namespace using the given configuration.
    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, Self::Error>;

    /// Opens a shared partition starting at `root_key`. It is understood that the
    /// partition MAY be read and written simultaneously from other clients.
    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error>;

    /// Opens an exclusive partition starting at `root_key`. It is assumed that the
    /// partition WILL NOT be read and written simultaneously by other clients.
    ///
    /// IMPORTANT: This assumption is not enforced at the moment. However, future
    /// implementations may choose to return an error if another client is detected.
    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error>;

    /// Obtains the list of existing namespaces.
    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error>;

    /// Lists the root keys of the namespace.
    /// It is possible that some root keys have no keys.
    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>, Self::Error>;

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
    ) -> impl Future<Output = Result<Self, Self::Error>> {
        async {
            if !Self::exists(config, namespace).await? {
                Self::create(config, namespace).await?;
            }
            Self::connect(config, namespace).await
        }
    }

    /// Creates a new storage. Overwrites it if this namespace already exists.
    fn recreate_and_connect(
        config: &Self::Config,
        namespace: &str,
    ) -> impl Future<Output = Result<Self, Self::Error>> {
        async {
            if Self::exists(config, namespace).await? {
                Self::delete(config, namespace).await?;
            }
            Self::create(config, namespace).await?;
            Self::connect(config, namespace).await
        }
    }
}

/// A key-value store that can perform both read and direct write operations.
///
/// This trait combines the capabilities of [`ReadableKeyValueStore`] and
/// [`DirectWritableKeyValueStore`], providing a full interface for stores
/// that can handle simplified batches directly without journaling.
pub trait DirectKeyValueStore: ReadableKeyValueStore + DirectWritableKeyValueStore {}

impl<T> DirectKeyValueStore for T where T: ReadableKeyValueStore + DirectWritableKeyValueStore {}

/// A key-value store that can perform both read and write operations.
///
/// This trait combines the capabilities of [`ReadableKeyValueStore`] and
/// [`WritableKeyValueStore`], providing a full interface for stores that
/// can handle complex batches with journaling support.
pub trait KeyValueStore: ReadableKeyValueStore + WritableKeyValueStore {}

impl<T> KeyValueStore for T where T: ReadableKeyValueStore + WritableKeyValueStore {}

/// The functions needed for testing purposes
#[cfg(with_testing)]
pub trait TestKeyValueDatabase: KeyValueDatabase {
    /// Obtains a test config
    async fn new_test_config() -> Result<Self::Config, Self::Error>;

    /// Creates a database for testing purposes
    async fn connect_test_namespace() -> Result<Self, Self::Error> {
        let config = Self::new_test_config().await?;
        let namespace = generate_test_namespace();
        Self::recreate_and_connect(&config, &namespace).await
    }

    /// Creates a store for testing purposes
    async fn new_test_store() -> Result<Self::Store, Self::Error> {
        let database = Self::connect_test_namespace().await?;
        database.open_shared(&[])
    }
}

/// A module containing a dummy store used for caching views.
pub mod inactive_store {
    use super::*;

    /// A store which does not actually store anything - used for caching views.
    pub struct InactiveStore;

    /// An error struct for the inactive store.
    #[derive(Clone, Copy, Debug)]
    pub struct InactiveStoreError;

    impl std::fmt::Display for InactiveStoreError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "inactive store error")
        }
    }

    impl From<bcs::Error> for InactiveStoreError {
        fn from(_other: bcs::Error) -> Self {
            Self
        }
    }

    impl std::error::Error for InactiveStoreError {}

    impl KeyValueStoreError for InactiveStoreError {
        const BACKEND: &'static str = "inactive";
    }

    impl WithError for InactiveStore {
        type Error = InactiveStoreError;
    }

    impl ReadableKeyValueStore for InactiveStore {
        const MAX_KEY_SIZE: usize = 0;

        fn max_stream_queries(&self) -> usize {
            0
        }

        fn root_key(&self) -> Result<Vec<u8>, Self::Error> {
            panic!("attempt to read from an inactive store!")
        }

        async fn read_value_bytes(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            panic!("attempt to read from an inactive store!")
        }

        async fn contains_key(&self, _key: &[u8]) -> Result<bool, Self::Error> {
            panic!("attempt to read from an inactive store!")
        }

        async fn contains_keys(&self, _keys: &[Vec<u8>]) -> Result<Vec<bool>, Self::Error> {
            panic!("attempt to read from an inactive store!")
        }

        async fn read_multi_values_bytes(
            &self,
            _keys: &[Vec<u8>],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            panic!("attempt to read from an inactive store!")
        }

        async fn find_keys_by_prefix(
            &self,
            _key_prefix: &[u8],
        ) -> Result<Vec<Vec<u8>>, Self::Error> {
            panic!("attempt to read from an inactive store!")
        }

        /// Finds the `(key,value)` pairs matching the prefix. The prefix is not included in the returned keys.
        async fn find_key_values_by_prefix(
            &self,
            _key_prefix: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
            panic!("attempt to read from an inactive store!")
        }
    }

    impl WritableKeyValueStore for InactiveStore {
        const MAX_VALUE_SIZE: usize = 0;

        async fn write_batch(&self, _batch: Batch) -> Result<(), Self::Error> {
            panic!("attempt to write to an inactive store!")
        }

        async fn clear_journal(&self) -> Result<(), Self::Error> {
            panic!("attempt to write to an inactive store!")
        }
    }
}
