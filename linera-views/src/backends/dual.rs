// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] by combining two existing stores.

use thiserror::Error;

#[cfg(with_testing)]
use crate::store::TestKeyValueStore;
use crate::{
    batch::Batch,
    store::{
        AdminKeyValueStore, KeyIterable, KeyValueIterable, KeyValueStoreError,
        ReadableKeyValueStore, WithError, WritableKeyValueStore,
    },
};

/// The initial configuration of the system.
#[derive(Debug)]
pub struct DualStoreConfig<C1, C2> {
    /// The first config.
    pub first_config: C1,
    /// The second config.
    pub second_config: C2,
}

/// The store in use.
#[derive(Clone, Copy, Debug)]
pub enum StoreInUse {
    /// The first store.
    First,
    /// The second store.
    Second,
}

/// The trait for a (static) root key assignement.
pub trait DualStoreRootKeyAssignment {
    /// Obtains the store assigned to this root key.
    fn assigned_store(root_key: &[u8]) -> Result<StoreInUse, bcs::Error>;
}

/// A store made of two existing stores.
#[derive(Clone)]
pub struct DualStore<S1, S2, A> {
    /// The first underlying store.
    first_store: S1,
    /// The second underlying store.
    second_store: S2,
    /// Which store is currently in use given the root key. (The root key in the other store will be set arbitrarily.)
    store_in_use: StoreInUse,
    /// Marker for the static root key assignement.
    _marker: std::marker::PhantomData<A>,
}

impl<S1, S2, A> WithError for DualStore<S1, S2, A>
where
    S1: WithError,
    S2: WithError,
{
    type Error = DualStoreError<S1::Error, S2::Error>;
}

impl<S1, S2, A> ReadableKeyValueStore for DualStore<S1, S2, A>
where
    S1: ReadableKeyValueStore + Send + Sync,
    S2: ReadableKeyValueStore + Send + Sync,
    A: Send + Sync,
{
    // TODO(#2524): consider changing MAX_KEY_SIZE into a function.
    const MAX_KEY_SIZE: usize = if S1::MAX_KEY_SIZE < S2::MAX_KEY_SIZE {
        S1::MAX_KEY_SIZE
    } else {
        S2::MAX_KEY_SIZE
    };

    type Keys = DualStoreKeys<S1::Keys, S2::Keys>;
    type KeyValues = DualStoreKeyValues<S1::KeyValues, S2::KeyValues>;

    fn max_stream_queries(&self) -> usize {
        match self.store_in_use {
            StoreInUse::First => self.first_store.max_stream_queries(),
            StoreInUse::Second => self.second_store.max_stream_queries(),
        }
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let result = match self.store_in_use {
            StoreInUse::First => self
                .first_store
                .read_value_bytes(key)
                .await
                .map_err(DualStoreError::First)?,
            StoreInUse::Second => self
                .second_store
                .read_value_bytes(key)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let result = match self.store_in_use {
            StoreInUse::First => self
                .first_store
                .contains_key(key)
                .await
                .map_err(DualStoreError::First)?,
            StoreInUse::Second => self
                .second_store
                .contains_key(key)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        let result = match self.store_in_use {
            StoreInUse::First => self
                .first_store
                .contains_keys(keys)
                .await
                .map_err(DualStoreError::First)?,
            StoreInUse::Second => self
                .second_store
                .contains_keys(keys)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let result = match self.store_in_use {
            StoreInUse::First => self
                .first_store
                .read_multi_values_bytes(keys)
                .await
                .map_err(DualStoreError::First)?,
            StoreInUse::Second => self
                .second_store
                .read_multi_values_bytes(keys)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        let result = match self.store_in_use {
            StoreInUse::First => DualStoreKeys::First(
                self.first_store
                    .find_keys_by_prefix(key_prefix)
                    .await
                    .map_err(DualStoreError::First)?,
            ),
            StoreInUse::Second => DualStoreKeys::Second(
                self.second_store
                    .find_keys_by_prefix(key_prefix)
                    .await
                    .map_err(DualStoreError::Second)?,
            ),
        };
        Ok(result)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let result = match self.store_in_use {
            StoreInUse::First => DualStoreKeyValues::First(
                self.first_store
                    .find_key_values_by_prefix(key_prefix)
                    .await
                    .map_err(DualStoreError::First)?,
            ),
            StoreInUse::Second => DualStoreKeyValues::Second(
                self.second_store
                    .find_key_values_by_prefix(key_prefix)
                    .await
                    .map_err(DualStoreError::Second)?,
            ),
        };
        Ok(result)
    }
}

impl<S1, S2, A> WritableKeyValueStore for DualStore<S1, S2, A>
where
    S1: WritableKeyValueStore + WithError + Send + Sync,
    S2: WritableKeyValueStore + WithError + Send + Sync,
    A: Send + Sync,
{
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        match self.store_in_use {
            StoreInUse::First => self
                .first_store
                .write_batch(batch)
                .await
                .map_err(DualStoreError::First)?,
            StoreInUse::Second => self
                .second_store
                .write_batch(batch)
                .await
                .map_err(DualStoreError::Second)?,
        }
        Ok(())
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        match self.store_in_use {
            StoreInUse::First => self
                .first_store
                .clear_journal()
                .await
                .map_err(DualStoreError::First)?,
            StoreInUse::Second => self
                .second_store
                .clear_journal()
                .await
                .map_err(DualStoreError::Second)?,
        }
        Ok(())
    }
}

impl<S1, S2, A> AdminKeyValueStore for DualStore<S1, S2, A>
where
    S1: AdminKeyValueStore + Send + Sync,
    S2: AdminKeyValueStore + Send + Sync,
    A: DualStoreRootKeyAssignment + Send + Sync,
{
    type Config = DualStoreConfig<S1::Config, S2::Config>;

    fn get_name() -> String {
        format!("dual {} and {}", S1::get_name(), S2::get_name())
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, Self::Error> {
        let first_store = S1::connect(&config.first_config, namespace, root_key)
            .await
            .map_err(DualStoreError::First)?;
        let second_store = S2::connect(&config.second_config, namespace, root_key)
            .await
            .map_err(DualStoreError::Second)?;
        let store_in_use = A::assigned_store(root_key)?;
        Ok(Self {
            first_store,
            second_store,
            store_in_use,
            _marker: std::marker::PhantomData,
        })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, Self::Error> {
        let first_store = self
            .first_store
            .clone_with_root_key(root_key)
            .map_err(DualStoreError::First)?;
        let second_store = self
            .second_store
            .clone_with_root_key(root_key)
            .map_err(DualStoreError::Second)?;
        let store_in_use = A::assigned_store(root_key)?;
        Ok(Self {
            first_store,
            second_store,
            store_in_use,
            _marker: std::marker::PhantomData,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        let namespaces1 = S1::list_all(&config.first_config)
            .await
            .map_err(DualStoreError::First)?;
        let mut namespaces = Vec::new();
        for namespace in namespaces1 {
            if S2::exists(&config.second_config, &namespace)
                .await
                .map_err(DualStoreError::Second)?
            {
                namespaces.push(namespace);
            } else {
                tracing::warn!("Namespace {} only exists in the first store", namespace);
            }
        }
        Ok(namespaces)
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        Ok(S1::exists(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::First)?
            && S2::exists(&config.second_config, namespace)
                .await
                .map_err(DualStoreError::Second)?)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        S1::create(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::First)?;
        S2::create(&config.second_config, namespace)
            .await
            .map_err(DualStoreError::Second)?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        S1::delete(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::First)?;
        S2::delete(&config.second_config, namespace)
            .await
            .map_err(DualStoreError::Second)?;
        Ok(())
    }
}

#[cfg(with_testing)]
impl<S1, S2, A> TestKeyValueStore for DualStore<S1, S2, A>
where
    S1: TestKeyValueStore + Send + Sync,
    S2: TestKeyValueStore + Send + Sync,
    A: DualStoreRootKeyAssignment + Send + Sync,
{
    async fn new_test_config() -> Result<Self::Config, Self::Error> {
        let first_config = S1::new_test_config().await.map_err(DualStoreError::First)?;
        let second_config = S2::new_test_config()
            .await
            .map_err(DualStoreError::Second)?;
        Ok(DualStoreConfig {
            first_config,
            second_config,
        })
    }
}

/// The error type for [`DualStore`].
#[derive(Error, Debug)]
pub enum DualStoreError<E1, E2> {
    /// Serialization error with BCS.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    /// First store.
    #[error("Error in first store: {0}")]
    First(E1),

    /// Second store.
    #[error("Error in second store: {0}")]
    Second(E2),
}

impl<E1, E2> KeyValueStoreError for DualStoreError<E1, E2>
where
    E1: KeyValueStoreError,
    E2: KeyValueStoreError,
{
    const BACKEND: &'static str = "dual_store";
}

/// A set of keys returned by [`DualStore::find_keys_by_prefix`].
pub enum DualStoreKeys<K1, K2> {
    /// A set of keys from the first store.
    First(K1),
    /// A set of Keys from the second store.
    Second(K2),
}

/// An iterator over the keys in [`DualStoreKeys`].
pub enum DualStoreKeyIterator<I1, I2> {
    /// Iterating over keys from the first store.
    First(I1),
    /// Iterating over keys from the second store.
    Second(I2),
}

/// A set of key-values returned by [`DualStore::find_key_values_by_prefix`].
pub enum DualStoreKeyValues<K1, K2> {
    /// A set of key-values from the first store.
    First(K1),
    /// A set of key-values from the second store.
    Second(K2),
}

/// An iterator over the key-values in [`DualStoreKeyValues`].
pub enum DualStoreKeyValueIterator<I1, I2> {
    /// Iterating over key-values from the first store.
    First(I1),
    /// Iterating over key-values from the second store.
    Second(I2),
}

/// An owning iterator over the key-values in [`DualStoreKeyValues`].
pub enum DualStoreKeyValueIteratorOwned<I1, I2> {
    /// Iterating over key-values from the first store.
    First(I1),
    /// Iterating over key-values from the second store.
    Second(I2),
}

impl<E1, E2, K1, K2> KeyIterable<DualStoreError<E1, E2>> for DualStoreKeys<K1, K2>
where
    K1: KeyIterable<E1>,
    K2: KeyIterable<E2>,
{
    type Iterator<'a>
        = DualStoreKeyIterator<K1::Iterator<'a>, K2::Iterator<'a>>
    where
        K1: 'a,
        K2: 'a;

    fn iterator(&self) -> Self::Iterator<'_> {
        match self {
            Self::First(keys) => DualStoreKeyIterator::First(keys.iterator()),
            Self::Second(keys) => DualStoreKeyIterator::Second(keys.iterator()),
        }
    }
}

impl<'a, I1, I2, E1, E2> Iterator for DualStoreKeyIterator<I1, I2>
where
    I1: Iterator<Item = Result<&'a [u8], E1>>,
    I2: Iterator<Item = Result<&'a [u8], E2>>,
{
    type Item = Result<&'a [u8], DualStoreError<E1, E2>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::First(iter) => iter
                .next()
                .map(|result| result.map_err(DualStoreError::First)),
            Self::Second(iter) => iter
                .next()
                .map(|result| result.map_err(DualStoreError::Second)),
        }
    }
}

impl<E1, E2, K1, K2> KeyValueIterable<DualStoreError<E1, E2>> for DualStoreKeyValues<K1, K2>
where
    K1: KeyValueIterable<E1>,
    K2: KeyValueIterable<E2>,
{
    type Iterator<'a>
        = DualStoreKeyValueIterator<K1::Iterator<'a>, K2::Iterator<'a>>
    where
        K1: 'a,
        K2: 'a;
    type IteratorOwned = DualStoreKeyValueIteratorOwned<K1::IteratorOwned, K2::IteratorOwned>;

    fn iterator(&self) -> Self::Iterator<'_> {
        match self {
            Self::First(keys) => DualStoreKeyValueIterator::First(keys.iterator()),
            Self::Second(keys) => DualStoreKeyValueIterator::Second(keys.iterator()),
        }
    }

    fn into_iterator_owned(self) -> Self::IteratorOwned {
        match self {
            Self::First(keys) => DualStoreKeyValueIteratorOwned::First(keys.into_iterator_owned()),
            Self::Second(keys) => {
                DualStoreKeyValueIteratorOwned::Second(keys.into_iterator_owned())
            }
        }
    }
}

impl<'a, I1, I2, E1, E2> Iterator for DualStoreKeyValueIterator<I1, I2>
where
    I1: Iterator<Item = Result<(&'a [u8], &'a [u8]), E1>>,
    I2: Iterator<Item = Result<(&'a [u8], &'a [u8]), E2>>,
{
    type Item = Result<(&'a [u8], &'a [u8]), DualStoreError<E1, E2>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::First(iter) => iter
                .next()
                .map(|result| result.map_err(DualStoreError::First)),
            Self::Second(iter) => iter
                .next()
                .map(|result| result.map_err(DualStoreError::Second)),
        }
    }
}

impl<I1, I2, E1, E2> Iterator for DualStoreKeyValueIteratorOwned<I1, I2>
where
    I1: Iterator<Item = Result<(Vec<u8>, Vec<u8>), E1>>,
    I2: Iterator<Item = Result<(Vec<u8>, Vec<u8>), E2>>,
{
    type Item = Result<(Vec<u8>, Vec<u8>), DualStoreError<E1, E2>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::First(iter) => iter
                .next()
                .map(|result| result.map_err(DualStoreError::First)),
            Self::Second(iter) => iter
                .next()
                .map(|result| result.map_err(DualStoreError::Second)),
        }
    }
}
