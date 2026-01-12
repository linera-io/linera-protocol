// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] by combining two existing stores.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::Batch,
    store::{
        KeyValueDatabase, KeyValueStoreError, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
    },
};

/// A dual database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualDatabase<D1, D2, A> {
    /// The first database.
    pub first_database: D1,
    /// The second database.
    pub second_database: D2,
    /// Marker for the static root key assignment.
    _marker: std::marker::PhantomData<A>,
}

/// The initial configuration of the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// The trait for a (static) root key assignment.
pub trait DualStoreRootKeyAssignment {
    /// Obtains the store assigned to this root key.
    fn assigned_store(root_key: &[u8]) -> Result<StoreInUse, bcs::Error>;
}

/// A partition opened in one of the two databases.
#[derive(Clone)]
pub enum DualStore<S1, S2> {
    /// The first store.
    First(S1),
    /// The second store.
    Second(S2),
}

impl<D1, D2, A> WithError for DualDatabase<D1, D2, A>
where
    D1: WithError,
    D2: WithError,
{
    type Error = DualStoreError<D1::Error, D2::Error>;
}

impl<S1, S2> WithError for DualStore<S1, S2>
where
    S1: WithError,
    S2: WithError,
{
    type Error = DualStoreError<S1::Error, S2::Error>;
}

impl<S1, S2> ReadableKeyValueStore for DualStore<S1, S2>
where
    S1: ReadableKeyValueStore,
    S2: ReadableKeyValueStore,
{
    // TODO(#2524): consider changing MAX_KEY_SIZE into a function.
    const MAX_KEY_SIZE: usize = if S1::MAX_KEY_SIZE < S2::MAX_KEY_SIZE {
        S1::MAX_KEY_SIZE
    } else {
        S2::MAX_KEY_SIZE
    };

    fn max_stream_queries(&self) -> usize {
        match self {
            Self::First(store) => store.max_stream_queries(),
            Self::Second(store) => store.max_stream_queries(),
        }
    }

    fn root_key(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(match self {
            Self::First(store) => store.root_key().map_err(DualStoreError::First)?,
            Self::Second(store) => store.root_key().map_err(DualStoreError::Second)?,
        })
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let result = match self {
            Self::First(store) => store
                .read_value_bytes(key)
                .await
                .map_err(DualStoreError::First)?,
            Self::Second(store) => store
                .read_value_bytes(key)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let result = match self {
            Self::First(store) => store
                .contains_key(key)
                .await
                .map_err(DualStoreError::First)?,
            Self::Second(store) => store
                .contains_key(key)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn contains_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>, Self::Error> {
        let result = match self {
            Self::First(store) => store
                .contains_keys(keys)
                .await
                .map_err(DualStoreError::First)?,
            Self::Second(store) => store
                .contains_keys(keys)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let result = match self {
            Self::First(store) => store
                .read_multi_values_bytes(keys)
                .await
                .map_err(DualStoreError::First)?,
            Self::Second(store) => store
                .read_multi_values_bytes(keys)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let result = match self {
            Self::First(store) => store
                .find_keys_by_prefix(key_prefix)
                .await
                .map_err(DualStoreError::First)?,
            Self::Second(store) => store
                .find_keys_by_prefix(key_prefix)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let result = match self {
            Self::First(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(DualStoreError::First)?,
            Self::Second(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(DualStoreError::Second)?,
        };
        Ok(result)
    }
}

impl<S1, S2> WritableKeyValueStore for DualStore<S1, S2>
where
    S1: WritableKeyValueStore,
    S2: WritableKeyValueStore,
{
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        match self {
            Self::First(store) => store
                .write_batch(batch)
                .await
                .map_err(DualStoreError::First)?,
            Self::Second(store) => store
                .write_batch(batch)
                .await
                .map_err(DualStoreError::Second)?,
        }
        Ok(())
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        match self {
            Self::First(store) => store.clear_journal().await.map_err(DualStoreError::First)?,
            Self::Second(store) => store
                .clear_journal()
                .await
                .map_err(DualStoreError::Second)?,
        }
        Ok(())
    }
}

impl<D1, D2, A> KeyValueDatabase for DualDatabase<D1, D2, A>
where
    D1: KeyValueDatabase,
    D2: KeyValueDatabase,
    A: DualStoreRootKeyAssignment + linera_base::util::traits::AutoTraits,
{
    type Config = DualStoreConfig<D1::Config, D2::Config>;
    type Store = DualStore<D1::Store, D2::Store>;

    fn get_name() -> String {
        format!("dual {} and {}", D1::get_name(), D2::get_name())
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, Self::Error> {
        let first_database = D1::connect(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::First)?;
        let second_database = D2::connect(&config.second_config, namespace)
            .await
            .map_err(DualStoreError::Second)?;
        let database = Self {
            first_database,
            second_database,
            _marker: std::marker::PhantomData,
        };
        Ok(database)
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        match A::assigned_store(root_key)? {
            StoreInUse::First => {
                let store = self
                    .first_database
                    .open_shared(root_key)
                    .map_err(DualStoreError::First)?;
                Ok(DualStore::First(store))
            }
            StoreInUse::Second => {
                let store = self
                    .second_database
                    .open_shared(root_key)
                    .map_err(DualStoreError::Second)?;
                Ok(DualStore::Second(store))
            }
        }
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        match A::assigned_store(root_key)? {
            StoreInUse::First => {
                let store = self
                    .first_database
                    .open_exclusive(root_key)
                    .map_err(DualStoreError::First)?;
                Ok(DualStore::First(store))
            }
            StoreInUse::Second => {
                let store = self
                    .second_database
                    .open_exclusive(root_key)
                    .map_err(DualStoreError::Second)?;
                Ok(DualStore::Second(store))
            }
        }
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        let namespaces1 = D1::list_all(&config.first_config)
            .await
            .map_err(DualStoreError::First)?;
        let mut namespaces = Vec::new();
        for namespace in namespaces1 {
            if D2::exists(&config.second_config, &namespace)
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

    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut root_keys = self
            .first_database
            .list_root_keys()
            .await
            .map_err(DualStoreError::First)?;
        root_keys.extend(
            self.second_database
                .list_root_keys()
                .await
                .map_err(DualStoreError::Second)?,
        );
        Ok(root_keys)
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        Ok(D1::exists(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::First)?
            && D2::exists(&config.second_config, namespace)
                .await
                .map_err(DualStoreError::Second)?)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        let exists1 = D1::exists(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::First)?;
        let exists2 = D2::exists(&config.second_config, namespace)
            .await
            .map_err(DualStoreError::Second)?;
        if exists1 && exists2 {
            return Err(DualStoreError::StoreAlreadyExists);
        }
        if !exists1 {
            D1::create(&config.first_config, namespace)
                .await
                .map_err(DualStoreError::First)?;
        }
        if !exists2 {
            D2::create(&config.second_config, namespace)
                .await
                .map_err(DualStoreError::Second)?;
        }
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        D1::delete(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::First)?;
        D2::delete(&config.second_config, namespace)
            .await
            .map_err(DualStoreError::Second)?;
        Ok(())
    }
}

#[cfg(with_testing)]
impl<D1, D2, A> TestKeyValueDatabase for DualDatabase<D1, D2, A>
where
    D1: TestKeyValueDatabase,
    D2: TestKeyValueDatabase,
    A: DualStoreRootKeyAssignment + linera_base::util::traits::AutoTraits,
{
    async fn new_test_config() -> Result<Self::Config, Self::Error> {
        let first_config = D1::new_test_config().await.map_err(DualStoreError::First)?;
        let second_config = D2::new_test_config()
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
    /// Store already exists during a create operation
    #[error("Store already exists during a create operation")]
    StoreAlreadyExists,

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
