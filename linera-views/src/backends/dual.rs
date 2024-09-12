// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::common::KeyValueStore`] by combining two existing stores.

use thiserror::Error;

use crate::{
    batch::Batch,
    common::{
        AdminKeyValueStore, KeyValueStoreError, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
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
#[derive(Debug, Clone, Copy)]
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
    const MAX_KEY_SIZE: usize = if S1::MAX_KEY_SIZE < S2::MAX_KEY_SIZE {
        S1::MAX_KEY_SIZE
    } else {
        S2::MAX_KEY_SIZE
    }; // Not amazing but that should be sufficient for now.

    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

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
                .map_err(DualStoreError::FirstStoreError)?,
            StoreInUse::Second => self
                .second_store
                .read_value_bytes(key)
                .await
                .map_err(DualStoreError::SecondStoreError)?,
        };
        Ok(result)
    }

    async fn contains_key(&self, _key: &[u8]) -> Result<bool, Self::Error> {
        todo!()
    }

    async fn contains_keys(&self, _keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        todo!()
    }

    async fn read_multi_values_bytes(
        &self,
        _keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        todo!()
    }

    async fn find_keys_by_prefix(&self, _key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        todo!()
    }

    async fn find_key_values_by_prefix(
        &self,
        _key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        todo!()
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
                .map_err(DualStoreError::FirstStoreError)?,
            StoreInUse::Second => self
                .second_store
                .write_batch(batch)
                .await
                .map_err(DualStoreError::SecondStoreError)?,
        }
        Ok(())
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        // TODO: either this or clear both journals instead?
        match self.store_in_use {
            StoreInUse::First => self
                .first_store
                .clear_journal()
                .await
                .map_err(DualStoreError::FirstStoreError)?,
            StoreInUse::Second => self
                .second_store
                .clear_journal()
                .await
                .map_err(DualStoreError::SecondStoreError)?,
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

    async fn new_test_config() -> Result<Self::Config, Self::Error> {
        todo!()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, Self::Error> {
        let first_store = S1::connect(&config.first_config, namespace, root_key)
            .await
            .map_err(DualStoreError::FirstStoreError)?;
        let second_store = S2::connect(&config.second_config, namespace, root_key)
            .await
            .map_err(DualStoreError::SecondStoreError)?;
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
            .map_err(DualStoreError::FirstStoreError)?;
        let second_store = self
            .second_store
            .clone_with_root_key(root_key)
            .map_err(DualStoreError::SecondStoreError)?;
        let store_in_use = A::assigned_store(root_key)?;
        Ok(Self {
            first_store,
            second_store,
            store_in_use,
            _marker: std::marker::PhantomData,
        })
    }

    async fn list_all(_config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        // Not sure what we want here. The intersection?
        todo!()
    }

    async fn exists(_config: &Self::Config, _namespace: &str) -> Result<bool, Self::Error> {
        // Not sure what we want here. The conjunction?
        todo!()
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        S1::create(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::FirstStoreError)?;
        S2::create(&config.second_config, namespace)
            .await
            .map_err(DualStoreError::SecondStoreError)?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        S1::delete(&config.first_config, namespace)
            .await
            .map_err(DualStoreError::FirstStoreError)?;
        S2::delete(&config.second_config, namespace)
            .await
            .map_err(DualStoreError::SecondStoreError)?;
        Ok(())
    }
}

/// The error type for [`DualStore`].
#[derive(Error, Debug)]
pub enum DualStoreError<E1, E2> {
    /// Serialization error with BCS.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    /// First store.
    #[error("Error in first store: {0}")]
    FirstStoreError(E1),

    /// Second store.
    #[error("Error in second store: {0}")]
    SecondStoreError(E2),
}

impl<E1, E2> KeyValueStoreError for DualStoreError<E1, E2>
where
    E1: KeyValueStoreError,
    E2: KeyValueStoreError,
{
    const BACKEND: &'static str = "dual_store";
}
