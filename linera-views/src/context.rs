// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::{Batch, DeletePrefixExpander},
    common::from_bytes_option,
    memory::MemoryStore,
    store::{KeyIterable, KeyValueIterable, KeyValueStoreError, RestrictedKeyValueStore},
    views::MIN_VIEW_TAG,
};

/// The context in which a view is operated. Typically, this includes the client to
/// connect to the database and the address of the current entry.
#[async_trait]
pub trait Context: Clone {
    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// The maximal size of keys that can be stored.
    const MAX_KEY_SIZE: usize;

    /// User-provided data to be carried along.
    type Extra: Clone + Send + Sync;

    /// The error type in use by internal operations.
    type Error: KeyValueStoreError + Send + Sync + 'static;

    /// Returns type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Returns type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Retrieves the number of stream queries.
    fn max_stream_queries(&self) -> usize;

    /// Retrieves a `Vec<u8>` from the database using the provided `key` prefixed by the current
    /// context.
    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Tests whether a key exists in the database
    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error>;

    /// Tests whether a set of keys exist in the database
    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error>;

    /// Retrieves multiple `Vec<u8>` from the database using the provided `keys`.
    async fn read_multi_values_bytes(
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

    /// Getter for the address of the base key.
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

    /// Concatenates the base_key and index.
    fn base_index(&self, index: &[u8]) -> Vec<u8> {
        let mut key = self.base_key();
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
    async fn read_value<Item>(&self, key: &[u8]) -> Result<Option<Item>, Self::Error>
    where
        Item: DeserializeOwned,
    {
        from_bytes_option(&self.read_value_bytes(key).await?)
    }

    /// Reads multiple `keys` and deserializes the results if present.
    async fn read_multi_values<V: DeserializeOwned + Send>(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<V>>, Self::Error>
    where
        Self::Error: From<bcs::Error>,
    {
        let mut values = Vec::with_capacity(keys.len());
        for entry in self.read_multi_values_bytes(keys).await? {
            values.push(from_bytes_option(&entry)?);
        }
        Ok(values)
    }
}

/// Implementation of the [`Context`] trait on top of a DB client implementing
/// [`crate::store::KeyValueStore`].
#[derive(Debug, Default, Clone)]
pub struct ViewContext<E, S> {
    /// The DB client that is shared between views.
    store: S,
    /// The base key for the context.
    base_key: Vec<u8>,
    /// User-defined data attached to the view.
    extra: E,
}

impl<E, S> ViewContext<E, S>
where
    S: RestrictedKeyValueStore,
{
    /// Creates a context suitable for a root view, using the given store. If the
    /// journal's store is non-empty, it will be cleared first, before the context is
    /// returned.
    pub async fn create_root_context(store: S, extra: E) -> Result<Self, S::Error> {
        store.clear_journal().await?;
        Ok(Self::new_unsafe(store, Vec::new(), extra))
    }
}

impl<E, S> ViewContext<E, S> {
    /// Creates a context for the given base key, store, and an extra argument. NOTE: this
    /// constructor doesn't check the journal of the store. In doubt, use
    /// [`ViewContext::create_root_context`] instead.
    pub fn new_unsafe(store: S, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            store,
            base_key,
            extra,
        }
    }
}

#[async_trait]
impl<E, S> Context for ViewContext<E, S>
where
    E: Clone + Send + Sync,
    S: RestrictedKeyValueStore + Clone + Send + Sync,
    S::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
{
    const MAX_VALUE_SIZE: usize = S::MAX_VALUE_SIZE;
    const MAX_KEY_SIZE: usize = S::MAX_KEY_SIZE;
    type Extra = E;
    type Error = S::Error;
    type Keys = S::Keys;
    type KeyValues = S::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    fn extra(&self) -> &E {
        &self.extra
    }

    fn base_key(&self) -> Vec<u8> {
        self.base_key.clone()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        if keys.is_empty() {
            Ok(Vec::new())
        } else {
            self.store.contains_keys(keys).await
        }
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        if keys.is_empty() {
            Ok(Vec::new())
        } else {
            self.store.read_multi_values_bytes(keys).await
        }
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        self.store.write_batch(batch).await
    }

    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self {
        Self {
            store: self.store.clone(),
            base_key,
            extra: self.extra.clone(),
        }
    }
}

/// An implementation of [`crate::context::Context`] that stores all values in memory.
pub type MemoryContext<E> = ViewContext<E, MemoryStore>;

/// Provides a `MemoryContext<()>` that can be used for tests.
#[cfg(with_testing)]
pub fn create_test_memory_context() -> MemoryContext<()> {
    let namespace = crate::random::generate_test_namespace();
    let root_key = &[];
    MemoryContext::new_for_testing(
        crate::memory::TEST_MEMORY_MAX_STREAM_QUERIES,
        &namespace,
        root_key,
        (),
    )
}

impl<E> MemoryContext<E> {
    /// Creates a [`Context`] instance in memory for testing.
    #[cfg(with_testing)]
    pub fn new_for_testing(
        max_stream_queries: usize,
        namespace: &str,
        root_key: &[u8],
        extra: E,
    ) -> Self {
        let store = MemoryStore::new_for_testing(max_stream_queries, namespace, root_key).unwrap();
        let base_key = Vec::new();
        Self {
            store,
            base_key,
            extra,
        }
    }
}

impl DeletePrefixExpander for MemoryContext<()> {
    type Error = crate::memory::MemoryStoreError;

    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut vector_list = Vec::new();
        for key in <Vec<Vec<u8>> as KeyIterable<Self::Error>>::iterator(
            &self.find_keys_by_prefix(key_prefix).await?,
        ) {
            vector_list.push(key?.to_vec());
        }
        Ok(vector_list)
    }
}
