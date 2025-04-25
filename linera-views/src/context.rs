// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::DeletePrefixExpander,
    memory::MemoryStore,
    store::{
        KeyIterable, KeyValueStoreError, ReadableKeyValueStore, WithError, WritableKeyValueStore,
    },
    views::MIN_VIEW_TAG,
};

/// The context in which a view is operated. Typically, this includes the client to
/// connect to the database and the address of the current entry.
#[async_trait]
pub trait Context: Clone {
    /// The type of the key-value store used by this context.
    type Store: ReadableKeyValueStore
        + WritableKeyValueStore
        + WithError<Error = Self::Error>
        + Send
        + Sync;

    /// User-provided data to be carried along.
    type Extra: Clone + Send + Sync;

    /// The type of errors that may be returned by operations on the `Store`, a
    /// convenience alias for `<Self::Store as WithError>::Error`.
    type Error: KeyValueStoreError;

    /// Getter for the store.
    fn store(&self) -> &Self::Store;

    /// Getter for the user-provided data.
    fn extra(&self) -> &Self::Extra;

    /// Obtains a similar [`Context`] implementation with a different base key.
    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self;

    /// Getter for the address of the base key.
    fn base_key(&self) -> Vec<u8>;

    /// Concatenates the base key and tag.
    fn base_tag(&self, tag: u8) -> Vec<u8> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_key();
        key.extend([tag]);
        key
    }

    /// Concatenates the base key, tag and index.
    fn base_tag_index(&self, tag: u8, index: &[u8]) -> Vec<u8> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_key();
        key.extend([tag]);
        key.extend_from_slice(index);
        key
    }

    /// Concatenates the base key and index.
    fn base_index(&self, index: &[u8]) -> Vec<u8> {
        let mut key = self.base_key();
        key.extend_from_slice(index);
        key
    }

    /// Obtains the `Vec<u8>` key from the key by serialization and using the base key.
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
    S: ReadableKeyValueStore + WritableKeyValueStore,
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
    S: ReadableKeyValueStore + WritableKeyValueStore + Clone + Send + Sync,
    S::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
{
    type Extra = E;
    type Store = S;

    type Error = S::Error;

    fn store(&self) -> &Self::Store {
        &self.store
    }

    fn extra(&self) -> &E {
        &self.extra
    }

    fn base_key(&self) -> Vec<u8> {
        self.base_key.clone()
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

impl<E> MemoryContext<E> {
    /// Creates a [`Context`] instance in memory for testing.
    #[cfg(with_testing)]
    pub fn new_for_testing(extra: E) -> Self {
        let max_stream_queries = crate::memory::TEST_MEMORY_MAX_STREAM_QUERIES;
        let namespace = crate::random::generate_test_namespace();
        let store = MemoryStore::new_for_testing(max_stream_queries, &namespace).unwrap();
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
            &self.store().find_keys_by_prefix(key_prefix).await?,
        ) {
            vector_list.push(key?.to_vec());
        }
        Ok(vector_list)
    }
}
