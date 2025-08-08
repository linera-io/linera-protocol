// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::DeletePrefixExpander,
    memory::MemoryStore,
    store::{KeyValueStoreError, ReadableKeyValueStore, WithError, WritableKeyValueStore},
    views::MIN_VIEW_TAG,
};

/// A wrapper over `Vec<u8>` with functions for using it as a key prefix.
#[derive(Default, Debug, Clone, derive_more::From)]
pub struct BaseKey {
    /// The byte value of the key prefix.
    #[from]
    pub bytes: Vec<u8>,
}

impl BaseKey {
    /// Concatenates the base key and tag.
    pub fn base_tag(&self, tag: u8) -> Vec<u8> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = Vec::with_capacity(self.bytes.len() + 1);
        key.extend_from_slice(&self.bytes);
        key.push(tag);
        key
    }

    /// Concatenates the base key, tag and index.
    pub fn base_tag_index(&self, tag: u8, index: &[u8]) -> Vec<u8> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = Vec::with_capacity(self.bytes.len() + 1 + index.len());
        key.extend_from_slice(&self.bytes);
        key.push(tag);
        key.extend_from_slice(index);
        key
    }

    /// Concatenates the base key and index.
    pub fn base_index(&self, index: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.bytes.len() + index.len());
        key.extend_from_slice(&self.bytes);
        key.extend_from_slice(index);
        key
    }

    /// Obtains the `Vec<u8>` key from the key by serialization and using the base key.
    pub fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>, bcs::Error> {
        let mut key = self.bytes.clone();
        bcs::serialize_into(&mut key, index)?;
        assert!(
            key.len() > self.bytes.len(),
            "Empty indices are not allowed"
        );
        Ok(key)
    }

    /// Obtains the `Vec<u8>` key from the key by serialization and using the `base_key`.
    pub fn derive_tag_key<I: Serialize>(&self, tag: u8, index: &I) -> Result<Vec<u8>, bcs::Error> {
        assert!(tag >= MIN_VIEW_TAG, "tag should be at least MIN_VIEW_TAG");
        let mut key = self.base_tag(tag);
        bcs::serialize_into(&mut key, index)?;
        Ok(key)
    }

    /// Obtains the short `Vec<u8>` key from the key by serialization.
    pub fn derive_short_key<I: Serialize + ?Sized>(index: &I) -> Result<Vec<u8>, bcs::Error> {
        bcs::to_bytes(index)
    }

    /// Deserialize `bytes` into type `Item`.
    pub fn deserialize_value<Item: DeserializeOwned>(bytes: &[u8]) -> Result<Item, bcs::Error> {
        bcs::from_bytes(bytes)
    }
}

/// The context in which a view is operated. Typically, this includes the client to
/// connect to the database and the address of the current entry.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
pub trait Context: Clone
where
    crate::ViewError: From<Self::Error>,
{
    /// The type of the key-value store used by this context.
    type Store: ReadableKeyValueStore + WritableKeyValueStore + WithError<Error = Self::Error>;

    /// User-provided data to be carried along.
    type Extra: Clone + Send + Sync;

    /// The type of errors that may be returned by operations on the `Store`, a
    /// convenience alias for `<Self::Store as WithError>::Error`.
    type Error: KeyValueStoreError;

    /// Getter for the store.
    fn store(&self) -> &Self::Store;

    /// Getter for the user-provided data.
    fn extra(&self) -> &Self::Extra;

    /// Getter for the address of the base key.
    fn base_key(&self) -> &BaseKey;

    /// Mutable getter for the address of the base key.
    fn base_key_mut(&mut self) -> &mut BaseKey;

    /// Obtains a similar [`Context`] implementation with a different base key.
    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self {
        let mut context = self.clone();
        context.base_key_mut().bytes = base_key;
        context
    }
}

/// A context which can't be used to read or write data, only used for caching views.
#[derive(Debug, Default, Clone)]
pub struct InactiveContext(pub BaseKey);

impl Context for InactiveContext {
    type Store = crate::store::inactive_store::InactiveStore;
    type Extra = ();

    type Error = crate::store::inactive_store::InactiveStoreError;

    fn store(&self) -> &Self::Store {
        &crate::store::inactive_store::InactiveStore
    }

    fn extra(&self) -> &Self::Extra {
        &()
    }

    fn base_key(&self) -> &BaseKey {
        &self.0
    }

    fn base_key_mut(&mut self) -> &mut BaseKey {
        &mut self.0
    }
}

/// Implementation of the [`Context`] trait on top of a DB client implementing
/// [`crate::store::KeyValueStore`].
#[derive(Debug, Default, Clone)]
pub struct ViewContext<E, S> {
    /// The DB client that is shared between views.
    store: S,
    /// The base key for the context.
    base_key: BaseKey,
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
            base_key: BaseKey { bytes: base_key },
            extra,
        }
    }
}

impl<E, S> Context for ViewContext<E, S>
where
    E: Clone + Send + Sync,
    S: ReadableKeyValueStore + WritableKeyValueStore + Clone,
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

    fn base_key(&self) -> &BaseKey {
        &self.base_key
    }

    fn base_key_mut(&mut self) -> &mut BaseKey {
        &mut self.base_key
    }
}

/// An implementation of [`crate::context::Context`] that stores all values in memory.
pub type MemoryContext<E> = ViewContext<E, MemoryStore>;

impl<E> MemoryContext<E> {
    /// Creates a [`Context`] instance in memory for testing.
    #[cfg(with_testing)]
    pub fn new_for_testing(extra: E) -> Self {
        Self {
            store: MemoryStore::new_for_testing(),
            base_key: BaseKey::default(),
            extra,
        }
    }
}

impl DeletePrefixExpander for MemoryContext<()> {
    type Error = crate::memory::MemoryStoreError;

    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.store().find_keys_by_prefix(key_prefix).await
    }
}
