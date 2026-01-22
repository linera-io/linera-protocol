// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::{Deref, DerefMut};

use crate::{
    context::Context,
    sync_view::{block_on, SyncClonableView, SyncReplaceContext, SyncView},
    views::{ClonableView as _, ReplaceContext as _, View as _},
    ViewError,
};

/// A synchronous view that represents a key-value store.
#[derive(Debug)]
pub struct KeyValueStoreView<C> {
    inner: crate::views::key_value_store_view::KeyValueStoreView<C>,
}

impl<C> KeyValueStoreView<C> {
    /// Iterates over indices while `f` returns `true`.
    pub fn for_each_index_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_while(f))
    }

    /// Iterates over indices.
    pub fn for_each_index<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index(f))
    }

    /// Iterates over index/value pairs while `f` returns `true`.
    pub fn for_each_index_value_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_value_while(f))
    }

    /// Iterates over index/value pairs.
    pub fn for_each_index_value<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index_value(f))
    }

    /// Reads all indices.
    pub fn indices(&self) -> Result<Vec<Vec<u8>>, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.indices())
    }

    /// Reads all index/value pairs.
    pub fn index_values(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.index_values())
    }

    /// Returns the number of keys.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.count())
    }

    /// Reads a value.
    pub fn get(&self, index: &[u8]) -> Result<Option<Vec<u8>>, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.get(index))
    }

    /// Checks if a key is present.
    pub fn contains_key(&self, index: &[u8]) -> Result<bool, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.contains_key(index))
    }

    /// Checks if multiple keys are present.
    pub fn contains_keys(&self, indices: &[Vec<u8>]) -> Result<Vec<bool>, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.contains_keys(indices))
    }

    /// Reads multiple values.
    pub fn multi_get(&self, indices: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.multi_get(indices))
    }

    /// Writes a batch.
    pub fn write_batch(&mut self, batch: crate::batch::Batch) -> Result<(), ViewError>
    where
        C: Context,
    {
        block_on(self.inner.write_batch(batch))
    }

    /// Inserts or updates a value.
    pub fn insert(&mut self, index: Vec<u8>, value: Vec<u8>) -> Result<(), ViewError>
    where
        C: Context,
    {
        block_on(self.inner.insert(index, value))
    }

    /// Removes a value.
    pub fn remove(&mut self, index: Vec<u8>) -> Result<(), ViewError>
    where
        C: Context,
    {
        block_on(self.inner.remove(index))
    }

    /// Removes all values with a common prefix.
    pub fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) -> Result<(), ViewError>
    where
        C: Context,
    {
        block_on(self.inner.remove_by_prefix(key_prefix))
    }

    /// Finds keys by prefix.
    pub fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.find_keys_by_prefix(key_prefix))
    }

    /// Finds key/value pairs by prefix.
    pub fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.find_key_values_by_prefix(key_prefix))
    }

    /// Builds a trivial view that is already deleted.
    pub fn new(context: C) -> Result<Self, ViewError>
    where
        C: Context,
    {
        let inner = block_on(crate::views::key_value_store_view::KeyValueStoreView::new(context))?;
        Ok(Self { inner })
    }
}

impl<C> SyncView for KeyValueStoreView<C>
where
    C: Context,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::key_value_store_view::KeyValueStoreView<C> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::key_value_store_view::KeyValueStoreView::<C>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner =
            crate::views::key_value_store_view::KeyValueStoreView::<C>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner =
            block_on(crate::views::key_value_store_view::KeyValueStoreView::<C>::load(context))?;
        Ok(Self { inner })
    }

    fn rollback(&mut self) {
        self.inner.rollback();
    }

    fn has_pending_changes(&self) -> bool {
        block_on(self.inner.has_pending_changes())
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn pre_save(&self, batch: &mut crate::batch::Batch) -> Result<bool, ViewError> {
        self.inner.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.inner.post_save();
    }
}

impl<C, C2> SyncReplaceContext<C2> for KeyValueStoreView<C>
where
    C: Context,
    C2: Context,
{
    type Target = KeyValueStoreView<C2>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        KeyValueStoreView { inner }
    }
}

impl<C> SyncClonableView for KeyValueStoreView<C>
where
    C: Context,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C> Deref for KeyValueStoreView<C> {
    type Target = crate::views::key_value_store_view::KeyValueStoreView<C>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C> DerefMut for KeyValueStoreView<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A view container that exposes a key-value store interface.
pub type ViewContainer<C> = crate::views::key_value_store_view::ViewContainer<C>;
