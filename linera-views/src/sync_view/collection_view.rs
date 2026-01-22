// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::{Deref, DerefMut};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    common::CustomSerialize,
    context::Context,
    sync_view::{block_on, SyncClonableView, SyncHashableView, SyncReplaceContext, SyncView},
    views::{ClonableView as _, HashableView as _, ReplaceContext as _, View as _},
    ViewError,
};

pub type ReadGuardedView<'a, W> = crate::views::collection_view::ReadGuardedView<'a, W>;

/// A synchronous collection view with byte keys.
#[derive(Debug)]
pub struct ByteCollectionView<C, W> {
    inner: crate::views::collection_view::ByteCollectionView<C, W>,
}

impl<C, W> ByteCollectionView<C, W> {
    /// Loads a mutable entry for a key, inserting a default view if missing.
    pub fn load_entry_mut(&mut self, short_key: &[u8]) -> Result<&mut W, ViewError>
    where
        C: Context,
        W: SyncView + Default + Clone,
        W::Context: Context,
    {
        block_on(self.inner.load_entry_mut(short_key))
    }

    /// Tries to load an entry for a key.
    pub fn try_load_entry(
        &self,
        short_key: &[u8],
    ) -> Result<Option<ReadGuardedView<'_, W>>, ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entry(short_key))
    }

    /// Tries to load multiple entries.
    pub fn try_load_entries<'a>(
        &'a self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<ReadGuardedView<'a, W>>>, ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entries(short_keys))
    }

    /// Tries to load multiple entries and keys.
    pub fn try_load_entries_pairs(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Option<ReadGuardedView<'_, W>>)>, ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entries_pairs(short_keys))
    }

    /// Tries to load all entries.
    pub fn try_load_all_entries(
        &self,
    ) -> Result<Vec<(Vec<u8>, ReadGuardedView<'_, W>)>, ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_all_entries())
    }

    /// Checks if a key is present.
    pub fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.contains_key(short_key))
    }

    /// Iterates over keys while `f` returns `true`.
    pub fn for_each_key_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_key_while(f))
    }

    /// Iterates over keys.
    pub fn for_each_key<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_key(f))
    }

    /// Reads all keys.
    pub fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.keys())
    }

    /// Returns the number of keys.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.count())
    }
}

impl<C, W> SyncView for ByteCollectionView<C, W>
where
    C: Context,
    W: SyncView + Clone,
    W::Context: Context,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::collection_view::ByteCollectionView<C, W> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::collection_view::ByteCollectionView::<C, W>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner =
            crate::views::collection_view::ByteCollectionView::<C, W>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner =
            block_on(crate::views::collection_view::ByteCollectionView::<C, W>::load(context))?;
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

impl<C, W, C2> SyncReplaceContext<C2> for ByteCollectionView<C, W>
where
    C: Context,
    C2: Context,
    W: SyncView + Clone,
    W::Context: Context,
{
    type Target = ByteCollectionView<C2, W>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        ByteCollectionView { inner }
    }
}

impl<C, W> SyncClonableView for ByteCollectionView<C, W>
where
    C: Context,
    W: SyncView + Clone,
    W::Context: Context,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, W> SyncHashableView for ByteCollectionView<C, W>
where
    C: Context,
    W: SyncView + Clone,
    W::Context: Context,
{
    type Hasher =
        <crate::views::collection_view::ByteCollectionView<C, W> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, W> Deref for ByteCollectionView<C, W> {
    type Target = crate::views::collection_view::ByteCollectionView<C, W>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, W> DerefMut for ByteCollectionView<C, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A synchronous collection view with BCS-serialized indices.
#[derive(Debug)]
pub struct CollectionView<C, I, W> {
    inner: crate::views::collection_view::CollectionView<C, I, W>,
}

impl<C, I, W> CollectionView<C, I, W> {
    /// Loads a mutable entry for a key, inserting a default view if missing.
    pub fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        Q: Serialize + ?Sized,
        W: SyncView + Default + Clone,
        W::Context: Context,
    {
        block_on(self.inner.load_entry_mut(index))
    }

    /// Tries to load an entry for a key.
    pub fn try_load_entry<Q>(
        &self,
        index: &Q,
    ) -> Result<Option<ReadGuardedView<'_, W>>, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        Q: Serialize + ?Sized,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entry(index))
    }

    /// Tries to load multiple entries.
    pub fn try_load_entries<'a, Q>(
        &'a self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<ReadGuardedView<'a, W>>>, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        Q: Serialize + Clone + 'a,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entries(indices))
    }

    /// Tries to load multiple entries and keys.
    pub fn try_load_entries_pairs<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<(Q, Option<ReadGuardedView<'_, W>>)>, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        Q: Serialize + Clone,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entries_pairs(indices))
    }

    /// Tries to load all entries.
    pub fn try_load_all_entries(
        &self,
    ) -> Result<Vec<(I, ReadGuardedView<'_, W>)>, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_all_entries())
    }

    /// Reads all indices.
    pub fn indices(&self) -> Result<Vec<I>, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.indices())
    }

    /// Returns the number of indices.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.count())
    }

    /// Iterates over indices while `f` returns `true`.
    pub fn for_each_index_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        W: SyncView + Clone,
        W::Context: Context,
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_while(f))
    }

    /// Iterates over indices.
    pub fn for_each_index<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        W: SyncView + Clone,
        W::Context: Context,
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index(f))
    }
}

impl<C, I, W> SyncView for CollectionView<C, I, W>
where
    C: Context,
    I: Serialize + DeserializeOwned,
    W: SyncView + Clone,
    W::Context: Context,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::collection_view::CollectionView<C, I, W> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::collection_view::CollectionView::<C, I, W>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner =
            crate::views::collection_view::CollectionView::<C, I, W>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner =
            block_on(crate::views::collection_view::CollectionView::<C, I, W>::load(context))?;
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

impl<C, I, W, C2> SyncReplaceContext<C2> for CollectionView<C, I, W>
where
    C: Context,
    C2: Context,
    I: Serialize + DeserializeOwned,
    W: SyncView + Clone,
    W::Context: Context,
{
    type Target = CollectionView<C2, I, W>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        CollectionView { inner }
    }
}

impl<C, I, W> SyncClonableView for CollectionView<C, I, W>
where
    C: Context,
    I: Serialize + DeserializeOwned,
    W: SyncView + Clone,
    W::Context: Context,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, I, W> SyncHashableView for CollectionView<C, I, W>
where
    C: Context,
    I: Serialize + DeserializeOwned,
    W: SyncView + Clone,
    W::Context: Context,
{
    type Hasher =
        <crate::views::collection_view::CollectionView<C, I, W> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, I, W> Deref for CollectionView<C, I, W> {
    type Target = crate::views::collection_view::CollectionView<C, I, W>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, I, W> DerefMut for CollectionView<C, I, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A synchronous collection view with custom serialization.
#[derive(Debug)]
pub struct CustomCollectionView<C, I, W> {
    inner: crate::views::collection_view::CustomCollectionView<C, I, W>,
}

impl<C, I, W> CustomCollectionView<C, I, W> {
    /// Loads a mutable entry for a key, inserting a default view if missing.
    pub fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        Q: CustomSerialize + ?Sized,
        W: SyncView + Default + Clone,
        W::Context: Context,
    {
        block_on(self.inner.load_entry_mut(index))
    }

    /// Tries to load an entry for a key.
    pub fn try_load_entry<Q>(
        &self,
        index: &Q,
    ) -> Result<Option<ReadGuardedView<'_, W>>, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        Q: CustomSerialize + ?Sized,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entry(index))
    }

    /// Tries to load multiple entries.
    pub fn try_load_entries<'a, Q>(
        &'a self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<ReadGuardedView<'a, W>>>, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        Q: CustomSerialize + Clone + 'a,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entries(indices))
    }

    /// Tries to load multiple entries and keys.
    pub fn try_load_entries_pairs<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<(Q, Option<ReadGuardedView<'_, W>>)>, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        Q: CustomSerialize + Clone,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_entries_pairs(indices))
    }

    /// Tries to load all entries.
    pub fn try_load_all_entries(
        &self,
    ) -> Result<Vec<(I, ReadGuardedView<'_, W>)>, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.try_load_all_entries())
    }

    /// Reads all indices.
    pub fn indices(&self) -> Result<Vec<I>, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.indices())
    }

    /// Returns the number of indices.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        W: SyncView + Clone,
        W::Context: Context,
    {
        block_on(self.inner.count())
    }

    /// Iterates over indices while `f` returns `true`.
    pub fn for_each_index_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        W: SyncView + Clone,
        W::Context: Context,
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_while(f))
    }

    /// Iterates over indices.
    pub fn for_each_index<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        W: SyncView + Clone,
        W::Context: Context,
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index(f))
    }
}

impl<C, I, W> SyncView for CustomCollectionView<C, I, W>
where
    C: Context,
    I: CustomSerialize + DeserializeOwned + Send + Sync,
    W: SyncView + Clone,
    W::Context: Context,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::collection_view::CustomCollectionView<C, I, W> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::collection_view::CustomCollectionView::<C, I, W>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner =
            crate::views::collection_view::CustomCollectionView::<C, I, W>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner =
            block_on(crate::views::collection_view::CustomCollectionView::<C, I, W>::load(context))?;
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

impl<C, I, W, C2> SyncReplaceContext<C2> for CustomCollectionView<C, I, W>
where
    C: Context,
    C2: Context,
    I: CustomSerialize + DeserializeOwned + Send + Sync,
    W: SyncView + Clone,
    W::Context: Context,
{
    type Target = CustomCollectionView<C2, I, W>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        CustomCollectionView { inner }
    }
}

impl<C, I, W> SyncClonableView for CustomCollectionView<C, I, W>
where
    C: Context,
    I: CustomSerialize + DeserializeOwned + Send + Sync,
    W: SyncView + Clone,
    W::Context: Context,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, I, W> SyncHashableView for CustomCollectionView<C, I, W>
where
    C: Context,
    I: CustomSerialize + DeserializeOwned + Send + Sync,
    W: SyncView + Clone,
    W::Context: Context,
{
    type Hasher =
        <crate::views::collection_view::CustomCollectionView<C, I, W> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, I, W> Deref for CustomCollectionView<C, I, W> {
    type Target = crate::views::collection_view::CustomCollectionView<C, I, W>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, I, W> DerefMut for CustomCollectionView<C, I, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A view for collections with a memoized hash.
pub type HashedCollectionView<C, I, W> = crate::sync_view::hashable_wrapper::WrappedHashableContainerView<
    C,
    CollectionView<C, I, W>,
    crate::common::HasherOutput,
>;

/// A view for byte collections with a memoized hash.
pub type HashedByteCollectionView<C, W> = crate::sync_view::hashable_wrapper::WrappedHashableContainerView<
    C,
    ByteCollectionView<C, W>,
    crate::common::HasherOutput,
>;

/// A view for collections with custom serialization and a memoized hash.
pub type HashedCustomCollectionView<C, I, W> = crate::sync_view::hashable_wrapper::WrappedHashableContainerView<
    C,
    CustomCollectionView<C, I, W>,
    crate::common::HasherOutput,
>;
