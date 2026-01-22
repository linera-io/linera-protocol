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

/// A synchronous view that supports inserting and removing values indexed by `Vec<u8>`.
#[derive(Debug)]
pub struct ByteSetView<C> {
    inner: crate::views::set_view::ByteSetView<C>,
}

impl<C> ByteSetView<C> {
    /// Inserts a value.
    pub fn insert(&mut self, short_key: Vec<u8>) {
        self.inner.insert(short_key);
    }

    /// Removes a value.
    pub fn remove(&mut self, short_key: Vec<u8>) {
        self.inner.remove(short_key);
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra
    where
        C: Context,
    {
        self.inner.extra()
    }

    /// Checks if a key is present.
    pub fn contains(&self, short_key: &[u8]) -> Result<bool, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.contains(short_key))
    }

    /// Reads all keys.
    pub fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.keys())
    }

    /// Returns the number of keys.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
    {
        block_on(self.inner.count())
    }

    /// Iterates over keys while `f` returns `true`.
    pub fn for_each_key_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_key_while(f))
    }

    /// Iterates over keys.
    pub fn for_each_key<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_key(f))
    }
}

impl<C> SyncView for ByteSetView<C>
where
    C: Context,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::set_view::ByteSetView<C> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::set_view::ByteSetView::<C>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::set_view::ByteSetView::<C>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::set_view::ByteSetView::<C>::load(context))?;
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

impl<C, C2> SyncReplaceContext<C2> for ByteSetView<C>
where
    C: Context,
    C2: Context,
{
    type Target = ByteSetView<C2>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        ByteSetView { inner }
    }
}

impl<C> SyncClonableView for ByteSetView<C>
where
    C: Context,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C> SyncHashableView for ByteSetView<C>
where
    C: Context,
{
    type Hasher = <crate::views::set_view::ByteSetView<C> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C> Deref for ByteSetView<C> {
    type Target = crate::views::set_view::ByteSetView<C>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C> DerefMut for ByteSetView<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A synchronous set view with BCS-serialized indices.
#[derive(Debug)]
pub struct SetView<C, I> {
    inner: crate::views::set_view::SetView<C, I>,
}

impl<C, I> SetView<C, I> {
    /// Inserts a value.
    pub fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Serialize,
        Q: Serialize + ?Sized,
    {
        self.inner.insert(index)
    }

    /// Removes a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Serialize,
        Q: Serialize + ?Sized,
    {
        self.inner.remove(index)
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra
    where
        C: Context,
    {
        self.inner.extra()
    }

    /// Checks if a key is present.
    pub fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        C: Context,
        I: Serialize,
        Q: Serialize + ?Sized,
    {
        block_on(self.inner.contains(index))
    }

    /// Reads all indices.
    pub fn indices(&self) -> Result<Vec<I>, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
    {
        block_on(self.inner.indices())
    }

    /// Returns the number of indices.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
    {
        block_on(self.inner.count())
    }

    /// Iterates over indices while `f` returns `true`.
    pub fn for_each_index_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_while(f))
    }

    /// Iterates over indices.
    pub fn for_each_index<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        I: Serialize + DeserializeOwned,
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index(f))
    }
}

impl<C, I> SyncView for SetView<C, I>
where
    C: Context,
    I: Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::set_view::SetView<C, I> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::set_view::SetView::<C, I>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::set_view::SetView::<C, I>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::set_view::SetView::<C, I>::load(context))?;
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

impl<C, I, C2> SyncReplaceContext<C2> for SetView<C, I>
where
    C: Context,
    C2: Context,
    I: Serialize + DeserializeOwned,
{
    type Target = SetView<C2, I>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        SetView { inner }
    }
}

impl<C, I> SyncClonableView for SetView<C, I>
where
    C: Context,
    I: Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, I> SyncHashableView for SetView<C, I>
where
    C: Context,
    I: Serialize + DeserializeOwned,
{
    type Hasher = <crate::views::set_view::SetView<C, I> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, I> Deref for SetView<C, I> {
    type Target = crate::views::set_view::SetView<C, I>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, I> DerefMut for SetView<C, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A synchronous set view with custom serialization.
#[derive(Debug)]
pub struct CustomSetView<C, I> {
    inner: crate::views::set_view::CustomSetView<C, I>,
}

impl<C, I> CustomSetView<C, I> {
    /// Inserts a value.
    pub fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
    {
        self.inner.insert(index)
    }

    /// Removes a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
    {
        self.inner.remove(index)
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra
    where
        C: Context,
    {
        self.inner.extra()
    }

    /// Checks if a key is present.
    pub fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        C: Context,
        I: CustomSerialize + Send + Sync,
        Q: CustomSerialize + ?Sized,
    {
        block_on(self.inner.contains(index))
    }

    /// Reads all indices.
    pub fn indices(&self) -> Result<Vec<I>, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
    {
        block_on(self.inner.indices())
    }

    /// Returns the number of indices.
    pub fn count(&self) -> Result<usize, ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
    {
        block_on(self.inner.count())
    }

    /// Iterates over indices while `f` returns `true`.
    pub fn for_each_index_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        block_on(self.inner.for_each_index_while(f))
    }

    /// Iterates over indices.
    pub fn for_each_index<F>(&self, f: F) -> Result<(), ViewError>
    where
        C: Context,
        I: CustomSerialize + DeserializeOwned + Send + Sync,
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        block_on(self.inner.for_each_index(f))
    }
}

impl<C, I> SyncView for CustomSetView<C, I>
where
    C: Context,
    I: CustomSerialize + DeserializeOwned + Send + Sync,
{
    const NUM_INIT_KEYS: usize =
        <crate::views::set_view::CustomSetView<C, I> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::set_view::CustomSetView::<C, I>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::set_view::CustomSetView::<C, I>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::set_view::CustomSetView::<C, I>::load(context))?;
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

impl<C, I, C2> SyncReplaceContext<C2> for CustomSetView<C, I>
where
    C: Context,
    C2: Context,
    I: CustomSerialize + DeserializeOwned + Send + Sync,
{
    type Target = CustomSetView<C2, I>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        CustomSetView { inner }
    }
}

impl<C, I> SyncClonableView for CustomSetView<C, I>
where
    C: Context,
    I: CustomSerialize + DeserializeOwned + Send + Sync,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, I> SyncHashableView for CustomSetView<C, I>
where
    C: Context,
    I: CustomSerialize + DeserializeOwned + Send + Sync,
{
    type Hasher = <crate::views::set_view::CustomSetView<C, I> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, I> Deref for CustomSetView<C, I> {
    type Target = crate::views::set_view::CustomSetView<C, I>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, I> DerefMut for CustomSetView<C, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A view for sets with a memoized hash.
pub type HashedSetView<C, I> =
    crate::sync_view::hashable_wrapper::WrappedHashableContainerView<C, SetView<C, I>, crate::common::HasherOutput>;

/// A view for byte sets with a memoized hash.
pub type HashedByteSetView<C> =
    crate::sync_view::hashable_wrapper::WrappedHashableContainerView<C, ByteSetView<C>, crate::common::HasherOutput>;

/// A view for sets with custom serialization and a memoized hash.
pub type HashedCustomSetView<C, I> = crate::sync_view::hashable_wrapper::WrappedHashableContainerView<
    C,
    CustomSetView<C, I>,
    crate::common::HasherOutput,
>;
