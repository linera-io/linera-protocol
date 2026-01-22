// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::{Deref, DerefMut};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    context::Context,
    sync_view::{block_on, SyncClonableView, SyncHashableView, SyncReplaceContext, SyncView},
    views::{ClonableView as _, HashableView as _, ReplaceContext as _, View as _},
    ViewError,
};

/// A synchronous queue view.
#[derive(Debug)]
pub struct QueueView<C, T> {
    inner: crate::views::queue_view::QueueView<C, T>,
}

impl<C, T> QueueView<C, T> {
    /// Deletes the front value, if any.
    pub fn delete_front(&mut self) {
        self.inner.delete_front();
    }

    /// Pushes a value to the end of the queue.
    pub fn push_back(&mut self, value: T) {
        self.inner.push_back(value);
    }

    /// Reads the size of the queue.
    pub fn count(&self) -> usize {
        self.inner.count()
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra
    where
        C: Context,
    {
        self.inner.extra()
    }

    /// Reads the front value, if any.
    pub fn front(&self) -> Result<Option<T>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.front())
    }

    /// Reads the back value, if any.
    pub fn back(&self) -> Result<Option<T>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.back())
    }

    /// Reads the `count` next values in the queue (including staged ones).
    pub fn read_front(&self, count: usize) -> Result<Vec<T>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.read_front(count))
    }

    /// Reads the `count` last values in the queue (including staged ones).
    pub fn read_back(&self, count: usize) -> Result<Vec<T>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.read_back(count))
    }

    /// Reads all values in the queue.
    pub fn elements(&self) -> Result<Vec<T>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.elements())
    }

    /// Returns a mutable iterator over the queue values.
    pub fn iter_mut<'a>(&'a mut self) -> Result<crate::views::queue_view::IterMut<'a, T>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.iter_mut())
    }
}

impl<C, T> SyncView for QueueView<C, T>
where
    C: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = <crate::views::queue_view::QueueView<C, T> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::queue_view::QueueView::<C, T>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::queue_view::QueueView::<C, T>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::queue_view::QueueView::<C, T>::load(context))?;
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

impl<C, T, C2> SyncReplaceContext<C2> for QueueView<C, T>
where
    C: Context,
    C2: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Target = QueueView<C2, T>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        QueueView { inner }
    }
}

impl<C, T> SyncClonableView for QueueView<C, T>
where
    C: Context,
    T: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, T> SyncHashableView for QueueView<C, T>
where
    C: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Hasher = <crate::views::queue_view::QueueView<C, T> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, T> Deref for QueueView<C, T> {
    type Target = crate::views::queue_view::QueueView<C, T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, T> DerefMut for QueueView<C, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A view for queues with a memoized hash.
pub type HashedQueueView<C, T> =
    crate::sync_view::hashable_wrapper::WrappedHashableContainerView<C, QueueView<C, T>, crate::common::HasherOutput>;

