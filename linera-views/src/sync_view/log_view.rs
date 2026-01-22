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

/// A synchronous log view.
#[derive(Debug)]
pub struct LogView<C, T> {
    inner: crate::views::log_view::LogView<C, T>,
}

impl<C, T> LogView<C, T> {
    /// Pushes a value to the end of the log.
    pub fn push(&mut self, value: T) {
        self.inner.push(value);
    }

    /// Reads the size of the log.
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

    /// Reads the entry at `index`.
    pub fn get(&self, index: usize) -> Result<Option<T>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.get(index))
    }

    /// Reads multiple entries from the log.
    pub fn multi_get(&self, indices: Vec<usize>) -> Result<Vec<Option<T>>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.multi_get(indices))
    }

    /// Reads multiple entries and indices.
    pub fn multi_get_pairs(
        &self,
        indices: Vec<usize>,
    ) -> Result<Vec<(usize, Option<T>)>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
    {
        block_on(self.inner.multi_get_pairs(indices))
    }

    /// Reads a range of entries.
    pub fn read<R>(&self, range: R) -> Result<Vec<T>, ViewError>
    where
        C: Context,
        T: Send + Sync + Clone + Serialize + DeserializeOwned,
        R: std::ops::RangeBounds<usize> + Clone,
    {
        block_on(self.inner.read(range))
    }
}

impl<C, T> SyncView for LogView<C, T>
where
    C: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = <crate::views::log_view::LogView<C, T> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::log_view::LogView::<C, T>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::log_view::LogView::<C, T>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::log_view::LogView::<C, T>::load(context))?;
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

impl<C, T, C2> SyncReplaceContext<C2> for LogView<C, T>
where
    C: Context,
    C2: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Target = LogView<C2, T>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        LogView { inner }
    }
}

impl<C, T> SyncClonableView for LogView<C, T>
where
    C: Context,
    T: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, T> SyncHashableView for LogView<C, T>
where
    C: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Hasher = <crate::views::log_view::LogView<C, T> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, T> Deref for LogView<C, T> {
    type Target = crate::views::log_view::LogView<C, T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, T> DerefMut for LogView<C, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A view for logs with a memoized hash.
pub type HashedLogView<C, T> =
    crate::sync_view::hashable_wrapper::WrappedHashableContainerView<C, LogView<C, T>, crate::common::HasherOutput>;

