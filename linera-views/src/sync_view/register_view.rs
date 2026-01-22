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

/// A synchronous view that supports modifying a single value of type `T`.
#[derive(Debug)]
pub struct RegisterView<C, T> {
    inner: crate::views::register_view::RegisterView<C, T>,
}

impl<C, T> RegisterView<C, T> {
    /// Access the current value in the register.
    pub fn get(&self) -> &T {
        self.inner.get()
    }

    /// Sets the value in the register.
    pub fn set(&mut self, value: T) {
        self.inner.set(value);
    }

    /// Access the current value in the register mutably.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra
    where
        C: Context,
    {
        self.inner.extra()
    }

    /// Returns the wrapped async view.
    pub fn into_inner(self) -> crate::views::register_view::RegisterView<C, T> {
        self.inner
    }
}

impl<C, T> SyncView for RegisterView<C, T>
where
    C: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = <crate::views::register_view::RegisterView<C, T> as crate::views::View>::NUM_INIT_KEYS;

    type Context = C;

    fn context(&self) -> Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        crate::views::register_view::RegisterView::<C, T>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let inner = crate::views::register_view::RegisterView::<C, T>::post_load(context, values)?;
        Ok(Self { inner })
    }

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let inner = block_on(crate::views::register_view::RegisterView::<C, T>::load(context))?;
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

impl<C, T, C2> SyncReplaceContext<C2> for RegisterView<C, T>
where
    C: Context,
    C2: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned + Clone,
{
    type Target = RegisterView<C2, T>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        let inner = block_on(self.inner.with_context(ctx));
        RegisterView { inner }
    }
}

impl<C, T> SyncClonableView for RegisterView<C, T>
where
    C: Context,
    T: Clone + Default + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let inner = self.inner.clone_unchecked()?;
        Ok(Self { inner })
    }
}

impl<C, T> SyncHashableView for RegisterView<C, T>
where
    C: Context,
    T: Default + Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = <crate::views::register_view::RegisterView<C, T> as crate::views::HashableView>::Hasher;

    fn hash(&self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash())
    }

    fn hash_mut(&mut self) -> Result<<Self::Hasher as crate::sync_view::Hasher>::Output, ViewError> {
        block_on(self.inner.hash_mut())
    }
}

impl<C, T> Deref for RegisterView<C, T> {
    type Target = crate::views::register_view::RegisterView<C, T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<C, T> DerefMut for RegisterView<C, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A view for registers with a memoized hash.
pub type HashedRegisterView<C, T> = crate::sync_view::hashable_wrapper::WrappedHashableContainerView<
    C,
    RegisterView<C, T>,
    crate::common::HasherOutput,
>;
