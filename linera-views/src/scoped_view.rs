// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::{Batch, Context},
    views::{HashView, Hasher, View, ViewError},
};
use async_trait::async_trait;
use std::fmt::Debug;

/// A view that adds a prefix to all the keys of the contained view.
#[derive(Debug)]
pub struct ScopedView<const INDEX: u64, W> {
    pub(crate) view: W,
}

impl<W, const INDEX: u64> std::ops::Deref for ScopedView<INDEX, W> {
    type Target = W;

    fn deref(&self) -> &W {
        &self.view
    }
}

impl<W, const INDEX: u64> std::ops::DerefMut for ScopedView<INDEX, W> {
    fn deref_mut(&mut self) -> &mut W {
        &mut self.view
    }
}

#[async_trait]
impl<C, W, const INDEX: u64> View<C> for ScopedView<INDEX, W>
where
    C: Context + Send + Sync + 'static,
    ViewError: From<C::Error>,
    W: View<C> + Send,
{
    fn context(&self) -> &C {
        self.view.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let scoped_context = context.clone_with_base_key(context.derive_key(&INDEX)?);
        let view = W::load(scoped_context).await?;
        Ok(Self { view })
    }

    fn rollback(&mut self) {
        self.view.rollback();
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.view.flush(batch)
    }

    fn delete(self, batch: &mut Batch) {
        self.view.delete(batch)
    }

    fn clear(&mut self) {
        self.view.clear();
    }
}

#[async_trait]
impl<C, W, const INDEX: u64> HashView<C> for ScopedView<INDEX, W>
where
    C: Context + Send + Sync + 'static,
    ViewError: From<C::Error>,
    W: HashView<C> + Send,
{
    type Hasher = W::Hasher;

    async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.view.hash().await
    }
}
