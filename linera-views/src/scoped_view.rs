use crate::{
    common::{Batch, Context},
    views::{HashView, Hasher, HashingContext, View, ViewError},
};
use async_trait::async_trait;
use std::fmt::Debug;

/// A view that adds a prefix to all the keys of the contained view.
#[derive(Debug, Clone)]
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

pub trait ScopedOperations: Context {
    /// Clone the context and advance the (otherwise implicit) base key for all read/write
    /// operations.
    fn clone_with_scope(&self, index: u64) -> Self;
}

impl<C: Context> ScopedOperations for C {
    fn clone_with_scope(&self, index: u64) -> Self {
        self.clone_self(self.derive_key(&index).expect("derive_key should not fail"))
    }
}

#[async_trait]
impl<C, W, const INDEX: u64> View<C> for ScopedView<INDEX, W>
where
    C: Context + Send + Sync + ScopedOperations + 'static,
    ViewError: From<C::Error>,
    W: View<C> + Send,
{
    fn context(&self) -> &C {
        self.view.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let view = W::load(context.clone_with_scope(INDEX)).await?;
        Ok(Self { view })
    }

    fn rollback(&mut self) {
        self.view.rollback();
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.view.flush(batch)
    }

    fn delete(self, batch: &mut Batch) -> Result<(), ViewError> {
        self.view.delete(batch)
    }

    fn clear(&mut self) {
        self.view.clear();
    }
}

#[async_trait]
impl<C, W, const INDEX: u64> HashView<C> for ScopedView<INDEX, W>
where
    C: HashingContext + Send + Sync + ScopedOperations + 'static,
    ViewError: From<C::Error>,
    W: HashView<C> + Send,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        self.view.hash().await
    }
}
