// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::{
    batch::Batch,
    common::from_bytes_option,
    context::Context,
    store::ReadableKeyValueStore as _,
    views::{
        ClonableView, HashableView, Hasher, HasherOutput, PreFlushView, ReplaceContext, View,
        ViewError, MIN_VIEW_TAG,
    },
};

/// A hash for `ContainerView` and storing of the hash for memoization purposes.
#[derive(Debug)]
pub struct HistoricallyHashableView<C, W> {
    /// The hash in storage.
    stored_hash: Option<HasherOutput>,
    /// The inner view.
    inner: W,
    /// Track context type.
    _phantom: PhantomData<C>,
}

/// Key tags to create the sub-keys of a `MapView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view.
    Inner = MIN_VIEW_TAG,
    /// Prefix for the hash.
    Hash,
}

impl<C, W> HistoricallyHashableView<C, W> {
    fn make_hash(&self, batch: &Batch) -> Result<HasherOutput, ViewError> {
        // TODO: metric
        let stored_hash = self.stored_hash.unwrap_or_default();
        if batch.is_empty() {
            return Ok(stored_hash);
        }
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bytes(&stored_hash)?;
        hasher.update_with_bcs_bytes(&batch)?;
        Ok(hasher.finalize())
    }
}

impl<C, W, C2> ReplaceContext<C2> for HistoricallyHashableView<C, W>
where
    W: View<Context = C> + ReplaceContext<C2>,
    C: Context,
    C2: Context,
{
    type Target = HistoricallyHashableView<C2, <W as ReplaceContext<C2>>::Target>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        HistoricallyHashableView {
            _phantom: PhantomData,
            stored_hash: self.stored_hash,
            inner: self.inner.with_context(ctx).await,
        }
    }
}

impl<W> View for HistoricallyHashableView<W::Context, W>
where
    W: View,
{
    const NUM_INIT_KEYS: usize = 1 + W::NUM_INIT_KEYS;

    type Context = W::Context;

    fn context(&self) -> &Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut v = vec![context.base_key().base_tag(KeyTag::Hash as u8)];
        let base_key = context.base_key().base_tag(KeyTag::Inner as u8);
        let context = context.clone_with_base_key(base_key);
        v.extend(W::pre_load(&context)?);
        Ok(v)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let hash = from_bytes_option(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let base_key = context.base_key().base_tag(KeyTag::Inner as u8);
        let context = context.clone_with_base_key(base_key);
        let inner = W::post_load(
            context,
            values.get(1..).ok_or(ViewError::PostLoadValuesError)?,
        )?;
        Ok(Self {
            _phantom: PhantomData,
            stored_hash: hash,
            inner,
        })
    }

    async fn load(context: Self::Context) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.inner.rollback();
    }

    async fn has_pending_changes(&self) -> bool {
        self.inner.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut inner_batch = Batch::new();
        let delete_view = self.inner.flush(&mut inner_batch)?;
        let hash = self.make_hash(&inner_batch)?;
        batch.operations.extend(inner_batch.operations);
        if delete_view {
            let mut key_prefix = self.inner.context().base_key().bytes.clone();
            key_prefix.pop();
            batch.delete_key_prefix(key_prefix);
            self.stored_hash = None;
        } else if self.stored_hash != Some(hash) {
            let mut key = self.inner.context().base_key().bytes.clone();
            let tag = key.last_mut().unwrap();
            *tag = KeyTag::Hash as u8;
            batch.put_key_value(key, &hash)?;
            self.stored_hash = Some(hash);
        }
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<W> ClonableView for HistoricallyHashableView<W::Context, W>
where
    W: ClonableView,
{
    fn clone_unchecked(&mut self) -> Self {
        HistoricallyHashableView {
            _phantom: PhantomData,
            stored_hash: self.stored_hash,
            inner: self.inner.clone_unchecked(),
        }
    }
}

impl<W: PreFlushView> HashableView for HistoricallyHashableView<W::Context, W> {
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<HasherOutput, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<HasherOutput, ViewError> {
        let mut batch = Batch::new();
        self.inner.pre_flush_unchecked(&mut batch)?;
        self.make_hash(&batch)
    }
}

impl<C, W> Deref for HistoricallyHashableView<C, W> {
    type Target = W;

    fn deref(&self) -> &W {
        &self.inner
    }
}

impl<C, W> DerefMut for HistoricallyHashableView<C, W> {
    fn deref_mut(&mut self) -> &mut W {
        &mut self.inner
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::HistoricallyHashableView;
    use crate::context::Context;

    impl<C, W> async_graphql::OutputType for HistoricallyHashableView<C, W>
    where
        C: Context,
        W: async_graphql::OutputType + Send + Sync,
    {
        fn type_name() -> Cow<'static, str> {
            W::type_name()
        }

        fn qualified_type_name() -> String {
            W::qualified_type_name()
        }

        fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
            W::create_type_info(registry)
        }

        async fn resolve(
            &self,
            ctx: &async_graphql::ContextSelectionSet<'_>,
            field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
        ) -> async_graphql::ServerResult<async_graphql::Value> {
            (**self).resolve(ctx, field).await
        }
    }
}
