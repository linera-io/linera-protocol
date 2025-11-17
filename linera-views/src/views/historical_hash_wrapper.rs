// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Mutex,
};

use allocative::Allocative;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::visit_allocative_simple;

use crate::{
    batch::Batch,
    common::from_bytes_option,
    context::Context,
    store::ReadableKeyValueStore as _,
    views::{ClonableView, Hasher, HasherOutput, ReplaceContext, View, ViewError, MIN_VIEW_TAG},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The runtime of hash computation
    pub static HISTORICALLY_HASHABLE_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "historically_hashable_view_hash_runtime",
                "HistoricallyHashableView hash runtime",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });
}

/// Wrapper to compute the hash of the view based on its history of modifications.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, W: Allocative")]
pub struct HistoricallyHashableView<C, W> {
    /// The hash in storage.
    #[allocative(visit = visit_allocative_simple)]
    stored_hash: Option<HasherOutput>,
    /// The inner view.
    inner: W,
    /// Memoized hash, if any.
    #[allocative(visit = visit_allocative_simple)]
    hash: Mutex<Option<HasherOutput>>,
    /// Track context type.
    #[allocative(skip)]
    _phantom: PhantomData<C>,
}

/// Key tags to create the sub-keys of a `HistoricallyHashableView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view.
    Inner = MIN_VIEW_TAG,
    /// Prefix for the hash.
    Hash,
}

impl<C, W> HistoricallyHashableView<C, W> {
    fn make_hash(
        stored_hash: Option<HasherOutput>,
        batch: &Batch,
    ) -> Result<HasherOutput, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::HISTORICALLY_HASHABLE_VIEW_HASH_RUNTIME.measure_latency();
        let stored_hash = stored_hash.unwrap_or_default();
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
            hash: Mutex::new(*self.hash.get_mut().unwrap()),
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

    fn context(&self) -> Self::Context {
        // The inner context has our base key plus the KeyTag::Inner byte
        self.inner.context().clone_with_trimmed_key(1)
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
            hash: Mutex::new(hash),
            inner,
        })
    }

    async fn load(context: Self::Context) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(&keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.inner.rollback();
        *self.hash.get_mut().unwrap() = self.stored_hash;
    }

    async fn has_pending_changes(&self) -> bool {
        self.inner.has_pending_changes().await
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut inner_batch = Batch::new();
        self.inner.pre_save(&mut inner_batch)?;
        let new_hash = {
            let mut maybe_hash = self.hash.lock().unwrap();
            match maybe_hash.as_mut() {
                Some(hash) => *hash,
                None => {
                    let hash = Self::make_hash(self.stored_hash, &inner_batch)?;
                    *maybe_hash = Some(hash);
                    hash
                }
            }
        };
        batch.operations.extend(inner_batch.operations);

        if self.stored_hash != Some(new_hash) {
            let mut key = self.inner.context().base_key().bytes.clone();
            let tag = key.last_mut().unwrap();
            *tag = KeyTag::Hash as u8;
            batch.put_key_value(key, &new_hash)?;
        }
        // Never delete the stored hash, even if the inner view was cleared.
        Ok(false)
    }

    fn post_save(&mut self) {
        let new_hash = self
            .hash
            .get_mut()
            .unwrap()
            .expect("hash should be computed in pre_save");
        self.stored_hash = Some(new_hash);
        self.inner.post_save();
    }

    fn clear(&mut self) {
        self.inner.clear();
        *self.hash.get_mut().unwrap() = None;
    }
}

impl<W> ClonableView for HistoricallyHashableView<W::Context, W>
where
    W: ClonableView,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(HistoricallyHashableView {
            _phantom: PhantomData,
            stored_hash: self.stored_hash,
            hash: Mutex::new(*self.hash.get_mut().unwrap()),
            inner: self.inner.clone_unchecked()?,
        })
    }
}

impl<W: View> HistoricallyHashableView<W::Context, W> {
    /// Obtains a hash of the history of the changes in the view.
    pub async fn historical_hash(&mut self) -> Result<HasherOutput, ViewError> {
        if let Some(hash) = self.hash.get_mut().unwrap() {
            return Ok(*hash);
        }
        let mut batch = Batch::new();
        self.inner.pre_save(&mut batch)?;
        let hash = Self::make_hash(self.stored_hash, &batch)?;
        // Remember the hash that we just computed.
        *self.hash.get_mut().unwrap() = Some(hash);
        Ok(hash)
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
        // Clear the memoized hash.
        *self.hash.get_mut().unwrap() = None;
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
            self.inner.resolve(ctx, field).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        context::MemoryContext, register_view::RegisterView, store::WritableKeyValueStore as _,
    };

    #[tokio::test]
    async fn test_historically_hashable_view_initial_state() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Initially should have no pending changes
        assert!(!view.has_pending_changes().await);

        // Initial hash should be the hash of an empty batch with default stored_hash
        let hash = view.historical_hash().await?;
        assert_eq!(hash, HasherOutput::default());

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_hash_changes_with_modifications(
    ) -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Get initial hash
        let hash0 = view.historical_hash().await?;

        // Set a value
        view.set(42);
        assert!(view.has_pending_changes().await);

        // Hash should change after modification
        let hash1 = view.historical_hash().await?;

        // Calling `historical_hash` doesn't flush changes.
        assert!(view.has_pending_changes().await);
        assert_ne!(hash0, hash1);

        // Flush and verify hash is stored
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();
        assert!(!view.has_pending_changes().await);
        assert_eq!(hash1, view.historical_hash().await?);

        // Make another modification
        view.set(84);
        let hash2 = view.historical_hash().await?;
        assert_ne!(hash1, hash2);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_reloaded() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set initial value and flush
        view.set(42);
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();

        let hash_after_flush = view.historical_hash().await?;

        // Reload the view
        let mut view2 =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Hash should be the same (loaded from storage)
        let hash_reloaded = view2.historical_hash().await?;
        assert_eq!(hash_after_flush, hash_reloaded);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_rollback() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set and persist a value
        view.set(42);
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();

        let hash_before = view.historical_hash().await?;
        assert!(!view.has_pending_changes().await);

        // Make a modification
        view.set(84);
        assert!(view.has_pending_changes().await);
        let hash_modified = view.historical_hash().await?;
        assert_ne!(hash_before, hash_modified);

        // Rollback
        view.rollback();
        assert!(!view.has_pending_changes().await);

        // Hash should return to previous value
        let hash_after_rollback = view.historical_hash().await?;
        assert_eq!(hash_before, hash_after_rollback);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_clear() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set and persist a value
        view.set(42);
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();

        assert_ne!(view.historical_hash().await?, HasherOutput::default());

        // Clear the view
        view.clear();
        assert!(view.has_pending_changes().await);

        // Flush the clear operation
        let mut batch = Batch::new();
        let delete_view = view.pre_save(&mut batch)?;
        assert!(!delete_view);
        context.store().write_batch(batch).await?;
        view.post_save();

        // Verify the view is not reset to default
        assert_ne!(view.historical_hash().await?, HasherOutput::default());

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_clone_unchecked() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set a value
        view.set(42);
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();

        let original_hash = view.historical_hash().await?;

        // Clone the view
        let mut cloned_view = view.clone_unchecked()?;

        // Verify the clone has the same hash initially
        let cloned_hash = cloned_view.historical_hash().await?;
        assert_eq!(original_hash, cloned_hash);

        // Modify the clone
        cloned_view.set(84);
        let cloned_hash_after = cloned_view.historical_hash().await?;
        assert_ne!(original_hash, cloned_hash_after);

        // Original should be unchanged
        let original_hash_after = view.historical_hash().await?;
        assert_eq!(original_hash, original_hash_after);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_flush_updates_stored_hash() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Initial state - no stored hash
        assert!(!view.has_pending_changes().await);

        // Set a value
        view.set(42);
        assert!(view.has_pending_changes().await);

        let hash_before_flush = view.historical_hash().await?;

        // Flush - this should update stored_hash
        let mut batch = Batch::new();
        let delete_view = view.pre_save(&mut batch)?;
        assert!(!delete_view);
        context.store().write_batch(batch).await?;
        view.post_save();

        assert!(!view.has_pending_changes().await);

        // Make another change
        view.set(84);
        let hash_after_second_change = view.historical_hash().await?;

        // The new hash should be based on the previous stored hash
        assert_ne!(hash_before_flush, hash_after_second_change);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_deref() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Test Deref - we can access inner view methods directly
        view.set(42);
        assert_eq!(*view.get(), 42);

        // Test DerefMut
        view.set(84);
        assert_eq!(*view.get(), 84);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_sequential_modifications() -> Result<(), ViewError> {
        async fn get_hash(values: &[u32]) -> Result<HasherOutput, ViewError> {
            let context = MemoryContext::new_for_testing(());
            let mut view =
                HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

            let mut previous_hash = view.historical_hash().await?;
            for &value in values {
                view.set(value);
                if value % 2 == 0 {
                    // Immediately save after odd values.
                    let mut batch = Batch::new();
                    view.pre_save(&mut batch)?;
                    context.store().write_batch(batch).await?;
                    view.post_save();
                }
                let current_hash = view.historical_hash().await?;
                assert_ne!(previous_hash, current_hash);
                previous_hash = current_hash;
            }
            Ok(previous_hash)
        }

        let h1 = get_hash(&[10, 20, 30, 40, 50]).await?;
        let h2 = get_hash(&[20, 30, 40, 50]).await?;
        let h3 = get_hash(&[20, 21, 30, 40, 50]).await?;
        assert_ne!(h1, h2);
        assert_eq!(h2, h3);
        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_flush_with_no_hash_change() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set and flush a value
        view.set(42);
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();

        let hash_before = view.historical_hash().await?;

        // Flush again without changes - no new hash should be stored
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        assert!(batch.is_empty());
        context.store().write_batch(batch).await?;
        view.post_save();

        let hash_after = view.historical_hash().await?;
        assert_eq!(hash_before, hash_after);

        Ok(())
    }
}
