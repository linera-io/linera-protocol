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
    store::{ReadableKeyValueStore as _, WritableKeyValueStore as _},
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
    /// An override hash scheduled by [`Self::dump_content`]. While this is `Some`, the next
    /// save records this value as the new stored hash without mixing in the pending inner
    /// batch. Always derived from the canonical byte representation of the view's content,
    /// never from a caller-supplied value.
    #[allocative(visit = visit_allocative_simple)]
    force_stored_hash: Option<HasherOutput>,
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
            force_stored_hash: self.force_stored_hash,
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
            force_stored_hash: None,
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
        self.force_stored_hash = None;
    }

    async fn has_pending_changes(&self) -> bool {
        self.force_stored_hash.is_some() || self.inner.has_pending_changes().await
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut inner_batch = Batch::new();
        self.inner.pre_save(&mut inner_batch)?;
        let new_hash = {
            let mut maybe_hash = self.hash.lock().unwrap();
            if let Some(forced) = self.force_stored_hash {
                // The override pre-empts the hash chain: the inner batch is still written
                // to storage, but it does not contribute to the hash.
                *maybe_hash = Some(forced);
                forced
            } else {
                match maybe_hash.as_mut() {
                    Some(hash) => *hash,
                    None => {
                        let hash = Self::make_hash(self.stored_hash, &inner_batch)?;
                        *maybe_hash = Some(hash);
                        hash
                    }
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
        self.force_stored_hash = None;
        self.inner.post_save();
    }

    fn clear(&mut self) {
        self.inner.clear();
        *self.hash.get_mut().unwrap() = None;
        self.force_stored_hash = None;
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
            force_stored_hash: self.force_stored_hash,
            inner: self.inner.clone_unchecked()?,
        })
    }
}

impl<W: View> HistoricallyHashableView<W::Context, W> {
    /// Obtains a hash of the history of the changes in the view.
    pub async fn historical_hash(&mut self) -> Result<HasherOutput, ViewError> {
        if let Some(forced) = self.force_stored_hash {
            return Ok(forced);
        }
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

    /// Returns the canonical byte representation of the inner view's persisted content
    /// and arranges for the next save to record the hash of those bytes as the new
    /// stored hash. Subsequent updates extend the history from that hash normally.
    ///
    /// The bytes are the BCS encoding of `Vec<(Vec<u8>, Vec<u8>)>` — every entry stored
    /// under the inner view's prefix, in sorted lexicographic key order. Two views with
    /// identical persisted content produce identical bytes by construction; the hash is
    /// therefore reproducible by any party holding the bytes.
    ///
    /// Errors with [`ViewError::HasPendingChanges`] if the inner view has unflushed
    /// changes — the dump reads from the underlying KV store and would silently miss
    /// in-memory modifications.
    pub async fn dump_content(&mut self) -> Result<(Vec<u8>, HasherOutput), ViewError> {
        if self.inner.has_pending_changes().await {
            return Err(ViewError::HasPendingChanges);
        }
        let context = self.inner.context();
        // The inner context's base key is `<wrapper_base><Inner>`; passing it as the
        // search prefix scopes the dump to the inner view's data and excludes the
        // wrapper's hash key, which lives at `<wrapper_base><Hash>`.
        let inner_prefix = context.base_key().bytes.clone();
        let key_values = context
            .store()
            .find_key_values_by_prefix(&inner_prefix)
            .await
            .map_err(|err| ViewError::StoreError {
                backend: "HistoricallyHashableView::dump_content",
                error: Box::new(err),
                must_reload_view: false,
            })?;
        let bytes = bcs::to_bytes(&key_values)?;
        let hash = hash_bytes(&bytes);
        // Schedule the hash for the next save without forcing a save here, so the
        // checkpoint write coalesces with the rest of the block's batch.
        self.force_stored_hash = Some(hash);
        *self.hash.get_mut().unwrap() = None;
        Ok((bytes, hash))
    }

    /// Replaces the inner view's persisted content with `bytes` (a prior `dump_content`
    /// output) and atomically records the hash of those bytes as the new stored hash.
    /// Returns the recorded hash, so the caller can compare against an expected value.
    ///
    /// The replacement is save-atomic: this method writes a single batch containing the
    /// inner-prefix wipe, the decoded `(key, value)` puts, and the wrapper's hash key.
    /// The wrapper's normal `pre_save` lifecycle is bypassed.
    ///
    /// **The inner view's in-memory state is undefined after this returns.** Callers
    /// should drop the view and reload before using it further.
    pub async fn restore_from_content(&mut self, bytes: &[u8]) -> Result<HasherOutput, ViewError> {
        let entries = decode_key_values(bytes)?;
        let hash = hash_bytes(bytes);

        let context = self.inner.context();
        let inner_base = context.base_key().bytes.clone();
        let mut wrapper_hash_key = inner_base.clone();
        // The inner context's base key is `<wrapper_base><Inner tag>`; flipping the last
        // byte to `Hash` gives the wrapper's hash key.
        *wrapper_hash_key
            .last_mut()
            .expect("inner base key is non-empty") = KeyTag::Hash as u8;

        let mut batch = Batch::new();
        // Wipe whatever is currently under the inner prefix.
        batch.delete_key_prefix(inner_base.clone());
        // Re-insert the decoded entries.
        for (key, value) in entries {
            let mut full_key = inner_base.clone();
            full_key.extend_from_slice(&key);
            batch.put_key_value_bytes(full_key, value);
        }
        // Persist the new stored hash atomically with the content replacement.
        batch.put_key_value(wrapper_hash_key, &hash)?;

        context
            .store()
            .write_batch(batch)
            .await
            .map_err(|err| ViewError::StoreError {
                backend: "HistoricallyHashableView::restore_from_content",
                error: Box::new(err),
                must_reload_view: false,
            })?;

        // Update wrapper in-memory state; the inner view's in-memory state is now stale
        // and the caller is contractually obliged to reload.
        self.stored_hash = Some(hash);
        *self.hash.get_mut().unwrap() = Some(hash);
        self.force_stored_hash = None;

        Ok(hash)
    }
}

/// Decodes a canonical content byte string (a BCS-encoded `Vec<(Vec<u8>, Vec<u8>)>`)
/// and validates that keys are in strictly increasing lexicographic order. Returns
/// [`ViewError::MalformedContent`] if the ordering invariant is violated; BCS framing
/// errors surface as [`ViewError::BcsError`].
///
/// The order check is at this layer rather than relying on BCS, because BCS does not
/// constrain element ordering — only the bytes representation given a value. Two
/// callers building entries in different orders would produce different bytes and
/// different hashes; canonical content must always be sorted.
#[expect(clippy::type_complexity)]
fn decode_key_values(bytes: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
    let entries: Vec<(Vec<u8>, Vec<u8>)> = bcs::from_bytes(bytes)?;
    for window in entries.windows(2) {
        if window[1].0 <= window[0].0 {
            return Err(ViewError::MalformedContent(
                "keys must be in strictly increasing order",
            ));
        }
    }
    Ok(entries)
}

fn hash_bytes(bytes: &[u8]) -> HasherOutput {
    // Domain-separation tag: ensures the SHA3 input here cannot equal the SHA3 input of
    // `make_hash` (`<32-byte stored_hash> || bcs(batch)`). For the two SHA3 inputs to
    // coincide, an attacker would need a stored_hash equal to the first 32 bytes of this
    // tag — i.e., a SHA3-256 preimage attack. The tag is therefore at least 32 bytes long.
    const DOMAIN_TAG: &[u8] = b"linera-views::HistoricallyHashableView::dump_content/v1";
    const _: () = assert!(DOMAIN_TAG.len() >= 32);
    let mut hasher = sha3::Sha3_256::default();
    hasher
        .update_with_bytes(DOMAIN_TAG)
        .expect("Sha3_256 hashing of a byte slice cannot fail");
    hasher
        .update_with_bytes(bytes)
        .expect("Sha3_256 hashing of a byte slice cannot fail");
    hasher.finalize()
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
    use crate::{context::MemoryContext, register_view::RegisterView};

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

    #[tokio::test]
    async fn test_dump_content_then_save_records_content_hash() -> Result<(), ViewError> {
        // Persist some inner state, dump it, then save. The on-disk stored hash should be
        // the hash of the canonical bytes — independent of the prior history-of-batches
        // hash that the view had before dumping.
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        view.set(42);
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();

        let history_hash_before = view.historical_hash().await?;

        let (bytes, content_hash) = view.dump_content().await?;
        assert_ne!(history_hash_before, content_hash);
        assert_eq!(view.historical_hash().await?, content_hash);

        // Save: the override hash is persisted.
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();

        // Reload to confirm `stored_hash` on disk is now the content hash.
        let mut reloaded =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;
        assert_eq!(reloaded.historical_hash().await?, content_hash);
        assert_eq!(*reloaded.get(), 42);

        // And re-dumping produces the same bytes (canonical layout is deterministic).
        let (bytes_again, content_hash_again) = reloaded.dump_content().await?;
        assert_eq!(bytes, bytes_again);
        assert_eq!(content_hash, content_hash_again);

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_then_reload_matches_source() -> Result<(), ViewError> {
        // Dump from one view's state, restore those bytes into a fresh store, reload, and
        // confirm the restored view reports the same content hash and the same value.
        let source_context = MemoryContext::new_for_testing(());
        let mut source =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(source_context.clone())
                .await?;
        source.set(7);
        let mut batch = Batch::new();
        source.pre_save(&mut batch)?;
        source_context.store().write_batch(batch).await?;
        source.post_save();
        let (bytes, expected_hash) = source.dump_content().await?;

        // Restore into a fresh context and reload.
        let target_context = MemoryContext::new_for_testing(());
        let mut target =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(target_context.clone())
                .await?;
        let restored_hash = target.restore_from_content(&bytes).await?;
        assert_eq!(restored_hash, expected_hash);

        // Caller is contractually obliged to reload after restore.
        let mut reloaded =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(target_context.clone())
                .await?;
        assert_eq!(reloaded.historical_hash().await?, expected_hash);
        assert_eq!(*reloaded.get(), 7);

        // And re-dumping produces identical bytes — full round-trip.
        let (bytes_after_restore, _) = reloaded.dump_content().await?;
        assert_eq!(bytes, bytes_after_restore);

        Ok(())
    }

    #[tokio::test]
    async fn test_dump_content_errors_on_pending_changes() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;
        view.set(1);
        // The set() call did not flush, so the view has pending changes.
        match view.dump_content().await {
            Err(ViewError::HasPendingChanges) => Ok(()),
            other => panic!("expected HasPendingChanges, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_dump_content_rollback_discards_override() -> Result<(), ViewError> {
        // After dump_content, the view holds a forced-hash override. A rollback should
        // discard it, restoring the prior history-of-batches hash.
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;
        view.set(99);
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();

        let history_hash = view.historical_hash().await?;
        let (_, content_hash) = view.dump_content().await?;
        assert_eq!(view.historical_hash().await?, content_hash);

        view.rollback();
        assert_eq!(view.historical_hash().await?, history_hash);

        Ok(())
    }

    #[tokio::test]
    async fn test_decode_rejects_unsorted_keys() -> Result<(), ViewError> {
        // BCS-encode entries in non-increasing key order; restore should reject them.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"b".to_vec(), b"v1".to_vec()),
            (b"a".to_vec(), b"v2".to_vec()),
        ];
        let bytes = bcs::to_bytes(&entries).expect("encoding cannot fail");
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;
        match view.restore_from_content(&bytes).await {
            Err(ViewError::MalformedContent(_)) => Ok(()),
            other => panic!("expected MalformedContent, got {other:?}"),
        }
    }
}
