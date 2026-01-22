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
    store::ReadableSyncKeyValueStore as _,
    sync_view::{SyncClonableView, SyncReplaceContext, SyncView, MIN_VIEW_TAG},
    views::{Hasher, HasherOutput},
    ViewError,
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

impl<C, W, C2> SyncReplaceContext<C2> for HistoricallyHashableView<C, W>
where
    W: SyncView<Context = C> + SyncReplaceContext<C2>,
    C: Context,
    C2: Context,
{
    type Target = HistoricallyHashableView<C2, <W as SyncReplaceContext<C2>>::Target>;

    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        HistoricallyHashableView {
            _phantom: PhantomData,
            stored_hash: self.stored_hash,
            hash: Mutex::new(*self.hash.get_mut().unwrap()),
            inner: self.inner.with_context(ctx),
        }
    }
}

impl<W> SyncView for HistoricallyHashableView<W::Context, W>
where
    W: SyncView,
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

    fn load(context: Self::Context) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(&keys)?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.inner.rollback();
        *self.hash.get_mut().unwrap() = self.stored_hash;
    }

    fn has_pending_changes(&self) -> bool {
        self.inner.has_pending_changes()
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

impl<W> SyncClonableView for HistoricallyHashableView<W::Context, W>
where
    W: SyncClonableView,
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

impl<W: SyncView> HistoricallyHashableView<W::Context, W> {
    /// Obtains a hash of the history of the changes in the view.
    pub fn historical_hash(&mut self) -> Result<HasherOutput, ViewError> {
        if let Some(hash) = self.hash.get_mut().unwrap() {
            return Ok(*hash);
        }
        let mut batch = Batch::new();
        self.pre_save(&mut batch)?;
        let new_hash = Self::make_hash(self.stored_hash, &batch)?;
        *self.hash.get_mut().unwrap() = Some(new_hash);
        Ok(new_hash)
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

