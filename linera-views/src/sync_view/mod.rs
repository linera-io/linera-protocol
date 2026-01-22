// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::crypto::CryptoHash;
use crate::{batch::Batch, ViewError};

pub use linera_views_derive::SyncView;
pub use crate::views::Hasher;

/// The `SyncRegisterView` implements a register for a single value.
pub mod register_view;

/// The `SyncLogView` implements a log list that can be pushed.
pub mod log_view;

/// The `SyncBucketQueueView` implements a queue that can push on the back and delete on the front and group data in buckets.
pub mod bucket_queue_view;

/// The `SyncQueueView` implements a queue that can push on the back and delete on the front.
pub mod queue_view;

/// The `SyncMapView` implements a map with ordered keys.
pub mod map_view;

/// The `SyncSetView` implements a set with ordered entries.
pub mod set_view;

/// The `SyncCollectionView` implements a map structure whose keys are ordered and the values are views.
pub mod collection_view;

/// The implementation of a key-value store view.
pub mod key_value_store_view;

/// Wrapping a view to memoize hashing.
pub mod hashable_wrapper;

/// Wrapping a view to compute hash based on the history of modifications to the view.
pub mod historical_hash_wrapper;

/// The minimum value for the view tags. Values in `0..MIN_VIEW_TAG` are used for other purposes.
pub const MIN_VIEW_TAG: u8 = crate::views::MIN_VIEW_TAG;

/// A synchronous view gives exclusive access to read and write the data stored at an underlying
/// address in storage.
pub trait SyncView: Sized {
    /// The number of keys used for the initialization.
    const NUM_INIT_KEYS: usize;

    /// The type of context stored in this view.
    type Context: crate::context::Context;

    /// Obtains a mutable reference to the internal context.
    fn context(&self) -> Self::Context;

    /// Creates the keys needed for loading the view.
    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError>;

    /// Loads a view from the values.
    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError>;

    /// Loads a view.
    fn load(context: Self::Context) -> Result<Self, ViewError> {
        if Self::NUM_INIT_KEYS == 0 {
            Self::post_load(context, &[])
        } else {
            use crate::{context::Context, store::ReadableSyncKeyValueStore as _};
            let keys = Self::pre_load(&context)?;
            let values = context.store().read_multi_values_bytes(&keys)?;
            Self::post_load(context, &values)
        }
    }

    /// Discards all pending changes. After that `save` should have no effect to storage.
    fn rollback(&mut self);

    /// Returns [`true`] if saving this view would result in changes to the persistent storage.
    fn has_pending_changes(&self) -> bool;

    /// Clears the view. That can be seen as resetting to default. If the clear is followed
    /// by a save then all the relevant data is removed on the storage.
    fn clear(&mut self);

    /// Computes the batch of operations to persist changes to storage without modifying the view.
    /// Crash-resistant storage implementations accumulate the desired changes in the `batch` variable.
    /// The returned boolean indicates whether the operation removes the view or not.
    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError>;

    /// Updates the view state after the batch has been executed in the database.
    /// This should be called after `pre_save` and after the batch has been successfully written to storage.
    /// This leaves the view in a clean state with no pending changes.
    ///
    /// May panic if `pre_save` was not called right before on `self`.
    fn post_save(&mut self);

    /// Builds a trivial view that is already deleted.
    fn new(context: Self::Context) -> Result<Self, ViewError> {
        let values = vec![None; Self::NUM_INIT_KEYS];
        let mut view = Self::post_load(context, &values)?;
        view.clear();
        Ok(view)
    }
}

/// A view which can have its context replaced.
pub trait SyncReplaceContext<C: crate::context::Context>: SyncView {
    /// The type returned after replacing the context.
    type Target: SyncView<Context = C>;

    /// Returns a view with a replaced context.
    fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C + Clone) -> Self::Target;
}

/// A view that supports hashing its values.
pub trait SyncHashableView: SyncView {
    /// How to compute hashes.
    type Hasher: Hasher;

    /// Computes the hash of the values.
    fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError>;

    /// Same as `hash` but guaranteed to be wait-free.
    fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError>;
}

/// A [`SyncView`] whose staged modifications can be saved in storage.
pub trait SyncRootView: SyncView {
    /// Saves the root view to the database context.
    fn save(&mut self) -> Result<(), ViewError> {
        use crate::{context::Context as _, store::WritableSyncKeyValueStore as _};
        let mut batch = Batch::new();
        self.pre_save(&mut batch)?;
        if !batch.is_empty() {
            self.context().store().write_batch(batch)?;
        }
        self.post_save();
        Ok(())
    }
}

impl<T> SyncRootView for T where T: SyncView {}

/// A [`SyncView`] that also supports crypto hash.
pub trait SyncCryptoHashView: SyncHashableView {
    /// Computing the hash and attributing the type to it. May require locking.
    fn crypto_hash(&self) -> Result<CryptoHash, ViewError>;

    /// Same as `crypto_hash` but guaranteed to be wait-free.
    fn crypto_hash_mut(&mut self) -> Result<CryptoHash, ViewError>;
}

/// A [`SyncRootView`] that also supports crypto hash.
pub trait SyncCryptoHashRootView: SyncRootView + SyncCryptoHashView {}

/// A view that can be shared (unsafely) by cloning it.
pub trait SyncClonableView: SyncView {
    /// Creates a clone of this view, sharing the underlying storage context but prone to
    /// data races which can corrupt the view state.
    fn clone_unchecked(&mut self) -> Result<Self, ViewError>;
}

impl<T> crate::views::View for T
where
    T: SyncView,
{
    const NUM_INIT_KEYS: usize = T::NUM_INIT_KEYS;

    type Context = T::Context;

    fn context(&self) -> Self::Context {
        SyncView::context(self)
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        SyncView::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        SyncView::post_load(context, values)
    }

    async fn load(context: Self::Context) -> Result<Self, ViewError> {
        SyncView::load(context)
    }

    fn rollback(&mut self) {
        SyncView::rollback(self)
    }

    async fn has_pending_changes(&self) -> bool {
        SyncView::has_pending_changes(self)
    }

    fn clear(&mut self) {
        SyncView::clear(self)
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        SyncView::pre_save(self, batch)
    }

    fn post_save(&mut self) {
        SyncView::post_save(self)
    }
}

impl<T, C2> crate::views::ReplaceContext<C2> for T
where
    T: SyncReplaceContext<C2>,
    C2: crate::context::Context,
{
    type Target = T::Target;

    async fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C2 + Clone) -> Self::Target {
        SyncReplaceContext::with_context(self, ctx)
    }
}

impl<T> crate::views::ClonableView for T
where
    T: SyncClonableView,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        SyncClonableView::clone_unchecked(self)
    }
}

impl<T> crate::views::HashableView for T
where
    T: SyncHashableView,
{
    type Hasher = T::Hasher;

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        SyncHashableView::hash(self)
    }

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        SyncHashableView::hash_mut(self)
    }
}

impl<T> crate::views::RootView for T
where
    T: SyncRootView,
{
    async fn save(&mut self) -> Result<(), ViewError> {
        SyncRootView::save(self)
    }
}

impl<T> crate::views::CryptoHashView for T
where
    T: SyncCryptoHashView,
{
    async fn crypto_hash(&self) -> Result<CryptoHash, ViewError> {
        SyncCryptoHashView::crypto_hash(self)
    }

    async fn crypto_hash_mut(&mut self) -> Result<CryptoHash, ViewError> {
        SyncCryptoHashView::crypto_hash_mut(self)
    }
}

impl<T> crate::views::CryptoHashRootView for T where T: SyncCryptoHashRootView {}
