// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Debug, io::Write};

use linera_base::crypto::CryptoHash;
pub use linera_views_derive::{
    ClonableView, CryptoHashRootView, CryptoHashView, HashableView, RootView, View,
};
use serde::Serialize;

use crate::{batch::Batch, common::HasherOutput, ViewError};

#[cfg(test)]
#[path = "unit_tests/views.rs"]
mod tests;

/// The `RegisterView` implements a register for a single value.
pub mod register_view;

/// The `LogView` implements a log list that can be pushed.
pub mod log_view;

/// The `BucketQueueView` implements a queue that can push on the back and delete on the front and group data in buckets.
pub mod bucket_queue_view;

/// The `QueueView` implements a queue that can push on the back and delete on the front.
pub mod queue_view;

/// The `MapView` implements a map with ordered keys.
pub mod map_view;

/// The `SetView` implements a set with ordered entries.
pub mod set_view;

/// The `CollectionView` implements a map structure whose keys are ordered and the values are views.
pub mod collection_view;

/// The `ReentrantCollectionView` implements a map structure whose keys are ordered and the values are views with concurrent access.
pub mod reentrant_collection_view;

/// The implementation of a key-value store view.
pub mod key_value_store_view;

/// Wrapping a view to compute a hash.
pub mod hashable_wrapper;

/// The minimum value for the view tags. Values in `0..MIN_VIEW_TAG` are used for other purposes.
pub const MIN_VIEW_TAG: u8 = 1;

/// A view gives exclusive access to read and write the data stored at an underlying
/// address in storage.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
pub trait View: Sized {
    /// The number of keys used for the initialization
    const NUM_INIT_KEYS: usize;

    /// The type of context stored in this view
    type Context: crate::context::Context;

    /// Obtains a mutable reference to the internal context.
    fn context(&self) -> &Self::Context;

    /// Creates the keys needed for loading the view
    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError>;

    /// Loads a view from the values
    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError>;

    /// Loads a view
    async fn load(context: Self::Context) -> Result<Self, ViewError>;

    /// Discards all pending changes. After that `flush` should have no effect to storage.
    fn rollback(&mut self);

    /// Returns [`true`] if flushing this view would result in changes to the persistent storage.
    async fn has_pending_changes(&self) -> bool;

    /// Clears the view. That can be seen as resetting to default. If the clear is followed
    /// by a flush then all the relevant data is removed on the storage.
    fn clear(&mut self);

    /// Persists changes to storage. This leaves the view still usable and is essentially neutral to the
    /// program running. Crash-resistant storage implementations are expected to accumulate the desired
    /// changes in the `batch` variable first. If the view is dropped without calling `flush`, staged
    /// changes are simply lost.
    /// The returned boolean indicates whether the operation removes the view or not.
    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError>;

    /// Builds a trivial view that is already deleted
    fn new(context: Self::Context) -> Result<Self, ViewError> {
        let values = vec![None; Self::NUM_INIT_KEYS];
        let mut view = Self::post_load(context, &values)?;
        view.clear();
        Ok(view)
    }
}

/// A view which can have its context replaced.
pub trait ReplaceContext<C: crate::context::Context>: View {
    /// The type returned after replacing the context.
    type Target: View<Context = C>;

    /// Returns a view with a replaced context.
    async fn with_context(&mut self, ctx: impl FnOnce(&Self::Context) -> C + Clone)
        -> Self::Target;
}

/// A view that supports hashing its values.
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait HashableView: View {
    /// How to compute hashes.
    type Hasher: Hasher;

    /// Computes the hash of the values.
    ///
    /// Implementations do not need to include a type tag. However, the usual precautions
    /// to enforce collision resistance must be applied (e.g. including the length of a
    /// collection of values).
    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError>;

    /// Computes the hash of the values.
    ///
    /// Implementations do not need to include a type tag. However, the usual precautions
    /// to enforce collision resistance must be applied (e.g. including the length of a
    /// collection of values).
    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError>;
}

/// The requirement for the hasher type in [`HashableView`].
pub trait Hasher: Default + Write + Send + Sync + 'static {
    /// The output type.
    type Output: Debug + Clone + Eq + AsRef<[u8]> + 'static;

    /// Finishes the hashing process and returns its output.
    fn finalize(self) -> Self::Output;

    /// Serializes a value with BCS and includes it in the hash.
    fn update_with_bcs_bytes(&mut self, value: &impl Serialize) -> Result<(), ViewError> {
        bcs::serialize_into(self, value)?;
        Ok(())
    }

    /// Includes bytes in the hash.
    fn update_with_bytes(&mut self, value: &[u8]) -> Result<(), ViewError> {
        self.write_all(value)?;
        Ok(())
    }
}

impl Hasher for sha3::Sha3_256 {
    type Output = HasherOutput;

    fn finalize(self) -> Self::Output {
        <sha3::Sha3_256 as sha3::Digest>::finalize(self)
    }
}

/// A [`View`] whose staged modifications can be saved in storage.
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait RootView: View {
    /// Saves the root view to the database context
    async fn save(&mut self) -> Result<(), ViewError>;
}

/// A [`View`] that also supports crypto hash
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait CryptoHashView: HashableView {
    /// Computing the hash and attributing the type to it.
    async fn crypto_hash(&self) -> Result<CryptoHash, ViewError>;

    /// Computing the hash and attributing the type to it.
    async fn crypto_hash_mut(&mut self) -> Result<CryptoHash, ViewError>;
}

/// A [`RootView`] that also supports crypto hash
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait CryptoHashRootView: RootView + CryptoHashView {}

/// A [`ClonableView`] supports being shared (unsafely) by cloning it.
///
/// Sharing is unsafe because by having two view instances for the same data, they may have invalid
/// state if both are used for writing.
///
/// Sharing the view is guaranteed to not cause data races if only one of the shared view instances
/// is used for writing at any given point in time.
pub trait ClonableView: View {
    /// Creates a clone of this view, sharing the underlying storage context but prone to
    /// data races which can corrupt the view state.
    fn clone_unchecked(&mut self) -> Self;
}
