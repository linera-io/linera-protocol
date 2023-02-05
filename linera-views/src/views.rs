// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

extern crate linera_views_derive;
use crate::common::Batch;
use async_trait::async_trait;
use linera_base::crypto::CryptoHash;
pub use linera_views_derive::{ContainerView, HashableContainerView, HashableView, View};
use serde::Serialize;
use std::{fmt::Debug, io::Write};
use thiserror::Error;

#[cfg(test)]
#[path = "unit_tests/views.rs"]
mod tests;

/// A view gives an exclusive access to read and write the data stored at an underlying
/// address in storage.
#[async_trait]
pub trait View<C>: Sized {
    /// Obtain a mutable reference to the internal context.
    fn context(&self) -> &C;

    /// Create a view or a subview.
    async fn load(context: C) -> Result<Self, ViewError>;

    /// Discard all pending changes. After that `flush` should have no effect to storage.
    fn rollback(&mut self);

    /// Clear the view. That can be seen as resetting to default. In the case of a RegisterView
    /// this means setting the value to T::default(). For LogView, QueueView, this leaves
    /// the range data to be left in the database.
    fn clear(&mut self);

    /// Persist changes to storage. This leaves the view still usable and is essentially neutral to the
    /// program running. Crash-resistant storage implementations are expected to accumulate the desired
    /// changes in the `batch` variable first. If the view is dropped without calling `flush`, staged
    /// changes are simply lost.
    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError>;

    /// Instead of persisting changes, clear all the data that belong to this view and its
    /// subviews. Crash-resistant storage implementations are expected to accumulate the
    /// desired changes into the `batch` variable first.
    /// No data/metadata at all is left after delete. The view is consumed by delete
    /// and cannot be used in any way after delete.
    fn delete(self, batch: &mut Batch);
}

/// Main error type for the crate.
#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum ViewError {
    #[error("the entry with key {0} was removed thus cannot be loaded any more")]
    RemovedEntry(String),

    #[error("failed to serialize value to calculate its hash")]
    Serialization(#[from] bcs::Error),

    #[error("trying to access a collection view while some entries are still being accessed")]
    CannotAcquireCollectionEntry,

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[cfg(not(target_arch = "wasm32"))]
    #[error("Failed to lock collection entry: {0}")]
    TryLockError(#[from] tokio::sync::TryLockError),

    #[cfg(not(target_arch = "wasm32"))]
    #[error("Panic in sub-task: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    #[error("Storage operation error in {backend}: {error}")]
    ContextError { backend: String, error: String },

    /// FIXME(#148): This belongs to a future `linera_storage::StoreError`.
    #[error("Entry does not exist in memory: {0}")]
    NotFound(String),
}

/// A view that supports hashing its values.
#[async_trait]
pub trait HashableView<C>: View<C> {
    /// How to compute hashes.
    type Hasher: Hasher;

    /// Compute the hash of the values.
    ///
    /// Implementations do not need to include a type tag. However, the usual precautions
    /// to enforce collision-resistance must be applied (e.g. including the length of a
    /// collection of values).
    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError>;

    /// Compute the hash of the values.
    ///
    /// Implementations do not need to include a type tag. However, the usual precautions
    /// to enforce collision-resistance must be applied (e.g. including the length of a
    /// collection of values).
    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError>;
}

/// The requirement for the hasher type in [`HashableView`].
pub trait Hasher: Default + Write + Send + Sync + 'static {
    /// The output type.
    type Output: Debug + Clone + Eq + AsRef<[u8]> + 'static;

    /// Finish the hashing process and return its output.
    fn finalize(self) -> Self::Output;

    /// Serialize a value with BCS and include it in the hash.
    fn update_with_bcs_bytes(&mut self, value: &impl Serialize) -> Result<(), ViewError> {
        bcs::serialize_into(self, value)?;
        Ok(())
    }

    /// Include bytes in the hash.
    fn update_with_bytes(&mut self, value: &[u8]) -> Result<(), ViewError> {
        self.write_all(value)?;
        Ok(())
    }
}

impl Hasher for sha2::Sha512 {
    type Output = generic_array::GenericArray<u8, <sha2::Sha512 as sha2::Digest>::OutputSize>;

    fn finalize(self) -> Self::Output {
        <sha2::Sha512 as sha2::Digest>::finalize(self)
    }
}

/// A [`View`] whose staged modifications can be saved in storage.
#[async_trait]
pub trait ContainerView<C>: View<C> {
    /// Save the container view to a file
    async fn save(&mut self) -> Result<(), ViewError>;

    /// Delete the container view from the database
    async fn write_delete(self) -> Result<(), ViewError>;
}

/// A [`ContainerView`] that also supports hashing.
#[async_trait]
pub trait HashableContainerView<C>: ContainerView<C> + HashableView<C> {
    /// Computing the hash and attributing the type to it.
    async fn crypto_hash(&self) -> Result<CryptoHash, ViewError>;
}
