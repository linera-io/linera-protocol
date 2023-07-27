// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{batch::Batch, common::HasherOutput};
use async_trait::async_trait;
use linera_base::crypto::CryptoHash;
pub use linera_views_derive::{
    CryptoHashRootView, CryptoHashView, GraphQLView, HashableView, RootView, View,
};
use serde::Serialize;
use std::{fmt::Debug, io::Write};
use thiserror::Error;

#[cfg(test)]
#[path = "unit_tests/views.rs"]
mod tests;

/// A view gives exclusive access to read and write the data stored at an underlying
/// address in storage.
#[async_trait]
pub trait View<C>: Sized {
    /// Obtains a mutable reference to the internal context.
    fn context(&self) -> &C;

    /// Creates a view or a subview.
    async fn load(context: C) -> Result<Self, ViewError>;

    /// Discards all pending changes. After that `flush` should have no effect to storage.
    fn rollback(&mut self);

    /// Clears the view. That can be seen as resetting to default. In the case of a RegisterView
    /// this means setting the value to T::default(). For LogView, QueueView, this leaves
    /// the range data to be left in the database.
    fn clear(&mut self);

    /// Persists changes to storage. This leaves the view still usable and is essentially neutral to the
    /// program running. Crash-resistant storage implementations are expected to accumulate the desired
    /// changes in the `batch` variable first. If the view is dropped without calling `flush`, staged
    /// changes are simply lost.
    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError>;

    /// Instead of persisting changes, clears all the data that belong to this view and its
    /// subviews. Crash-resistant storage implementations are expected to accumulate the
    /// desired changes into the `batch` variable first.
    /// No data/metadata at all is left after deletion. The view is consumed by `delete`.
    fn delete(self, batch: &mut Batch);
}

/// Main error type for the crate.
#[derive(Error, Debug)]
pub enum ViewError {
    /// An error occurred while doing BCS serialization.
    #[error("failed to serialize value to calculate its hash")]
    Serialization(#[from] bcs::Error),

    /// We failed to acquire an entry in a CollectionView or a ReentrantCollectionView.
    #[error("trying to access a collection view or reentrant collection view while some entries are still being accessed")]
    CannotAcquireCollectionEntry,

    /// Input output error.
    #[error("IO error")]
    Io(#[from] std::io::Error),

    /// An error happened while trying to lock.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("Failed to lock collection entry: {0}")]
    TryLockError(#[from] tokio::sync::TryLockError),

    /// Tokio errors can happen while joining.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("Panic in sub-task: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    /// Errors within the context can occur and are presented as ViewError.
    #[error("Storage operation error in {backend}: {error}")]
    ContextError {
        /// backend can be e.g. RocksDB / DynamoDB / Memory / etc.
        backend: String,
        /// error is the specific problem that occurred within that context
        error: String,
    },

    /// Errors can happen within the Wasm guest and have to be transmitted within the Host/Guest where only elementary types can pass.
    #[error("Following error occurs in wasm code: {0}")]
    WasmHostGuestError(String),

    /// FIXME(#148): This belongs to a future `linera_storage::StoreError`.
    #[error("Entry does not exist in memory: {0}")]
    NotFound(String),

    /// The database is corrupt: Entries don't have the expected hash.
    #[error("Inconsistent database entries")]
    InconsistentEntries,

    /// The database is corrupt: Some entries are missing
    #[error("Missing database entries")]
    MissingEntries,

    /// The value is too large for the client
    #[error("The value is too large for the client")]
    TooLargeValue,
}

impl ViewError {
    /// Creates a `NotFound` error with the given message and key.
    pub fn not_found<T: Debug>(msg: &str, key: T) -> ViewError {
        ViewError::NotFound(format!("{} {:?}", msg, key))
    }
}

/// A view that supports hashing its values.
#[async_trait]
pub trait HashableView<C>: View<C> {
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
#[async_trait]
pub trait RootView<C>: View<C> {
    /// Saves the root view to the database context
    async fn save(&mut self) -> Result<(), ViewError>;

    /// Deletes the root view to the database context
    async fn write_delete(self) -> Result<(), ViewError>;
}

/// A [`View`] that also supports crypto hash
#[async_trait]
pub trait CryptoHashView<C>: HashableView<C> {
    /// Computing the hash and attributing the type to it.
    async fn crypto_hash(&self) -> Result<CryptoHash, ViewError>;
}

/// A [`RootView`] that also supports crypto hash
#[async_trait]
pub trait CryptoHashRootView<C>: RootView<C> + CryptoHashView<C> {}
