// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Main error type for the crate.
#[derive(thiserror::Error, Debug)]
pub enum ViewError {
    /// BCS serialization error.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    /// Input output error.
    #[error("I/O error")]
    IoError(#[from] std::io::Error),

    /// Arithmetic error
    #[error(transparent)]
    ArithmeticError(#[from] linera_base::data_types::ArithmeticError),

    /// Failed to lock a reentrant collection entry since it is currently being accessed
    #[error(
        "failed to lock a reentrant collection entry since it is currently being accessed: {0:?}"
    )]
    TryLockError(Vec<u8>),

    /// Tokio errors can happen while joining.
    #[error("panic in sub-task: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    /// Errors within the context can occur and are presented as `ViewError`.
    #[error("storage operation error in {backend}: {error}")]
    StoreError {
        /// The name of the backend that produced the error
        backend: &'static str,
        /// The inner error
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The key must not be too long
    #[error("the key must not be too long")]
    KeyTooLong,

    /// The entry does not exist in memory
    // FIXME(#148): This belongs to a future `linera_storage::StoreError`.
    #[error("entry does not exist in storage: {0}")]
    NotFound(String),

    /// The database is corrupt: Entries don't have the expected hash.
    #[error("inconsistent database entries")]
    InconsistentEntries,

    /// The database is corrupt: Some entries are missing
    #[error("missing database entries for the context  {0}")]
    MissingEntries(String),

    /// The values are incoherent.
    #[error("post load values error")]
    PostLoadValuesError,
}

impl ViewError {
    /// Returns `true` if this error was caused by a journal resolution failure,
    /// which may leave storage in an inconsistent state requiring a view reload.
    pub fn is_journal_resolution_failure(&self) -> bool {
        let ViewError::StoreError { error, .. } = self else {
            return false;
        };
        // Walk the error chain looking for a JournalingError::ResolutionFailed.
        let mut source: Option<&(dyn std::error::Error)> = Some(error.as_ref());
        while let Some(err) = source {
            if err.to_string().starts_with("Journal resolution failed:") {
                return true;
            }
            source = err.source();
        }
        false
    }
}
