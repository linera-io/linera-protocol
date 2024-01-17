// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SharedContextError {
    /// Underlying storage error
    #[error("Underlying storage error")]
    Underlying(String),

    /// Not matching entry
    #[error("Not matching entry")]
    NotMatchingEntry,
}
