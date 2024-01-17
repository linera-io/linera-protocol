// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tonic::Status;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SharedContextError {
    /// grpc error
    #[error("grpc error")]
    GrpcError(String),

    /// Not matching entry
    #[error("Not matching entry")]
    NotMatchingEntry,
}

impl From<Status> for SharedContextError {
    fn from(error: Status) -> Self {
        Self::GrpcError(error.to_string())
    }
}
