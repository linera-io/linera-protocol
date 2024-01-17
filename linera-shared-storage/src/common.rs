// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tonic::Status;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SharedContextError {
    /// Not matching entry
    #[error("Not matching entry")]
    NotMatchingEntry,

    /// gRPC error
    #[error(transparent)]
    GrpcError(#[from] Status),

    /// Transport error
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
}
