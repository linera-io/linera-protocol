// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::common::CommonStoreConfig;
use thiserror::Error;
use tonic::Status;

/// The shared store is potentially handling an infinite number of connections.
/// However, for testing or some other purpose we really need to decrease the number of
/// connections.
#[cfg(any(test, feature = "test"))]
const TEST_SHARED_STORE_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of entries in a stream of the tests can be controlled by this parameter for tests.
#[cfg(any(test, feature = "test"))]
const TEST_SHARED_STORE_MAX_STREAM_QUERIES: usize = 10;

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

#[cfg(any(test, feature = "test"))]
pub fn create_shared_store_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: Some(TEST_SHARED_STORE_MAX_CONCURRENT_QUERIES),
        max_stream_queries: TEST_SHARED_STORE_MAX_STREAM_QUERIES,
        cache_size: usize::MAX,
    }
}

#[derive(Debug)]
pub struct SharedStoreConfig {
    /// The endpoint used by the shared store
    pub endpoint: String,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}
