// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::command::resolve_binary;
use linera_views::{common::CommonStoreConfig, value_splitting::DatabaseConsistencyError};
use std::path::PathBuf;
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
pub enum ServiceContextError {
    /// Not matching entry
    #[error("Not matching entry")]
    NotMatchingEntry,

    /// Failed to find the storage_service_server binary
    #[error("Failed to find the storage_service_server binary")]
    FailedToFindStorageServiceServerBinary,

    /// gRPC error
    #[error(transparent)]
    GrpcError(#[from] Status),

    /// Transport error
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),

    /// An error occurred while doing BCS
    #[error("An error occurred while doing BCS")]
    Serialization(#[from] bcs::Error),

    /// The database is not consistent
    #[error(transparent)]
    DatabaseConsistencyError(#[from] DatabaseConsistencyError),
}

impl From<ServiceContextError> for linera_views::views::ViewError {
    fn from(error: ServiceContextError) -> Self {
        Self::ContextError {
            backend: "service".to_string(),
            error: error.to_string(),
        }
    }
}

#[cfg(any(test, feature = "test"))]
pub fn create_shared_store_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: Some(TEST_SHARED_STORE_MAX_CONCURRENT_QUERIES),
        max_stream_queries: TEST_SHARED_STORE_MAX_STREAM_QUERIES,
        cache_size: usize::MAX,
    }
}

#[derive(Debug, Clone)]
pub struct ServiceStoreConfig {
    /// The endpoint used by the shared store
    pub endpoint: String,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

/// Obtains the binary of the executable.
/// The path depends whether the test are run in the directory "linera-storage-service"
/// or in the main directory
pub async fn get_service_storage_binary() -> Result<PathBuf, ServiceContextError> {
    let binary = resolve_binary("storage_service_server", "linera-storage-service").await;
    if let Ok(binary) = binary {
        return Ok(binary);
    }
    let binary = resolve_binary("../storage_service_server", "linera-storage-service").await;
    if let Ok(binary) = binary {
        return Ok(binary);
    }
    Err(ServiceContextError::FailedToFindStorageServiceServerBinary)
}
