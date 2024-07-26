// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use linera_base::command::resolve_binary;
#[cfg(with_metrics)]
use linera_base::sync::Lazy;
#[cfg(with_metrics)]
use linera_views::metering::KeyValueStoreMetrics;
use linera_views::{
    common::{CommonStoreConfig, MIN_VIEW_TAG},
    value_splitting::DatabaseConsistencyError,
};
use thiserror::Error;
use tonic::Status;

/// The shared store is potentially handling an infinite number of connections.
/// However, for testing or some other purpose we really need to decrease the number of
/// connections.
#[cfg(with_testing)]
const TEST_SHARED_STORE_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of entries in a stream of the tests can be controlled by this parameter for tests.
#[cfg(with_testing)]
const TEST_SHARED_STORE_MAX_STREAM_QUERIES: usize = 10;

// The maximal block size on GRPC is 4M.
//
// That size occurs in almost every use of GRPC and in particular the
// `tonic` library. In particular for the `tonic` library, the limit is
// both for incoming and outgoing messages.
// We decrease the 4194304 to 4000000 to leave space for the rest of the message
// (that includes length prefixes)
pub const MAX_PAYLOAD_SIZE: usize = 4000000;

/// Key tags to create the sub keys used for storing data on storage.
#[repr(u8)]
pub enum KeyTag {
    /// Prefix for the storage of the keys of the map
    Key = MIN_VIEW_TAG,
    /// Prefix for the storage of existence or not of the namespaces.
    Namespace,
}

#[derive(Debug, Error)]
pub enum ServiceStoreError {
    /// Not matching entry
    #[error("Not matching entry")]
    NotMatchingEntry,

    /// Failed to find the linera-storage-server binary
    #[error("Failed to find the linera-storage-server binary")]
    FailedToFindStorageServerBinary,

    /// gRPC error
    #[error(transparent)]
    GrpcError(#[from] Status),

    /// The key size must be at most 1 MB
    #[error("The key size must be at most 1 MB")]
    KeyTooLong,

    /// Transport error
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),

    /// An error occurred during BCS serialization
    #[error("An error occurred during BCS serialization")]
    Serialization(#[from] bcs::Error),

    /// The database is not consistent
    #[error(transparent)]
    DatabaseConsistencyError(#[from] DatabaseConsistencyError),
}

impl From<ServiceStoreError> for linera_views::views::ViewError {
    fn from(error: ServiceStoreError) -> Self {
        Self::StoreError {
            backend: "service".to_string(),
            error: error.to_string(),
        }
    }
}

#[cfg(with_testing)]
pub fn create_shared_store_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: Some(TEST_SHARED_STORE_MAX_CONCURRENT_QUERIES),
        max_stream_queries: TEST_SHARED_STORE_MAX_STREAM_QUERIES,
        cache_size: usize::MAX,
    }
}

#[cfg(with_metrics)]
pub(crate) static STORAGE_SERVICE_METRICS: Lazy<KeyValueStoreMetrics> =
    Lazy::new(|| KeyValueStoreMetrics::new("storage service".to_string()));

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
pub async fn get_service_storage_binary() -> Result<PathBuf, ServiceStoreError> {
    let binary = resolve_binary("linera-storage-server", "linera-storage-service").await;
    if let Ok(binary) = binary {
        return Ok(binary);
    }
    let binary = resolve_binary("../linera-storage-server", "linera-storage-service").await;
    if let Ok(binary) = binary {
        return Ok(binary);
    }
    Err(ServiceStoreError::FailedToFindStorageServerBinary)
}
