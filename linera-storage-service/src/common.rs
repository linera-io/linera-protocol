// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use linera_base::command::resolve_binary;
use linera_views::{
    lru_caching::LruCachingConfig,
    store::{CommonStoreInternalConfig, KeyValueStoreError},
};
use thiserror::Error;
use tonic::Status;

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
pub enum KeyPrefix {
    /// Key prefix for the storage of the keys of the map
    Key,
    /// Key prefix for the storage of existence or not of the namespaces.
    Namespace,
    /// Key prefix for the root key
    RootKey,
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

    /// Var error
    #[error(transparent)]
    VarError(#[from] std::env::VarError),

    /// An error occurred during BCS serialization
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    #[cfg(feature = "artificial_random_read_error")]
    /// An artificial read error occurred
    #[error("An artificial read error occurred")]
    ArtificialReadError,

    #[cfg(feature = "artificial_random_write_error")]
    /// An artificial write batch error occurred
    #[error("An artificial write batch error occurred")]
    ArtificialWriteBatchError,
}

impl KeyValueStoreError for ServiceStoreError {
    const BACKEND: &'static str = "service";
}

pub fn storage_service_test_endpoint() -> Result<String, ServiceStoreError> {
    Ok(std::env::var("LINERA_STORAGE_SERVICE")?)
}

#[derive(Debug, Clone)]
pub struct ServiceStoreInternalConfig {
    /// The endpoint used by the shared store
    pub endpoint: String,
    /// The common configuration code
    pub common_config: CommonStoreInternalConfig,
}

/// The config type
pub type ServiceStoreConfig = LruCachingConfig<ServiceStoreInternalConfig>;

impl ServiceStoreInternalConfig {
    pub fn http_address(&self) -> String {
        format!("http://{}", self.endpoint)
    }
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
