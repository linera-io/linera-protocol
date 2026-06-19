// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use linera_base::command::resolve_binary;
use linera_views::{lru_caching::LruCachingConfig, store::KeyValueStoreError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tonic::Status;

// The maximal block size on GRPC is 4M.
//
// That size occurs in almost every use of GRPC and in particular the
// `tonic` library. In particular for the `tonic` library, the limit is
// both for incoming and outgoing messages.
// We decrease the 4194304 to 4000000 to leave space for the rest of the message
// (that includes length prefixes)
/// The maximum size of a gRPC payload, in bytes.
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

/// The errors returned by the storage service store.
#[derive(Debug, Error)]
pub enum StorageServiceStoreError {
    /// Store already exists during a create operation
    #[error("Store already exists during a create operation")]
    StoreAlreadyExists,

    /// Not matching entry
    #[error("Not matching entry")]
    NotMatchingEntry,

    /// Failed to find the linera-storage-server binary
    #[error("Failed to find the linera-storage-server binary")]
    FailedToFindStorageServerBinary,

    /// gRPC error
    #[error(transparent)]
    GrpcError(#[from] Box<Status>),

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
}

impl From<Status> for StorageServiceStoreError {
    fn from(error: Status) -> Self {
        Box::new(error).into()
    }
}

impl KeyValueStoreError for StorageServiceStoreError {
    const BACKEND: &'static str = "service";

    fn must_reload_view(&self) -> bool {
        // A `write_batch` is sent as one or more gRPC calls. If the round-trip fails
        // (a gRPC status such as `DEADLINE_EXCEEDED`/`UNAVAILABLE`, or a transport-level
        // error) the server may or may not have applied the batch, so the in-memory view
        // must be reloaded from storage. These variants can also surface on read RPCs,
        // where a reload is unnecessary but harmless; we err on the side of reloading.
        matches!(self, Self::GrpcError(_) | Self::TransportError(_))
    }
}

/// Returns the storage service endpoint used for testing, read from the environment.
pub fn storage_service_test_endpoint() -> Result<String, StorageServiceStoreError> {
    Ok(std::env::var("LINERA_STORAGE_SERVICE")?)
}

/// The configuration for connecting to a storage service.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageServiceStoreInternalConfig {
    /// The endpoint used by the shared store
    pub endpoint: String,
    /// Maximum number of concurrent database queries allowed for this client.
    pub max_concurrent_queries: Option<usize>,
    /// Preferred buffer size for async streams.
    pub max_stream_queries: usize,
}

/// The config type
pub type StorageServiceStoreConfig = LruCachingConfig<StorageServiceStoreInternalConfig>;

impl StorageServiceStoreInternalConfig {
    /// Returns the HTTP address of the endpoint.
    pub fn http_address(&self) -> String {
        format!("http://{}", self.endpoint)
    }
}

/// Obtains the binary of the executable.
/// The path depends whether the test are run in the directory "linera-storage-service"
/// or in the main directory
pub async fn get_service_storage_binary() -> Result<PathBuf, StorageServiceStoreError> {
    let binary = resolve_binary("linera-storage-server", "linera-storage-service").await;
    if let Ok(binary) = binary {
        return Ok(binary);
    }
    let binary = resolve_binary("../linera-storage-server", "linera-storage-service").await;
    if let Ok(binary) = binary {
        return Ok(binary);
    }
    Err(StorageServiceStoreError::FailedToFindStorageServerBinary)
}
