// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub type IdbStorage = linera_storage::DbStorage<
    linera_views::indexed_db::IndexedDbDatabase,
    linera_storage::WallClock,
>;

pub type MemStorage =
    linera_storage::DbStorage<linera_views::memory::MemoryDatabase, linera_storage::WallClock>;

pub type IdbEnvironment =
    linera_core::environment::Impl<IdbStorage, super::Network, super::Signer, super::Wallet>;

pub type MemEnvironment =
    linera_core::environment::Impl<MemStorage, super::Network, super::Signer, super::Wallet>;

/// Create and return the IndexedDB storage implementation.
///
/// # Errors
/// If the storage can't be initialized.
pub async fn get_idb_storage(namespace: &str) -> Result<IdbStorage, linera_views::ViewError> {
    Ok(linera_storage::DbStorage::maybe_create_and_connect(
        &linera_views::indexed_db::IndexedDbStoreConfig {
            max_stream_queries: 1,
        },
        namespace,
        Some(linera_execution::WasmRuntime::Wasmer),
        linera_storage::StorageCacheSizes {
            blob_cache_size: 1000,
            confirmed_block_cache_size: 1000,
            lite_certificate_cache_size: 1000,
            certificate_raw_cache_size: 1000,
            event_cache_size: 1000,
        },
    )
    .await?
    .with_allow_application_logs(true))
}

/// Create and return an in-memory storage implementation.
///
/// # Errors
/// If the storage can't be initialized.
pub async fn get_mem_storage(namespace: &str) -> Result<MemStorage, linera_views::ViewError> {
    Ok(linera_storage::DbStorage::maybe_create_and_connect(
        &linera_views::memory::MemoryStoreConfig {
            max_stream_queries: 1,
            kill_on_drop: false,
        },
        namespace,
        Some(linera_execution::WasmRuntime::Wasmer),
        1000,
    )
    .await?
    .with_allow_application_logs(true))
}
