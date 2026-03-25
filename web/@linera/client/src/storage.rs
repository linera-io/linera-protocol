// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub type Storage = linera_storage::DbStorage<
    linera_views::indexed_db::IndexedDbDatabase,
    linera_storage::WallClock,
>;

/// Create and return the storage implementation.
///
/// # Errors
/// If the storage can't be initialized.
pub async fn get_storage(namespace: &str) -> Result<Storage, linera_views::ViewError> {
    Ok(linera_storage::DbStorage::maybe_create_and_connect(
        &linera_views::indexed_db::IndexedDbStoreConfig {
            max_stream_queries: 1,
        },
        namespace,
        Some(linera_execution::WasmRuntime::Wasmer),
        linera_storage::StorageCacheSizes {
            blob_cache_size: 1024 * 1024,
            confirmed_block_cache_size: 1024 * 1024,
            lite_certificate_cache_size: 1024 * 1024,
            certificate_raw_cache_size: 1024 * 1024,
            event_cache_size: 1024 * 1024,
        },
    )
    .await?
    .with_allow_application_logs(true))
}
