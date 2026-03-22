// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

pub type Storage = linera_storage::DbStorage<
    linera_views::indexed_db::IndexedDbDatabase,
    linera_storage::WallClock,
>;

/// Timeout for opening IndexedDB storage. If a previous connection was not
/// properly closed (e.g. page reload during a write), IndexedDB's `open()` can
/// block indefinitely waiting for the old connection to release. This timeout
/// ensures we fail fast instead of hanging forever.
const STORAGE_OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// Create and return the storage implementation.
///
/// # Errors
/// If the storage can't be initialized or if the operation times out.
pub async fn get_storage(namespace: &str) -> Result<Storage, linera_views::ViewError> {
    let storage = linera_base::time::timer::timeout(
        STORAGE_OPEN_TIMEOUT,
        linera_storage::DbStorage::maybe_create_and_connect(
            &linera_views::indexed_db::IndexedDbStoreConfig {
                max_stream_queries: 1,
            },
            namespace,
            Some(linera_execution::WasmRuntime::Wasmer),
            1000,
        ),
    )
    .await
    .map_err(|_| linera_views::ViewError::StoreError {
        backend: "indexed_db",
        error: Box::<dyn std::error::Error + Send + Sync>::from(
            "Timed out opening IndexedDB storage. This usually means a previous \
             connection was not properly closed. Try clearing IndexedDB in DevTools \
             (Application > IndexedDB) and reloading.",
        ),
    })??;

    Ok(storage.with_allow_application_logs(true))
}
