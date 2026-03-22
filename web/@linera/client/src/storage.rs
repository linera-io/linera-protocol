// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

pub type Storage = linera_storage::DbStorage<
    linera_views::indexed_db::IndexedDbDatabase,
    linera_storage::WallClock,
>;

/// Create and return the storage implementation.
///
/// # Errors
/// If the storage can't be initialized or if the operation times out.
pub async fn get_storage(
    namespace: &str,
    timeout: Duration,
) -> Result<Storage, linera_views::ViewError> {
    let storage = linera_base::time::timer::timeout(
        timeout,
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
