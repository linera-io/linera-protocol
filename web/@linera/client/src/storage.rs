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
pub async fn get_storage() -> Result<Storage, linera_views::indexed_db::IndexedDbStoreError> {
    Ok(linera_storage::DbStorage::maybe_create_and_connect(
        &linera_views::indexed_db::IndexedDbStoreConfig {
            max_stream_queries: 1,
        },
        "linera",
        Some(linera_execution::WasmRuntime::Wasmer),
    )
    .await?
    .with_allow_application_logs(true))
}
