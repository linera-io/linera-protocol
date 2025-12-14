// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO(#12): convert to IndexedDbStore once we refactor Context
pub type Storage =
    linera_storage::DbStorage<linera_views::memory::MemoryDatabase, linera_storage::WallClock>;

pub async fn get_storage() -> Result<Storage, linera_views::memory::MemoryStoreError> {
    Ok(linera_storage::DbStorage::maybe_create_and_connect(
        &linera_views::memory::MemoryStoreConfig {
            max_stream_queries: 1,
            kill_on_drop: false,
        },
        "linera",
        Some(linera_execution::WasmRuntime::Wasmer),
    )
    .await?
    .with_allow_application_logs(true))
}
