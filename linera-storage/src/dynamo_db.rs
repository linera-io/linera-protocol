// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::DbStorage;
use linera_views::dynamo_db::DynamoDbStore;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_views::{
        dynamo_db::{create_dynamo_db_test_config, DynamoDbContextError, DynamoDbStoreConfig},
        test_utils::get_namespace,
    },
};

#[cfg(test)]
#[path = "unit_tests/dynamo_db.rs"]
mod tests;

pub type DynamoDbStorage<C> = DbStorage<DynamoDbStore, C>;

#[cfg(any(test, feature = "test"))]
impl DynamoDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let store_config = create_dynamo_db_test_config().await;
        let namespace = get_namespace();
        DynamoDbStorage::new_for_testing(store_config, &namespace, wasm_runtime, TestClock::new())
            .await
            .expect("storage")
    }

    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, DynamoDbContextError> {
        let storage =
            DbStorageInner::<DynamoDbStore>::new_for_testing(store_config, namespace, wasm_runtime)
                .await?;
        Ok(Self::create(storage, clock))
    }
}
