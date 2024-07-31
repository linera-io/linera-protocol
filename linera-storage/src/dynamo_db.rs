// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::dynamo_db::DynamoDbStore;
#[cfg(with_testing)]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_views::{
        dynamo_db::{create_dynamo_db_test_config, DynamoDbStoreConfig, DynamoDbStoreError},
        test_utils::generate_test_namespace,
    },
};

use crate::db_storage::DbStorage;

pub type DynamoDbStorage<C> = DbStorage<DynamoDbStore, C>;

#[cfg(with_testing)]
impl DynamoDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let store_config = create_dynamo_db_test_config().await;
        let namespace = generate_test_namespace();
        DynamoDbStorage::new_for_testing(store_config, &namespace, wasm_runtime, TestClock::new())
            .await
            .expect("storage")
    }

    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, DynamoDbStoreError> {
        let storage =
            DbStorageInner::<DynamoDbStore>::new_for_testing(store_config, namespace, wasm_runtime)
                .await?;
        Ok(Self::create(storage, clock))
    }
}
