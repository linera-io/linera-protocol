// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::dynamo_db::DynamoDbStore;
#[cfg(with_testing)]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_views::{
        dynamo_db::{DynamoDbStoreConfig, DynamoDbStoreError},
        common::{AdminKeyValueStore as _},
        test_utils::generate_test_namespace,
    },
};

use crate::db_storage::DbStorage;

pub type DynamoDbStorage<C> = DbStorage<DynamoDbStore, C>;

#[cfg(with_testing)]
impl DynamoDbStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let config = DynamoDbStore::get_test_config().await.expect("config");
        let namespace = generate_test_namespace();
        let root_key = &[];
        DynamoDbStorage::new_for_testing(
            config,
            &namespace,
            root_key,
            wasm_runtime,
            TestClock::new(),
        )
        .await
        .expect("storage")
    }

    pub async fn new_for_testing(
        store_config: DynamoDbStoreConfig,
        namespace: &str,
        root_key: &[u8],
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, DynamoDbStoreError> {
        let storage = DbStorageInner::<DynamoDbStore>::new_for_testing(
            store_config,
            namespace,
            root_key,
            wasm_runtime,
        )
        .await?;
        Ok(Self::create(storage, clock))
    }
}
