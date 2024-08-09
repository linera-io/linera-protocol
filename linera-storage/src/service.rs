// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage_service::client::ServiceStoreClient;
#[cfg(with_testing)]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_storage_service::{
        client::service_config_from_endpoint,
        common::{ServiceStoreConfig, ServiceStoreError},
    },
    linera_views::test_utils::generate_test_namespace,
};

use crate::db_storage::DbStorage;

pub type ServiceStorage<C> = DbStorage<ServiceStoreClient, C>;

#[cfg(with_testing)]
impl ServiceStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let endpoint = "127.0.0.1:8942";
        let store_config = service_config_from_endpoint(endpoint).expect("store_config");
        let namespace = generate_test_namespace();
        let root_key = &[];
        ServiceStorage::new_for_testing(
            store_config,
            &namespace,
            root_key,
            wasm_runtime,
            TestClock::new(),
        )
        .await
        .expect("storage")
    }

    pub async fn new_for_testing(
        store_config: ServiceStoreConfig,
        namespace: &str,
        root_key: &[u8],
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, ServiceStoreError> {
        let storage = DbStorageInner::<ServiceStoreClient>::new_for_testing(
            store_config,
            namespace,
            root_key,
            wasm_runtime,
        )
        .await?;
        Ok(Self::create(storage, clock))
    }
}
