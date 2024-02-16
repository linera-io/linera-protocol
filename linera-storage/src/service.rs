// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::DbStorage;
use linera_storage_service::client::ServiceStoreClient;

#[cfg(any(test, feature = "test"))]
use {
    crate::db_storage::{DbStorageInner, TestClock},
    linera_execution::WasmRuntime,
    linera_storage_service::{
        client::service_config_from_endpoint,
        common::{ServiceContextError, ServiceStoreConfig},
    },
    linera_views::test_utils::generate_test_namespace,
};

pub type ServiceStorage<C> = DbStorage<ServiceStoreClient, C>;

#[cfg(any(test, feature = "test"))]
impl ServiceStorage<TestClock> {
    pub async fn make_test_storage(wasm_runtime: Option<WasmRuntime>) -> Self {
        let endpoint = "127.0.0.1:8942".to_string();
        let store_config = service_config_from_endpoint(endpoint)
            .await
            .expect("store_config");
        let namespace = generate_test_namespace();
        ServiceStorage::new_for_testing(store_config, &namespace, wasm_runtime, TestClock::new())
            .await
            .expect("storage")
    }

    pub async fn new_for_testing(
        store_config: ServiceStoreConfig,
        namespace: &str,
        wasm_runtime: Option<WasmRuntime>,
        clock: TestClock,
    ) -> Result<Self, ServiceContextError> {
        let storage = DbStorageInner::<ServiceStoreClient>::new_for_testing(
            store_config,
            namespace,
            wasm_runtime,
        )
        .await?;
        Ok(Self::create(storage, clock))
    }
}
