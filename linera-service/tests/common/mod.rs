// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::sync::Lazy;
use tokio::sync::Mutex;
use anyhow::Result;
use async_lock::RwLock;
use std::ops::Deref;
use linera_storage_service::child::StorageServiceGuard;
use linera_storage_service::child::StorageServiceBuilder;
use linera_storage_service::common::get_service_storage_binary;
use linera_storage_service::child::get_free_port;
use linera_storage_service::client::service_config_from_endpoint;
use linera_storage_service::common::ServiceStoreConfig;

#[cfg(feature = "rocksdb")]
use {
    linera_views::rocks_db::{create_rocks_db_test_config, RocksDbStoreConfig},
    tempfile::TempDir,
};

/// A static lock to prevent integration tests from running in parallel.
pub static INTEGRATION_TEST_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub struct IntegrationTestServerInternal {
    pub service_config: ServiceStoreConfig,
    _service_guard: StorageServiceGuard,
    #[cfg(feature = "rocksdb")]
    pub rocks_db_config: RocksDbStoreConfig,
    #[cfg(feature = "rocksdb")]
    _temp_dir: TempDir,
}

pub struct IntegrationTestServerConfig {
    pub service_config: ServiceStoreConfig,
    #[cfg(feature = "rocksdb")]
    pub rocks_db_config: RocksDbStoreConfig,
}

impl IntegrationTestServerInternal {
    async fn service_info() -> Result<(ServiceStoreConfig, StorageServiceGuard)> {
        let endpoint = get_free_port().await.unwrap();
        let service_config = service_config_from_endpoint(&endpoint)?;
        let binary = get_service_storage_binary().await?.display().to_string();
        let spanner = StorageServiceBuilder::new(&endpoint, binary);
        let service_guard = spanner.run_service().await?;
        Ok((service_config, service_guard))
    }

    #[cfg(feature = "rocksdb")]
    pub async fn new() -> Result<Self> {
        let (service_config, _service_guard) = Self::service_info().await?;
        let (rocks_db_config, _temp_dir) = create_rocks_db_test_config().await;
        Ok(Self { service_config, _service_guard, rocks_db_config, _temp_dir })
    }

    #[cfg(not(feature = "rocksdb"))]
    pub async fn new() -> Result<Self> {
        let (service_config, _service_guard) = Self::service_info().await?;
        Ok(Self { service_config, _service_guard })
    }

    #[cfg(feature = "rocksdb")]
    pub fn get_config(&self) -> IntegrationTestServerConfig {
        let service_config = self.service_config.clone();
        let rocks_db_config = self.rocks_db_config.clone();
        IntegrationTestServerConfig { service_config, rocks_db_config }
    }

    #[cfg(not(feature = "rocksdb"))]
    pub fn get_config(&self) -> IntegrationTestServerConfig {
        let service_config = self.service_config.clone();
        IntegrationTestServerConfig { service_config }
    }
}

pub struct IntegrationTestServer {
    pub internal_server: RwLock<Option<IntegrationTestServerInternal>>,
}

impl IntegrationTestServer {
    pub fn new() -> Self {
        Self { internal_server: RwLock::new(None) }
    }

    pub async fn get_config(&self) -> IntegrationTestServerConfig {
        let mut w = self.internal_server.write().await;
        if w.is_none() {
            *w = Some(IntegrationTestServerInternal::new().await.expect("server"));
        }
        let Some(internal_server) = w.deref() else {
            unreachable!();
        };
        internal_server.get_config()
    }
}



// A static data to store the integration test server
pub static INTEGRATION_TEST_SERVER: Lazy<IntegrationTestServer> = Lazy::new(|| IntegrationTestServer::new());
