// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_client::config::GenesisConfig;
use linera_storage::DbStorage;
pub use linera_storage::{StorageCacheConfig, DEFAULT_CLEANUP_INTERVAL_SECS};
pub use linera_storage_runtime::{
    AssertStorageV1, CommonStorageOptions, InnerStorageConfig, Runnable, RunnableWithStore,
    StorageConfig, StorageMigration, StoreConfig,
};
use linera_views::store::{KeyValueDatabase, KeyValueStore};

struct InitializeStorageJob<'a>(&'a GenesisConfig);

#[async_trait]
impl RunnableWithStore for InitializeStorageJob<'_> {
    type Output = ();

    async fn run<D>(
        self,
        config: D::Config,
        namespace: String,
        cache_sizes: StorageCacheConfig,
    ) -> Result<Self::Output, anyhow::Error>
    where
        D: KeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        let mut storage =
            DbStorage::<D, _>::maybe_create_and_connect(&config, &namespace, None, cache_sizes)
                .await?;
        self.0.initialize_storage(&mut storage).await?;
        Ok(())
    }
}

/// Initializes storage by running migration and then writing the genesis config.
pub async fn initialize(
    store_config: StoreConfig,
    cache_sizes: StorageCacheConfig,
    config: &GenesisConfig,
) -> Result<(), anyhow::Error> {
    store_config
        .clone()
        .run_with_store(cache_sizes, StorageMigration)
        .await?;
    store_config
        .run_with_store(cache_sizes, InitializeStorageJob(config))
        .await
}
