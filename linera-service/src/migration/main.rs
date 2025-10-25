// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use linera_service::{
    storage::{CommonStorageOptions, RunnableWithStore, StorageConfig},
};
use linera_storage::{DbStorage, WallClock};
use linera_views::store::{KeyValueDatabase, KeyValueStore};

/// Options for running the proxy.
#[derive(clap::Parser, Debug, Clone)]
#[command(
    name = "Linera Migration",
    about = "A migration tool for the storage",
    version = linera_version::VersionInfo::default_clap_str(),
)]
pub struct MigrationOptions {
    /// Storage configuration for the blockchain history, chain states and binary blobs.
    #[arg(long = "storage")]
    storage_config: StorageConfig,

    /// Common storage options.
    #[command(flatten)]
    common_storage_options: CommonStorageOptions,
}

fn main() -> Result<()> {
    let options = <MigrationOptions as clap::Parser>::parse();

    let mut runtime = tokio::runtime::Builder::new_current_thread();

    runtime.enable_all().build()?.block_on(options.run())
}

struct InitialMigration;

#[async_trait]
impl RunnableWithStore for InitialMigration {
    type Output = ();

    async fn run<D>(
        self,
        config: D::Config,
        namespace: String,
    ) -> Result<Self::Output, anyhow::Error>
    where
        D: KeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        if D::exists(&config, &namespace).await? {
            let wasm_runtime = None;
            let storage =
                DbStorage::<D, WallClock>::connect(&config, &namespace, wasm_runtime).await?;
            storage.migrate_if_needed(false).await?;
        }
        Ok(())
    }
}

impl MigrationOptions {
    async fn run(&self) -> Result<()> {
        let store_config = self
            .storage_config
            .add_common_storage_options(&self.common_storage_options)?;
        store_config
            .run_with_store(InitialMigration)
            .await?;
        Ok(())
    }
}
