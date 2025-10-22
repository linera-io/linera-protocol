// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_service::storage::{CommonStorageOptions, StorageMigration, StorageConfig};

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

impl MigrationOptions {
    async fn run(&self) -> Result<()> {
        let store_config = self
            .storage_config
            .add_common_storage_options(&self.common_storage_options)?;
        store_config.run_with_store(StorageMigration).await?;
        Ok(())
    }
}
