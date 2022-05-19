// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::GenesisConfig;
use std::path::PathBuf;
use zef_storage::{InMemoryStoreClient, RocksdbStoreClient, Storage};

pub type MixedStorage = Box<dyn Storage>;

pub async fn make_storage(
    db_path: Option<&PathBuf>,
    config: &GenesisConfig,
) -> Result<MixedStorage, anyhow::Error> {
    let client: MixedStorage = match db_path {
        None => {
            let mut client = InMemoryStoreClient::default();
            config.initialize_store(&mut client).await?;
            Box::new(client)
        }
        Some(path) if path.is_dir() => {
            log::warn!("Using existing database {:?}", path);
            let client = RocksdbStoreClient::new(path.clone())?;
            Box::new(client)
        }
        Some(path) => {
            std::fs::create_dir_all(path)?;
            let mut client = RocksdbStoreClient::new(path.clone())?;
            config.initialize_store(&mut client).await?;
            Box::new(client)
        }
    };
    Ok(client)
}
