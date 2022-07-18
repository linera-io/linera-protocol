// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::GenesisConfig;
use linera_storage::{InMemoryStoreClient, RocksdbStoreClient, Storage};
use std::{path::PathBuf, str::FromStr};

/// The description of a storage implementation.
#[derive(Debug, PartialEq, Eq)]
pub enum StorageConfig {
    InMemory,
    Rocksdb { path: PathBuf },
}

pub type MixedStorage = Box<dyn Storage>;

impl StorageConfig {
    pub async fn make_storage(
        &self,
        config: &GenesisConfig,
    ) -> Result<MixedStorage, anyhow::Error> {
        use StorageConfig::*;
        let client: MixedStorage = match self {
            InMemory => {
                let mut client = InMemoryStoreClient::default();
                config.initialize_store(&mut client).await?;
                Box::new(client)
            }
            Rocksdb { path } if path.is_dir() => {
                log::warn!("Using existing database {:?}", path);
                let client = RocksdbStoreClient::new(path.clone(), 10000)?;
                Box::new(client)
            }
            Rocksdb { path } => {
                std::fs::create_dir_all(path)?;
                let mut client = RocksdbStoreClient::new(path.clone(), 10000)?;
                config.initialize_store(&mut client).await?;
                Box::new(client)
            }
        };
        Ok(client)
    }
}

const MEMORY: &str = "memory";
const ROCKSDB: &str = "rocksdb:";

impl FromStr for StorageConfig {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input == MEMORY {
            return Ok(Self::InMemory);
        }
        if let Some(s) = input.strip_prefix(ROCKSDB) {
            return Ok(Self::Rocksdb {
                path: s.to_string().into(),
            });
        }
        Err(anyhow::format_err!("Incorrect storage description"))
    }
}

#[test]
fn test_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("memory").unwrap(),
        StorageConfig::InMemory
    );
    assert_eq!(
        StorageConfig::from_str("rocksdb:foo.db").unwrap(),
        StorageConfig::Rocksdb {
            path: "foo.db".into()
        }
    );
    assert!(StorageConfig::from_str("memory_").is_err());
    assert!(StorageConfig::from_str("rocksdb_foo.db").is_err());
}
