// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::GenesisConfig;
use anyhow::format_err;
use clap::arg_enum;
use linera_storage::{InMemoryStoreClient, RocksdbStoreClient, S3Storage, Storage};
use std::{path::PathBuf, str::FromStr};

/// The description of a storage implementation.
#[derive(Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub enum StorageConfig {
    InMemory,
    Rocksdb { path: PathBuf },
    S3 { config: S3Config },
}

arg_enum! {

#[derive(Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub enum S3Config {
    Env,
    LocalStack,
}

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
            S3 { config } => {
                let client = match config {
                    S3Config::Env => S3Storage::new().await?,
                    S3Config::LocalStack => {
                        let localstack = linera_storage::LocalStackTestContext::new().await?;
                        let config = localstack.config();
                        S3Storage::from_config(config).await?
                    }
                };
                Box::new(client)
            }
        };
        Ok(client)
    }
}

const MEMORY: &str = "memory";
const ROCKSDB: &str = "rocksdb:";
const S3: &str = "s3:";

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
        if let Some(s) = input.strip_prefix(S3) {
            return Ok(Self::S3 {
                config: s.parse().map_err(|s| format_err!("{}", s))?,
            });
        }
        Err(format_err!("Incorrect storage description"))
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
    assert_eq!(
        StorageConfig::from_str("s3:localstack").unwrap(),
        StorageConfig::S3 {
            config: S3Config::LocalStack
        }
    );
    assert!(StorageConfig::from_str("memory_").is_err());
    assert!(StorageConfig::from_str("rocksdb_foo.db").is_err());
}
