// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::GenesisConfig;
use anyhow::format_err;
use async_trait::async_trait;
use linera_storage2::{DynamoDbStoreClient, MemoryStoreClient, RocksdbStoreClient};
use linera_views::dynamo_db::{TableName, TableStatus};
use std::{path::PathBuf, str::FromStr};

/// The description of a storage implementation.
#[derive(Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub enum StorageConfig {
    Memory,
    Rocksdb {
        path: PathBuf,
    },
    DynamoDb {
        table: TableName,
        use_localstack: bool,
    },
}

#[async_trait]
pub trait Runnable<S> {
    type Output;

    async fn run(self, storage: S) -> Result<Self::Output, anyhow::Error>;
}

impl StorageConfig {
    pub async fn run_with_storage<Job, Output>(
        &self,
        config: &GenesisConfig,
        job: Job,
    ) -> Result<Output, anyhow::Error>
    where
        Job: Runnable<MemoryStoreClient, Output = Output>
            + Runnable<RocksdbStoreClient, Output = Output>
            + Runnable<DynamoDbStoreClient, Output = Output>,
    {
        use StorageConfig::*;
        match self {
            Memory => {
                let mut client = MemoryStoreClient::default();
                config.initialize_store(&mut client).await?;
                job.run(client).await
            }
            Rocksdb { path } if path.is_dir() => {
                log::warn!("Using existing database {:?}", path);
                let client = RocksdbStoreClient::new(path.clone());
                job.run(client).await
            }
            Rocksdb { path } => {
                std::fs::create_dir_all(path)?;
                let mut client = RocksdbStoreClient::new(path.clone());
                config.initialize_store(&mut client).await?;
                job.run(client).await
            }
            DynamoDb {
                table,
                use_localstack,
            } => {
                let (mut client, table_status) = match use_localstack {
                    true => DynamoDbStoreClient::with_localstack(table.clone()).await?,
                    false => DynamoDbStoreClient::new(table.clone()).await?,
                };
                if table_status == TableStatus::New {
                    config.initialize_store(&mut client).await?;
                }
                job.run(client).await
            }
        }
    }
}

const MEMORY: &str = "memory";
const ROCKSDB: &str = "rocksdb:";
const DYNAMO_DB: &str = "dynamodb:";

impl FromStr for StorageConfig {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input == MEMORY {
            return Ok(Self::Memory);
        }
        if let Some(s) = input.strip_prefix(ROCKSDB) {
            return Ok(Self::Rocksdb {
                path: s.to_string().into(),
            });
        }
        if let Some(s) = input.strip_prefix(DYNAMO_DB) {
            let mut parts = s.splitn(2, ':');
            let table = parts
                .next()
                .ok_or_else(|| format_err!("Missing DynamoDB table name, e.g. {DYNAMO_DB}TABLE"))?
                .parse()?;
            let use_localstack = match parts.next() {
                None | Some("env") => false,
                Some("localstack") => true,
                Some(unknown) => {
                    return Err(format_err!(
                        "Invalid DynamoDB endpoint {unknown:?}. \
                        Expected {DYNAMO_DB}TABLE:[env|localstack]"
                    ));
                }
            };
            return Ok(Self::DynamoDb {
                table,
                use_localstack,
            });
        }
        Err(format_err!("Incorrect storage description"))
    }
}

#[test]
fn test_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("memory").unwrap(),
        StorageConfig::Memory
    );
    assert_eq!(
        StorageConfig::from_str("rocksdb:foo.db").unwrap(),
        StorageConfig::Rocksdb {
            path: "foo.db".into()
        }
    );
    assert_eq!(
        StorageConfig::from_str("dynamodb:table").unwrap(),
        StorageConfig::DynamoDb {
            table: "table".parse().unwrap(),
            use_localstack: false,
        }
    );
    assert_eq!(
        StorageConfig::from_str("dynamodb:table:env").unwrap(),
        StorageConfig::DynamoDb {
            table: "table".parse().unwrap(),
            use_localstack: false,
        }
    );
    assert_eq!(
        StorageConfig::from_str("dynamodb:table:localstack").unwrap(),
        StorageConfig::DynamoDb {
            table: "table".parse().unwrap(),
            use_localstack: true,
        }
    );
    assert!(StorageConfig::from_str("memory_").is_err());
    assert!(StorageConfig::from_str("rocksdb_foo.db").is_err());
    assert!(StorageConfig::from_str("dynamodb").is_err());
    assert!(StorageConfig::from_str("dynamodb:").is_err());
    assert!(StorageConfig::from_str("dynamodb:1").is_err());
    assert!(StorageConfig::from_str("dynamodb:wrong:endpoint").is_err());
}
