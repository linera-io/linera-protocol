// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::GenesisConfig;
use anyhow::format_err;
use async_trait::async_trait;
use linera_execution::WasmRuntime;
use linera_storage::MemoryStoreClient;
use std::str::FromStr;

#[cfg(feature = "rocksdb")]
use {linera_storage::RocksDbStoreClient, std::path::PathBuf};

#[cfg(feature = "aws")]
use {
    linera_storage::DynamoDbStoreClient,
    linera_views::dynamo_db::{TableName, TableStatus},
};

#[cfg(feature = "scylladb")]
use {anyhow::bail, linera_storage::ScyllaDbStoreClient};

/// The description of a storage implementation.
#[derive(Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub enum StorageConfig {
    Memory,
    #[cfg(feature = "rocksdb")]
    RocksDb {
        path: PathBuf,
    },
    #[cfg(feature = "aws")]
    DynamoDb {
        table: TableName,
        use_localstack: bool,
    },
    #[cfg(feature = "scylladb")]
    ScyllaDb {
        restart_database: bool,
        uri: String,
        table_name: String,
    },
}

#[async_trait]
pub trait Runnable<S> {
    type Output;

    async fn run(self, storage: S) -> Result<Self::Output, anyhow::Error>;
}

#[doc(hidden)]
#[cfg(feature = "rocksdb")]
pub type MaybeRocksDbStoreClient = RocksDbStoreClient;

#[doc(hidden)]
#[cfg(not(feature = "rocksdb"))]
pub type MaybeRocksDbStoreClient = MemoryStoreClient;

#[doc(hidden)]
#[cfg(feature = "aws")]
pub type MaybeDynamoDbStoreClient = DynamoDbStoreClient;

#[doc(hidden)]
#[cfg(not(feature = "aws"))]
pub type MaybeDynamoDbStoreClient = MemoryStoreClient;

#[doc(hidden)]
#[cfg(feature = "scylladb")]
pub type MaybeScyllaDbStoreClient = ScyllaDbStoreClient;

#[doc(hidden)]
#[cfg(not(feature = "scylladb"))]
pub type MaybeScyllaDbStoreClient = MemoryStoreClient;

pub trait RunnableJob<Output>:
    Runnable<MemoryStoreClient, Output = Output>
    + Runnable<MaybeRocksDbStoreClient, Output = Output>
    + Runnable<MaybeDynamoDbStoreClient, Output = Output>
    + Runnable<MaybeScyllaDbStoreClient, Output = Output>
{
}

impl<Output, T> RunnableJob<Output> for T where
    T: Runnable<MemoryStoreClient, Output = Output>
        + Runnable<MaybeRocksDbStoreClient, Output = Output>
        + Runnable<MaybeDynamoDbStoreClient, Output = Output>
        + Runnable<MaybeScyllaDbStoreClient, Output = Output>
{
}

impl StorageConfig {
    #[allow(unused_variables)]
    pub async fn run_with_storage<Job, Output>(
        &self,
        config: &GenesisConfig,
        wasm_runtime: Option<WasmRuntime>,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
        job: Job,
    ) -> Result<Output, anyhow::Error>
    where
        Job: RunnableJob<Output>,
    {
        use StorageConfig::*;
        match self {
            Memory => {
                let mut client = MemoryStoreClient::new(wasm_runtime, max_stream_queries);
                config.initialize_store(&mut client).await?;
                job.run(client).await
            }
            #[cfg(feature = "rocksdb")]
            RocksDb { path } => {
                let is_new_dir = if path.is_dir() {
                    tracing::warn!("Using existing database {:?}", path);
                    false
                } else {
                    std::fs::create_dir_all(path)?;
                    true
                };

                let mut client = RocksDbStoreClient::new(
                    path.clone(),
                    wasm_runtime,
                    max_stream_queries,
                    cache_size,
                );
                if is_new_dir {
                    config.initialize_store(&mut client).await?;
                }
                job.run(client).await
            }
            #[cfg(feature = "aws")]
            DynamoDb {
                table,
                use_localstack,
            } => {
                let (mut client, table_status) = match use_localstack {
                    true => {
                        DynamoDbStoreClient::with_localstack(
                            table.clone(),
                            max_concurrent_queries,
                            max_stream_queries,
                            cache_size,
                            wasm_runtime,
                        )
                        .await?
                    }
                    false => {
                        DynamoDbStoreClient::new(
                            table.clone(),
                            max_concurrent_queries,
                            max_stream_queries,
                            cache_size,
                            wasm_runtime,
                        )
                        .await?
                    }
                };
                if table_status == TableStatus::New {
                    config.initialize_store(&mut client).await?;
                }
                job.run(client).await
            }
            #[cfg(feature = "scylladb")]
            ScyllaDb {
                restart_database,
                uri,
                table_name,
            } => {
                let mut client = ScyllaDbStoreClient::new(
                    *restart_database,
                    uri,
                    table_name.to_string(),
                    max_concurrent_queries,
                    max_stream_queries,
                    cache_size,
                    wasm_runtime,
                )
                .await?;
                if *restart_database {
                    config.initialize_store(&mut client).await?;
                }
                job.run(client).await
            }
        }
    }
}

const MEMORY: &str = "memory";
#[cfg(feature = "rocksdb")]
const ROCKS_DB: &str = "rocksdb:";
#[cfg(feature = "aws")]
const DYNAMO_DB: &str = "dynamodb:";
#[cfg(feature = "scylladb")]
const SCYLLA_DB: &str = "scylladb:";

impl FromStr for StorageConfig {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input == MEMORY {
            return Ok(Self::Memory);
        }
        #[cfg(feature = "rocksdb")]
        if let Some(s) = input.strip_prefix(ROCKS_DB) {
            return Ok(Self::RocksDb {
                path: s.to_string().into(),
            });
        }
        #[cfg(feature = "aws")]
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
        #[cfg(feature = "scylladb")]
        if let Some(s) = input.strip_prefix(SCYLLA_DB) {
            let mut restart_database: Option<bool> = None;
            let mut uri: Option<String> = None;
            let mut table_name: Option<String> = None;
            if !s.is_empty() {
                let mut parts = s.split(':');
                while let Some(part_ent) = parts.next() {
                    match part_ent {
                        "https" => {
                            let err_msg = "Correct format is https:://my.validator.com:port";
                            let Some(empty) = parts.next() else {
                                bail!(err_msg);
                            };
                            if !empty.is_empty() {
                                bail!(err_msg);
                            }
                            let Some(address) = parts.next() else {
                                bail!(err_msg);
                            };
                            let Some(port_str) = parts.next() else {
                                bail!(err_msg);
                            };
                            let Ok(_num_port) = port_str.parse::<u16>() else {
                                bail!(err_msg);
                            };
                            if uri.is_some() {
                                bail!("The uri has already been assigned");
                            }
                            uri = Some(format!("https::{}:{}", address, port_str));
                        }
                        _ if part_ent.starts_with("table") => {
                            if table_name.is_some() {
                                bail!("The table_name has already been assigned");
                            }
                            table_name = Some(part_ent.to_string());
                        }
                        "true" => {
                            if restart_database.is_some() {
                                bail!("The restart_database entry has already been assigned");
                            }
                            restart_database = Some(true);
                        }
                        "false" => {
                            if restart_database.is_some() {
                                bail!("The restart_database entry has already been assigned");
                            }
                            restart_database = Some(false);
                        }
                        _ => {
                            bail!("the entry \"{}\" is not matching", part_ent);
                        }
                    }
                }
            }
            let restart_database = restart_database.unwrap_or(true);
            let uri = uri.unwrap_or("localhost:9042".to_string());
            let table_name = table_name.unwrap_or("table_storage".to_string());
            return Ok(Self::ScyllaDb {
                restart_database,
                uri,
                table_name,
            });
        }
        Err(format_err!("Incorrect storage description: {}", input))
    }
}

#[test]
fn test_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("memory").unwrap(),
        StorageConfig::Memory
    );
    assert!(StorageConfig::from_str("memory_").is_err());
}

#[cfg(feature = "rocksdb")]
#[test]
fn test_rocks_db_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("rocksdb:foo.db").unwrap(),
        StorageConfig::RocksDb {
            path: "foo.db".into()
        }
    );
    assert!(StorageConfig::from_str("rocksdb_foo.db").is_err());
}

#[cfg(feature = "aws")]
#[test]
fn test_aws_storage_config_from_str() {
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
    assert!(StorageConfig::from_str("dynamodb").is_err());
    assert!(StorageConfig::from_str("dynamodb:").is_err());
    assert!(StorageConfig::from_str("dynamodb:1").is_err());
    assert!(StorageConfig::from_str("dynamodb:wrong:endpoint").is_err());
}

#[cfg(feature = "scylladb")]
#[test]
fn test_scylla_db_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("scylladb:").unwrap(),
        StorageConfig::ScyllaDb {
            restart_database: true,
            uri: "localhost:9042".to_string(),
            table_name: "table_storage".to_string(),
        }
    );
    assert_eq!(
        StorageConfig::from_str("scylladb:false").unwrap(),
        StorageConfig::ScyllaDb {
            restart_database: false,
            uri: "localhost:9042".to_string(),
            table_name: "table_storage".to_string(),
        }
    );
    assert_eq!(
        StorageConfig::from_str("scylladb:https:://my.validator.com:230").unwrap(),
        StorageConfig::ScyllaDb {
            restart_database: true,
            uri: "https:://my.validator.com:230".to_string(),
            table_name: "table_storage".to_string(),
        }
    );
    assert!(StorageConfig::from_str("scylladb:-10").is_err());
    assert!(StorageConfig::from_str("scylladb:70000").is_err());
    assert!(StorageConfig::from_str("scylladb:230:234").is_err());
    assert!(StorageConfig::from_str("scylladb:https:://address1").is_err());
    assert!(StorageConfig::from_str("scylladb:https:://address1:https::/address2").is_err());
    assert!(StorageConfig::from_str("scylladb:wrong").is_err());
}
