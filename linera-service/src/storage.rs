// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::GenesisConfig;
use anyhow::{bail, format_err};
use async_trait::async_trait;
use linera_execution::WasmRuntime;
use linera_storage::{MemoryStorage, Storage, WallClock};
use linera_views::{common::CommonStoreConfig, memory::MemoryStoreConfig, views::ViewError};
use std::str::FromStr;
use tracing::error;

#[cfg(feature = "rocksdb")]
use {
    linera_storage::RocksDbStorage,
    linera_views::rocks_db::{RocksDbStore, RocksDbStoreConfig},
    std::path::PathBuf,
};

#[cfg(feature = "aws")]
use {
    linera_storage::DynamoDbStorage,
    linera_views::dynamo_db::{get_config, DynamoDbStore, DynamoDbStoreConfig, TableName},
};

#[cfg(feature = "scylladb")]
use {
    anyhow::Context,
    linera_storage::ScyllaDbStorage,
    linera_views::scylla_db::{ScyllaDbStore, ScyllaDbStoreConfig},
    std::num::NonZeroU16,
    tracing::debug,
};

/// The configuration of the key value store in use.
#[allow(clippy::large_enum_variant)]
pub enum StoreConfig {
    /// The memory key value store
    Memory(MemoryStoreConfig),
    /// The RocksDb key value store
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbStoreConfig),
    /// The DynamoDb key value store
    #[cfg(feature = "aws")]
    DynamoDb(DynamoDbStoreConfig),
    /// The ScyllaDb key value store
    #[cfg(feature = "scylladb")]
    ScyllaDb(ScyllaDbStoreConfig),
}

/// The description of a storage implementation.
#[derive(Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub enum StorageConfig {
    /// The memory description
    Memory,
    /// The RocksDb description
    #[cfg(feature = "rocksdb")]
    RocksDb {
        /// The path used
        path: PathBuf,
    },
    /// The DynamoDb description
    #[cfg(feature = "aws")]
    DynamoDb {
        /// The table name used
        table: TableName,
        /// Whether to use the localstack system
        use_localstack: bool,
    },
    /// The ScyllaDb description
    #[cfg(feature = "scylladb")]
    ScyllaDb {
        /// The URI for accessing the database
        uri: String,
        /// The table name
        table_name: String,
    },
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
            let mut uri: Option<String> = None;
            let mut table_name: Option<String> = None;
            let parse_error: &'static str = "Correct format is tcp:db_hostname:port.";
            if !s.is_empty() {
                let mut parts = s.split(':');
                while let Some(part) = parts.next() {
                    match part {
                        "tcp" => {
                            let address = parts.next().with_context(|| {
                                format!("Failed to find address for {}. {}", s, parse_error)
                            })?;
                            let port_str = parts.next().with_context(|| {
                                format!("Failed to find port for {}. {}", s, parse_error)
                            })?;
                            let port = NonZeroU16::from_str(port_str).with_context(|| {
                                format!(
                                    "Failed to find parse port {} for {}. {}",
                                    port_str, s, parse_error
                                )
                            })?;
                            if uri.is_some() {
                                bail!("The uri has already been assigned");
                            }
                            uri = Some(format!("{}:{}", &address, port));
                        }
                        _ if part.starts_with("table") => {
                            anyhow::ensure!(
                                table_name.is_none(),
                                "The table_name has already been assigned"
                            );
                            table_name = Some(part.to_string());
                        }
                        _ => {
                            bail!("the entry \"{}\" is not matching", part);
                        }
                    }
                }
            }
            let uri = uri.unwrap_or("localhost:9042".to_string());
            let table_name = table_name.unwrap_or("table_storage".to_string());
            let db = Self::ScyllaDb { uri, table_name };
            debug!("ScyllaDB connection info: {:?}", db);
            return Ok(db);
        }
        error!("available storage: memory");
        #[cfg(feature = "rocksdb")]
        error!("Also available is RocksDB");
        #[cfg(feature = "aws")]
        error!("Also available is DynamoDB");
        #[cfg(feature = "scylladb")]
        error!("Also available is ScyllaDB");
        Err(format_err!("The input has not matched: {}", input))
    }
}

impl StorageConfig {
    /// The addition of the common config to get a full configuration
    pub async fn add_common_config(
        &self,
        common_config: CommonStoreConfig,
    ) -> Result<StoreConfig, anyhow::Error> {
        match self {
            StorageConfig::Memory => {
                let config = MemoryStoreConfig { common_config };
                Ok(StoreConfig::Memory(config))
            }
            #[cfg(feature = "rocksdb")]
            StorageConfig::RocksDb { path } => {
                let path_buf = path.to_path_buf();
                let config = RocksDbStoreConfig {
                    path_buf,
                    common_config,
                };
                Ok(StoreConfig::RocksDb(config))
            }
            #[cfg(feature = "aws")]
            StorageConfig::DynamoDb {
                table,
                use_localstack,
            } => {
                let aws_config = get_config(*use_localstack).await?;
                let config = DynamoDbStoreConfig {
                    config: aws_config,
                    table_name: table.clone(),
                    common_config,
                };
                Ok(StoreConfig::DynamoDb(config))
            }
            #[cfg(feature = "scylladb")]
            StorageConfig::ScyllaDb { uri, table_name } => {
                let config = ScyllaDbStoreConfig {
                    uri: uri.to_string(),
                    table_name: table_name.to_string(),
                    common_config,
                };
                Ok(StoreConfig::ScyllaDb(config))
            }
        }
    }
}

impl StoreConfig {
    /// Deletes all the entries in the database
    pub async fn delete_all(self) -> Result<(), ViewError> {
        match self {
            StoreConfig::Memory(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "delete_all does not make sense for memory storage".to_string(),
            }),
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config) => {
                RocksDbStore::delete_all(config).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config) => {
                DynamoDbStore::delete_all(config).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config) => {
                ScyllaDbStore::delete_all(config).await?;
                Ok(())
            }
        }
    }

    /// Deletes only one table of the database
    pub async fn delete_single(self) -> Result<(), ViewError> {
        match self {
            StoreConfig::Memory(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "delete_single does not make sense for memory storage".to_string(),
            }),
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config) => {
                RocksDbStore::delete_single(config).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config) => {
                DynamoDbStore::delete_single(config).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config) => {
                ScyllaDbStore::delete_single(config).await?;
                Ok(())
            }
        }
    }

    /// Test existence of one table in the database
    pub async fn test_existence(self) -> Result<bool, ViewError> {
        match self {
            StoreConfig::Memory(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "existence not make sense for memory storage".to_string(),
            }),
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config) => Ok(RocksDbStore::test_existence(config).await?),
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config) => Ok(DynamoDbStore::test_existence(config).await?),
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config) => Ok(ScyllaDbStore::test_existence(config).await?),
        }
    }

    /// Deletes only one table of the database
    pub async fn initialize(self) -> Result<(), ViewError> {
        match self {
            StoreConfig::Memory(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "delete_single does not make sense for memory storage".to_string(),
            }),
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config) => {
                RocksDbStore::initialize(config).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config) => {
                DynamoDbStore::initialize(config).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config) => {
                ScyllaDbStore::initialize(config).await?;
                Ok(())
            }
        }
    }

    /// List all the tables of the database
    pub async fn list_tables(self) -> Result<Vec<String>, ViewError> {
        match self {
            StoreConfig::Memory(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "list_tables is not supported for the memory storage".to_string(),
            }),
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "list_tables is not currently supported for the RocksDb storage".to_string(),
            }),
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config) => {
                let tables = DynamoDbStore::list_tables(config).await?;
                Ok(tables)
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config) => {
                let tables = ScyllaDbStore::list_tables(config).await?;
                Ok(tables)
            }
        }
    }
}

#[async_trait]
pub trait Runnable {
    type Output;

    async fn run<S>(self, storage: S) -> Result<Self::Output, anyhow::Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>;
}

// The design is that the initialization of the accounts should be separate
// from the running of the database.
// However, that does not apply to the memory storage which must be initialized
// in the same context in which it is used.
#[allow(unused_variables)]
pub async fn run_with_storage<Job>(
    config: StoreConfig,
    genesis_config: &GenesisConfig,
    wasm_runtime: Option<WasmRuntime>,
    job: Job,
) -> Result<Job::Output, anyhow::Error>
where
    Job: Runnable,
{
    match config {
        StoreConfig::Memory(config) => {
            let mut storage = MemoryStorage::new(
                wasm_runtime,
                config.common_config.max_stream_queries,
                WallClock,
            );
            genesis_config.initialize_storage(&mut storage).await?;
            job.run(storage).await
        }
        #[cfg(feature = "rocksdb")]
        StoreConfig::RocksDb(config) => {
            let (storage, table_status) = RocksDbStorage::new(config, wasm_runtime).await?;
            job.run(storage).await
        }
        #[cfg(feature = "aws")]
        StoreConfig::DynamoDb(config) => {
            let (storage, table_status) = DynamoDbStorage::new(config, wasm_runtime).await?;
            job.run(storage).await
        }
        #[cfg(feature = "scylladb")]
        StoreConfig::ScyllaDb(config) => {
            let (storage, table_status) = ScyllaDbStorage::new(config, wasm_runtime).await?;
            job.run(storage).await
        }
    }
}

#[allow(unused_variables)]
pub async fn full_initialize_storage(
    config: StoreConfig,
    genesis_config: &GenesisConfig,
) -> Result<(), anyhow::Error> {
    match config {
        StoreConfig::Memory(_) => {
            bail!("The initialization should not be called for memory");
        }
        #[cfg(feature = "rocksdb")]
        StoreConfig::RocksDb(config) => {
            let wasm_runtime = None;
            let mut storage = RocksDbStorage::initialize(config, wasm_runtime).await?;
            genesis_config.initialize_storage(&mut storage).await
        }
        #[cfg(feature = "aws")]
        StoreConfig::DynamoDb(config) => {
            let wasm_runtime = None;
            let mut storage = DynamoDbStorage::initialize(config, wasm_runtime).await?;
            genesis_config.initialize_storage(&mut storage).await
        }
        #[cfg(feature = "scylladb")]
        StoreConfig::ScyllaDb(config) => {
            let wasm_runtime = None;
            let mut storage = ScyllaDbStorage::initialize(config, wasm_runtime).await?;
            genesis_config.initialize_storage(&mut storage).await
        }
    }
}

#[allow(unused_variables)]
pub async fn test_existence_storage(config: StoreConfig) -> Result<bool, anyhow::Error> {
    match config {
        StoreConfig::Memory(_) => {
            bail!("The initialization should not be called for memory");
        }
        #[cfg(feature = "rocksdb")]
        StoreConfig::RocksDb(config) => Ok(RocksDbStore::test_existence(config).await?),
        #[cfg(feature = "aws")]
        StoreConfig::DynamoDb(config) => Ok(DynamoDbStore::test_existence(config).await?),
        #[cfg(feature = "scylladb")]
        StoreConfig::ScyllaDb(config) => Ok(ScyllaDbStore::test_existence(config).await?),
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
            path: "foo.db".into(),
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
            uri: "localhost:9042".to_string(),
            table_name: "table_storage".to_string(),
        }
    );
    assert_eq!(
        StorageConfig::from_str("scylladb:tcp:db_hostname:230:table_other_storage").unwrap(),
        StorageConfig::ScyllaDb {
            uri: "db_hostname:230".to_string(),
            table_name: "table_other_storage".to_string(),
        }
    );
    assert_eq!(
        StorageConfig::from_str("scylladb:tcp:db_hostname:230").unwrap(),
        StorageConfig::ScyllaDb {
            uri: "db_hostname:230".to_string(),
            table_name: "table_storage".to_string(),
        }
    );
    assert!(StorageConfig::from_str("scylladb:-10").is_err());
    assert!(StorageConfig::from_str("scylladb:70000").is_err());
    assert!(StorageConfig::from_str("scylladb:230:234").is_err());
    assert!(StorageConfig::from_str("scylladb:tcp:address1").is_err());
    assert!(StorageConfig::from_str("scylladb:tcp:address1:tcp:/address2").is_err());
    assert!(StorageConfig::from_str("scylladb:wrong").is_err());
}
