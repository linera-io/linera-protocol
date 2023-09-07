// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::format_err;
use linera_views::{common::CommonStoreConfig, memory::MemoryKvStoreConfig, views::ViewError};
use std::str::FromStr;
#[cfg(feature = "rocksdb")]
use {
    linera_views::rocks_db::{RocksDbClient, RocksDbKvStoreConfig},
    std::path::PathBuf,
};

#[cfg(feature = "aws")]
use linera_views::dynamo_db::{get_config, DynamoDbClient, DynamoDbKvStoreConfig, TableName};

#[cfg(feature = "scylladb")]
use {
    anyhow::bail,
    linera_views::scylla_db::{ScyllaDbClient, ScyllaDbKvStoreConfig},
};

/// The Full storage input to the constructor of the database client.
#[allow(clippy::large_enum_variant)]
pub enum FullStorageConfig {
    /// The memory key value store
    Memory(MemoryKvStoreConfig),
    /// The RocksDb key value store
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbKvStoreConfig),
    /// The DynamoDb key value store
    #[cfg(feature = "aws")]
    DynamoDb(DynamoDbKvStoreConfig),
    /// The ScyllaDb key value store
    #[cfg(feature = "scylladb")]
    ScyllaDb(ScyllaDbKvStoreConfig),
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
            if !s.is_empty() {
                let mut parts = s.split(':');
                while let Some(part) = parts.next() {
                    match part {
                        "https" => {
                            let err_msg = "Correct format is https:://db_hostname:port";
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
                        _ if part.starts_with("table") => {
                            if table_name.is_some() {
                                bail!("The table_name has already been assigned");
                            }
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
            return Ok(Self::ScyllaDb { uri, table_name });
        }
        print!("available storage: memory");
        #[cfg(feature = "rocksdb")]
        print!(", rocksdb");
        #[cfg(feature = "aws")]
        print!(", dynamodb");
        #[cfg(feature = "scylladb")]
        print!(", scyladb");
        println!();
        Err(format_err!("The input has not matched: {}", input))
    }
}

impl StorageConfig {
    /// The addition of the common config to get a full configuration
    pub async fn add_common_config(
        &self,
        common_config: CommonStoreConfig,
    ) -> Result<FullStorageConfig, anyhow::Error> {
        match self {
            StorageConfig::Memory => {
                let kv_config = MemoryKvStoreConfig { common_config };
                Ok(FullStorageConfig::Memory(kv_config))
            }
            #[cfg(feature = "rocksdb")]
            StorageConfig::RocksDb { path } => {
                let path_buf = path.to_path_buf();
                let kv_config = RocksDbKvStoreConfig {
                    path_buf,
                    common_config,
                };
                Ok(FullStorageConfig::RocksDb(kv_config))
            }
            #[cfg(feature = "aws")]
            StorageConfig::DynamoDb {
                table,
                use_localstack,
            } => {
                let aws_config = get_config(*use_localstack).await?;
                let kv_config = DynamoDbKvStoreConfig {
                    config: aws_config,
                    table_name: table.clone(),
                    common_config,
                };
                Ok(FullStorageConfig::DynamoDb(kv_config))
            }
            #[cfg(feature = "scylladb")]
            StorageConfig::ScyllaDb { uri, table_name } => {
                let kv_config = ScyllaDbKvStoreConfig {
                    uri: uri.to_string(),
                    table_name: table_name.to_string(),
                    common_config,
                };
                Ok(FullStorageConfig::ScyllaDb(kv_config))
            }
        }
    }
}

impl FullStorageConfig {
    /// Deletes all the entries in the database
    pub async fn delete_all(self) -> Result<(), ViewError> {
        match self {
            FullStorageConfig::Memory(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "delete_all does not make sense for memory storage".to_string(),
            }),
            #[cfg(feature = "rocksdb")]
            FullStorageConfig::RocksDb(kv_config) => {
                RocksDbClient::delete_all(kv_config).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            FullStorageConfig::DynamoDb(kv_config) => {
                DynamoDbClient::delete_all(kv_config).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            FullStorageConfig::ScyllaDb(kv_config) => {
                ScyllaDbClient::delete_all(kv_config).await?;
                Ok(())
            }
        }
    }

    /// Deletes only one table of the database
    pub async fn delete_single(self) -> Result<(), ViewError> {
        match self {
            FullStorageConfig::Memory(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "delete_single does not make sense for memory storage".to_string(),
            }),
            #[cfg(feature = "rocksdb")]
            FullStorageConfig::RocksDb(kv_config) => {
                RocksDbClient::delete_single(kv_config).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            FullStorageConfig::DynamoDb(kv_config) => {
                DynamoDbClient::delete_single(kv_config).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            FullStorageConfig::ScyllaDb(kv_config) => {
                ScyllaDbClient::delete_single(kv_config).await?;
                Ok(())
            }
        }
    }

    /// Deletes only one table of the database
    pub async fn initialize(self) -> Result<(), ViewError> {
        match self {
            FullStorageConfig::Memory(_) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "delete_single does not make sense for memory storage".to_string(),
            }),
            #[cfg(feature = "rocksdb")]
            FullStorageConfig::RocksDb(kv_config) => {
                RocksDbClient::initialize(kv_config).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            FullStorageConfig::DynamoDb(kv_config) => {
                DynamoDbClient::initialize(kv_config).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            FullStorageConfig::ScyllaDb(kv_config) => {
                ScyllaDbClient::initialize(kv_config).await?;
                Ok(())
            }
        }
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
        StorageConfig::from_str("scylladb:https:://db_hostname:230").unwrap(),
        StorageConfig::ScyllaDb {
            uri: "https:://db_hostname:230".to_string(),
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
