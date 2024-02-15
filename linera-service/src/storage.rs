// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::GenesisConfig;
use anyhow::{bail, format_err};
use async_trait::async_trait;
use linera_execution::WasmRuntime;
use linera_storage::{MemoryStorage, ServiceStorage, Storage};
use linera_storage_service::{client::ServiceStoreClient, common::ServiceStoreConfig};
use linera_views::{
    common::{AdminKeyValueStore, CommonStoreConfig},
    memory::MemoryStoreConfig,
    views::ViewError,
};
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
    linera_views::dynamo_db::{get_config, DynamoDbStore, DynamoDbStoreConfig},
};

#[cfg(feature = "scylladb")]
use {
    anyhow::Context,
    linera_storage::ScyllaDbStorage,
    linera_views::scylla_db::{ScyllaDbStore, ScyllaDbStoreConfig},
    std::num::NonZeroU16,
    tracing::debug,
};

const DEFAULT_NAMESPACE: &str = "table_linera";

/// The configuration of the key value store in use.
#[allow(clippy::large_enum_variant)]
pub enum StoreConfig {
    /// The storage service  key value store
    Service(ServiceStoreConfig, String),
    /// The memory key value store
    Memory(MemoryStoreConfig, String),
    /// The RocksDb key value store
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbStoreConfig, String),
    /// The DynamoDb key value store
    #[cfg(feature = "aws")]
    DynamoDb(DynamoDbStoreConfig, String),
    /// The ScyllaDb key value store
    #[cfg(feature = "scylladb")]
    ScyllaDb(ScyllaDbStoreConfig, String),
}

/// The description of a storage implementation.
#[derive(Clone, Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub enum StorageConfig {
    /// The storage service description
    Service {
        /// The endpoint used
        endpoint: String,
        /// The namespace used
        namespace: String,
    },
    /// The memory description
    Memory {
        /// The namespace
        namespace: String,
    },
    /// The RocksDb description
    #[cfg(feature = "rocksdb")]
    RocksDb {
        /// The path used
        path: PathBuf,
        /// The namespace
        namespace: String,
    },
    /// The DynamoDB description
    #[cfg(feature = "aws")]
    DynamoDb {
        /// Whether to use the localstack system
        use_localstack: bool,
        /// The namespace
        namespace: String,
    },
    /// The ScyllaDb description
    #[cfg(feature = "scylladb")]
    ScyllaDb {
        /// The URI for accessing the database
        uri: String,
        /// The namespace
        namespace: String,
    },
}

const MEMORY: &str = "memory";
const MEMORY_EXT: &str = "memory:";
const STORAGE_SERVICE: &str = "service:";
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
            let namespace = DEFAULT_NAMESPACE.to_string();
            return Ok(Self::Memory { namespace });
        }
        if let Some(s) = input.strip_prefix(MEMORY_EXT) {
            let namespace = s.to_string();
            return Ok(Self::Memory { namespace });
        }
        if let Some(s) = input.strip_prefix(STORAGE_SERVICE) {
            if s.is_empty() {
                return Err(format_err!(
                    "For Storage service, the formatting has to be service:endpoint:namespace"
                ));
            }
            let parts = s.split(':').collect::<Vec<_>>();
            if parts.len() == 4 {
                let protocol = parts[0];
                let address = parts[1];
                let port = parts[2];
                let mut endpoint = protocol.to_string();
                endpoint.push(':');
                endpoint.push_str(address);
                endpoint.push(':');
                endpoint.push_str(port);
                let endpoint = endpoint.to_string();
                let namespace = parts[3].to_string();
                return Ok(Self::Service {
                    endpoint,
                    namespace,
                });
            }
            return Err(format_err!("We should have one endpoint and one namespace"));
        }
        #[cfg(feature = "rocksdb")]
        if let Some(s) = input.strip_prefix(ROCKS_DB) {
            if s.is_empty() {
                return Err(format_err!(
                    "For RocksDB, the formatting has to be rocksdb:directory:namespace"
                ));
            }
            let parts = s.split(':').collect::<Vec<_>>();
            if parts.len() == 1 {
                let path = parts[0].to_string().into();
                let namespace = DEFAULT_NAMESPACE.to_string();
                return Ok(Self::RocksDb { path, namespace });
            }
            if parts.len() == 2 {
                let path = parts[0].to_string().into();
                let namespace = parts[1].to_string();
                return Ok(Self::RocksDb { path, namespace });
            }
            return Err(format_err!("We should have one or two parts"));
        }
        #[cfg(feature = "aws")]
        if let Some(s) = input.strip_prefix(DYNAMO_DB) {
            let mut parts = s.splitn(2, ':');
            let namespace = parts
                .next()
                .ok_or_else(|| format_err!("Missing DynamoDB table name, e.g. {DYNAMO_DB}TABLE"))?
                .to_string();
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
                use_localstack,
                namespace,
            });
        }
        #[cfg(feature = "scylladb")]
        if let Some(s) = input.strip_prefix(SCYLLA_DB) {
            let mut uri: Option<String> = None;
            let mut namespace: Option<String> = None;
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
                                namespace.is_none(),
                                "The namespace has already been assigned"
                            );
                            namespace = Some(part.to_string());
                        }
                        _ => {
                            bail!("the entry \"{}\" is not matching", part);
                        }
                    }
                }
            }
            let uri = uri.unwrap_or("localhost:9042".to_string());
            let namespace = namespace.unwrap_or(DEFAULT_NAMESPACE.to_string());
            let db = Self::ScyllaDb { uri, namespace };
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
            StorageConfig::Service {
                endpoint,
                namespace,
            } => {
                let endpoint = endpoint.clone();
                let config = ServiceStoreConfig {
                    endpoint,
                    common_config,
                };
                let namespace = namespace.clone();
                Ok(StoreConfig::Service(config, namespace))
            }
            StorageConfig::Memory { namespace } => {
                let config = MemoryStoreConfig { common_config };
                let namespace = namespace.clone();
                Ok(StoreConfig::Memory(config, namespace))
            }
            #[cfg(feature = "rocksdb")]
            StorageConfig::RocksDb { path, namespace } => {
                let path_buf = path.to_path_buf();
                let config = RocksDbStoreConfig {
                    path_buf,
                    common_config,
                };
                let namespace = namespace.clone();
                Ok(StoreConfig::RocksDb(config, namespace))
            }
            #[cfg(feature = "aws")]
            StorageConfig::DynamoDb {
                namespace,
                use_localstack,
            } => {
                let aws_config = get_config(*use_localstack).await?;
                let config = DynamoDbStoreConfig {
                    config: aws_config,
                    common_config,
                };
                let namespace = namespace.clone();
                Ok(StoreConfig::DynamoDb(config, namespace))
            }
            #[cfg(feature = "scylladb")]
            StorageConfig::ScyllaDb { uri, namespace } => {
                let namespace = namespace.to_string();
                let config = ScyllaDbStoreConfig {
                    uri: uri.to_string(),
                    common_config,
                };
                Ok(StoreConfig::ScyllaDb(config, namespace))
            }
        }
    }
}

impl StoreConfig {
    /// Deletes all the entries in the database
    pub async fn delete_all(self) -> Result<(), ViewError> {
        match self {
            StoreConfig::Memory(_, _) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "delete_all does not make sense for memory storage".to_string(),
            }),
            StoreConfig::Service(config, _namespace) => {
                ServiceStoreClient::delete_all(&config).await?;
                Ok(())
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config, _namespace) => {
                RocksDbStore::delete_all(&config).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config, _namespace) => {
                DynamoDbStore::delete_all(&config).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config, _namespace) => {
                ScyllaDbStore::delete_all(&config).await?;
                Ok(())
            }
        }
    }

    /// Deletes only one table of the database
    pub async fn delete_namespace(self) -> Result<(), ViewError> {
        match self {
            StoreConfig::Memory(_, _) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "delete_namespace does not make sense for memory storage".to_string(),
            }),
            StoreConfig::Service(config, namespace) => {
                ServiceStoreClient::delete(&config, &namespace).await?;
                Ok(())
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config, namespace) => {
                RocksDbStore::delete(&config, &namespace).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config, namespace) => {
                DynamoDbStore::delete(&config, &namespace).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config, namespace) => {
                ScyllaDbStore::delete(&config, &namespace).await?;
                Ok(())
            }
        }
    }

    /// Test existence of one table in the database
    pub async fn test_existence(self) -> Result<bool, ViewError> {
        match self {
            StoreConfig::Memory(_, _) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "test_existence does not make sense for memory storage".to_string(),
            }),
            StoreConfig::Service(config, namespace) => {
                Ok(ServiceStoreClient::exists(&config, &namespace).await?)
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config, namespace) => {
                Ok(RocksDbStore::exists(&config, &namespace).await?)
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config, namespace) => {
                Ok(DynamoDbStore::exists(&config, &namespace).await?)
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config, namespace) => {
                Ok(ScyllaDbStore::exists(&config, &namespace).await?)
            }
        }
    }

    /// Deletes only one table of the database
    pub async fn initialize(self) -> Result<(), ViewError> {
        match self {
            StoreConfig::Memory(_, _) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "initialize does not make sense for memory storage".to_string(),
            }),
            StoreConfig::Service(config, namespace) => {
                ServiceStoreClient::maybe_create_and_connect(&config, &namespace).await?;
                Ok(())
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config, namespace) => {
                RocksDbStore::maybe_create_and_connect(&config, &namespace).await?;
                Ok(())
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config, namespace) => {
                DynamoDbStore::maybe_create_and_connect(&config, &namespace).await?;
                Ok(())
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config, namespace) => {
                ScyllaDbStore::maybe_create_and_connect(&config, &namespace).await?;
                Ok(())
            }
        }
    }

    /// Lists all the namespaces of the storage
    pub async fn list_all(self) -> Result<Vec<String>, ViewError> {
        match self {
            StoreConfig::Memory(_, _) => Err(ViewError::ContextError {
                backend: "memory".to_string(),
                error: "list_all is not supported for the memory storage".to_string(),
            }),
            StoreConfig::Service(config, _namespace) => {
                let tables = ServiceStoreClient::list_all(&config).await?;
                Ok(tables)
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config, _namespace) => {
                let tables = RocksDbStore::list_all(&config).await?;
                Ok(tables)
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(config, _namespace) => {
                let tables = DynamoDbStore::list_all(&config).await?;
                Ok(tables)
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config, _namespace) => {
                let tables = ScyllaDbStore::list_all(&config).await?;
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
        StoreConfig::Memory(config, namespace) => {
            let store_config = MemoryStoreConfig::new(config.common_config.max_stream_queries);
            let mut storage = MemoryStorage::new(store_config, &namespace, wasm_runtime).await?;
            genesis_config.initialize_storage(&mut storage).await?;
            job.run(storage).await
        }
        StoreConfig::Service(config, namespace) => {
            let storage = ServiceStorage::new(config, &namespace, wasm_runtime).await?;
            job.run(storage).await
        }
        #[cfg(feature = "rocksdb")]
        StoreConfig::RocksDb(config, namespace) => {
            let storage = RocksDbStorage::new(config, &namespace, wasm_runtime).await?;
            job.run(storage).await
        }
        #[cfg(feature = "aws")]
        StoreConfig::DynamoDb(config, namespace) => {
            let storage = DynamoDbStorage::new(config, &namespace, wasm_runtime).await?;
            job.run(storage).await
        }
        #[cfg(feature = "scylladb")]
        StoreConfig::ScyllaDb(config, namespace) => {
            let storage = ScyllaDbStorage::new(config, &namespace, wasm_runtime).await?;
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
        StoreConfig::Memory(_, _) => {
            bail!("The initialization should not be called for memory");
        }
        StoreConfig::Service(config, namespace) => {
            let wasm_runtime = None;
            let mut storage = ServiceStorage::initialize(config, &namespace, wasm_runtime).await?;
            genesis_config.initialize_storage(&mut storage).await
        }
        #[cfg(feature = "rocksdb")]
        StoreConfig::RocksDb(config, namespace) => {
            let wasm_runtime = None;
            let mut storage = RocksDbStorage::initialize(config, &namespace, wasm_runtime).await?;
            genesis_config.initialize_storage(&mut storage).await
        }
        #[cfg(feature = "aws")]
        StoreConfig::DynamoDb(config, namespace) => {
            let wasm_runtime = None;
            let mut storage = DynamoDbStorage::initialize(config, &namespace, wasm_runtime).await?;
            genesis_config.initialize_storage(&mut storage).await
        }
        #[cfg(feature = "scylladb")]
        StoreConfig::ScyllaDb(config, namespace) => {
            let wasm_runtime = None;
            let mut storage = ScyllaDbStorage::initialize(config, &namespace, wasm_runtime).await?;
            genesis_config.initialize_storage(&mut storage).await
        }
    }
}

#[test]
fn test_memory_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("memory:").unwrap(),
        StorageConfig::Memory {
            namespace: "".into()
        }
    );
    assert_eq!(
        StorageConfig::from_str("memory").unwrap(),
        StorageConfig::Memory {
            namespace: DEFAULT_NAMESPACE.into()
        }
    );
    assert_eq!(
        StorageConfig::from_str("memory:table_linera").unwrap(),
        StorageConfig::Memory {
            namespace: DEFAULT_NAMESPACE.into()
        }
    );
}

#[test]
fn test_shared_store_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("service:http://127.0.0.1:8942:linera").unwrap(),
        StorageConfig::Service {
            endpoint: "http://127.0.0.1:8942".to_string(),
            namespace: "linera".into()
        }
    );
    assert!(StorageConfig::from_str("service:http://127.0.0.1:8942").is_err());
    assert!(StorageConfig::from_str("service:http://127.0.0.1:linera").is_err());
}

#[cfg(feature = "rocksdb")]
#[test]
fn test_rocks_db_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("rocksdb:foo.db:chosen_namespace").unwrap(),
        StorageConfig::RocksDb {
            path: "foo.db".into(),
            namespace: "chosen_namespace".into(),
        }
    );
    assert!(StorageConfig::from_str("rocksdb_foo.db").is_err());
    assert_eq!(
        StorageConfig::from_str("rocksdb:foo.db").unwrap(),
        StorageConfig::RocksDb {
            path: "foo.db".into(),
            namespace: DEFAULT_NAMESPACE.to_string(),
        }
    );
}

#[cfg(feature = "aws")]
#[test]
fn test_aws_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("dynamodb:table").unwrap(),
        StorageConfig::DynamoDb {
            namespace: "table".to_string(),
            use_localstack: false,
        }
    );
    assert_eq!(
        StorageConfig::from_str("dynamodb:table:env").unwrap(),
        StorageConfig::DynamoDb {
            namespace: "table".to_string(),
            use_localstack: false,
        }
    );
    assert_eq!(
        StorageConfig::from_str("dynamodb:table:localstack").unwrap(),
        StorageConfig::DynamoDb {
            namespace: "table".to_string(),
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
            namespace: DEFAULT_NAMESPACE.to_string(),
        }
    );
    assert_eq!(
        StorageConfig::from_str("scylladb:tcp:db_hostname:230:table_other_storage").unwrap(),
        StorageConfig::ScyllaDb {
            uri: "db_hostname:230".to_string(),
            namespace: "table_other_storage".to_string(),
        }
    );
    assert_eq!(
        StorageConfig::from_str("scylladb:tcp:db_hostname:230").unwrap(),
        StorageConfig::ScyllaDb {
            uri: "db_hostname:230".to_string(),
            namespace: DEFAULT_NAMESPACE.to_string(),
        }
    );
    assert!(StorageConfig::from_str("scylladb:-10").is_err());
    assert!(StorageConfig::from_str("scylladb:70000").is_err());
    assert!(StorageConfig::from_str("scylladb:230:234").is_err());
    assert!(StorageConfig::from_str("scylladb:tcp:address1").is_err());
    assert!(StorageConfig::from_str("scylladb:tcp:address1:tcp:/address2").is_err());
    assert!(StorageConfig::from_str("scylladb:wrong").is_err());
}
