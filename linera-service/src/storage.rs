// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, str::FromStr};

use anyhow::anyhow;
use async_trait::async_trait;
use linera_client::config::GenesisConfig;
use linera_execution::WasmRuntime;
use linera_storage::{DbStorage, Storage, DEFAULT_NAMESPACE};
#[cfg(feature = "storage-service")]
use linera_storage_service::{
    client::ServiceStoreClient,
    common::{ServiceStoreConfig, ServiceStoreInternalConfig},
};
#[cfg(feature = "dynamodb")]
use linera_views::dynamo_db::{DynamoDbStore, DynamoDbStoreConfig};
use linera_views::{
    memory::{MemoryStore, MemoryStoreConfig},
    store::{CommonStoreConfig, KeyValueStore},
};
use serde::{Deserialize, Serialize};
use tracing::error;
#[allow(unused_imports)]
use {anyhow::bail, linera_views::store::LocalAdminKeyValueStore as _};
#[cfg(all(feature = "rocksdb", feature = "scylladb"))]
use {
    linera_storage::ChainStatesFirstAssignment,
    linera_views::backends::dual::{DualStore, DualStoreConfig},
    std::path::Path,
};
#[cfg(feature = "rocksdb")]
use {
    linera_views::rocks_db::{PathWithGuard, RocksDbSpawnMode, RocksDbStore, RocksDbStoreConfig},
    std::path::PathBuf,
};
#[cfg(feature = "scylladb")]
use {
    linera_views::scylla_db::{ScyllaDbStore, ScyllaDbStoreConfig},
    std::num::NonZeroU16,
    tracing::debug,
};

/// The configuration of the key value store in use.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum StoreConfig {
    /// The storage service key-value store
    #[cfg(feature = "storage-service")]
    Service(ServiceStoreConfig, String),
    /// The memory key value store
    Memory(MemoryStoreConfig, String),
    /// The RocksDB key value store
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbStoreConfig, String),
    /// The DynamoDB key value store
    #[cfg(feature = "dynamodb")]
    DynamoDb(DynamoDbStoreConfig, String),
    /// The ScyllaDB key value store
    #[cfg(feature = "scylladb")]
    ScyllaDb(ScyllaDbStoreConfig, String),
    #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
    DualRocksDbScyllaDb(
        DualStoreConfig<RocksDbStoreConfig, ScyllaDbStoreConfig>,
        String,
    ),
}

/// The description of a storage implementation.
#[derive(Clone, Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub enum StorageConfig {
    /// The storage service description
    #[cfg(feature = "storage-service")]
    Service {
        /// The endpoint used
        endpoint: String,
    },
    /// The memory description
    Memory,
    /// The RocksDB description
    #[cfg(feature = "rocksdb")]
    RocksDb {
        /// The path used
        path: PathBuf,
        /// Whether to use `block_in_place` or `spawn_blocking`.
        spawn_mode: RocksDbSpawnMode,
    },
    /// The DynamoDB description
    #[cfg(feature = "dynamodb")]
    DynamoDb {
        /// Whether to use the DynamoDB Local system
        use_dynamodb_local: bool,
    },
    /// The ScyllaDB description
    #[cfg(feature = "scylladb")]
    ScyllaDb {
        /// The URI for accessing the database
        uri: String,
    },
    #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
    DualRocksDbScyllaDb {
        /// The path used
        path_with_guard: PathWithGuard,
        /// Whether to use `block_in_place` or `spawn_blocking`.
        spawn_mode: RocksDbSpawnMode,
        /// The URI for accessing the database
        uri: String,
    },
}

impl StorageConfig {
    pub fn get_shared_storage(&self) -> Self {
        match self {
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StorageConfig::DualRocksDbScyllaDb {
                path_with_guard: _,
                spawn_mode: _,
                uri,
            } => {
                let uri = uri.clone();
                StorageConfig::ScyllaDb { uri }
            }
            x => x.clone(),
        }
    }

    pub fn are_chains_shared(&self) -> bool {
        match self {
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StorageConfig::DualRocksDbScyllaDb { .. } => false,
            _ => true,
        }
    }

    pub fn append_shard_str(&mut self, _shard_str: &str) {
        match self {
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StorageConfig::DualRocksDbScyllaDb {
                path_with_guard,
                spawn_mode: _,
                uri: _,
            } => {
                path_with_guard.path_buf.push(_shard_str);
            }
            _ => panic!("append_shard_str is not available for this storage"),
        }
    }
}

/// The description of a storage implementation.
#[derive(Clone, Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub struct StorageConfigNamespace {
    /// The storage config
    pub storage_config: StorageConfig,
    /// The namespace used
    pub namespace: String,
}

const MEMORY: &str = "memory";
const MEMORY_EXT: &str = "memory:";
#[cfg(feature = "storage-service")]
const STORAGE_SERVICE: &str = "service:";
#[cfg(feature = "rocksdb")]
const ROCKS_DB: &str = "rocksdb:";
#[cfg(feature = "dynamodb")]
const DYNAMO_DB: &str = "dynamodb:";
#[cfg(feature = "scylladb")]
const SCYLLA_DB: &str = "scylladb:";
#[cfg(all(feature = "rocksdb", feature = "scylladb"))]
const DUAL_ROCKS_DB_SCYLLA_DB: &str = "dualrocksdbscylladb:";

impl FromStr for StorageConfigNamespace {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input == MEMORY {
            let namespace = DEFAULT_NAMESPACE.to_string();
            let storage_config = StorageConfig::Memory;
            return Ok(StorageConfigNamespace {
                storage_config,
                namespace,
            });
        }
        if let Some(s) = input.strip_prefix(MEMORY_EXT) {
            let namespace = s.to_string();
            let storage_config = StorageConfig::Memory;
            return Ok(StorageConfigNamespace {
                storage_config,
                namespace,
            });
        }
        #[cfg(feature = "storage-service")]
        if let Some(s) = input.strip_prefix(STORAGE_SERVICE) {
            if s.is_empty() {
                bail!(
                    "For Storage service, the formatting has to be service:endpoint:namespace,\
example service:tcp:127.0.0.1:7878:table_do_my_test"
                );
            }
            let parts = s.split(':').collect::<Vec<_>>();
            if parts.len() != 4 {
                bail!("We should have one endpoint and one namespace");
            }
            let protocol = parts[0];
            if protocol != "tcp" {
                bail!("Only allowed protocol is tcp");
            }
            let endpoint = parts[1];
            let port = parts[2];
            let mut endpoint = endpoint.to_string();
            endpoint.push(':');
            endpoint.push_str(port);
            let endpoint = endpoint.to_string();
            let namespace = parts[3].to_string();
            let storage_config = StorageConfig::Service { endpoint };
            return Ok(StorageConfigNamespace {
                storage_config,
                namespace,
            });
        }
        #[cfg(feature = "rocksdb")]
        if let Some(s) = input.strip_prefix(ROCKS_DB) {
            if s.is_empty() {
                bail!(
                    "For RocksDB, the formatting has to be rocksdb:directory or rocksdb:directory:spawn_mode:namespace");
            }
            let parts = s.split(':').collect::<Vec<_>>();
            if parts.len() == 1 {
                let path = parts[0].to_string().into();
                let namespace = DEFAULT_NAMESPACE.to_string();
                let spawn_mode = RocksDbSpawnMode::SpawnBlocking;
                let storage_config = StorageConfig::RocksDb { path, spawn_mode };
                return Ok(StorageConfigNamespace {
                    storage_config,
                    namespace,
                });
            }
            if parts.len() == 2 || parts.len() == 3 {
                let path = parts[0].to_string().into();
                let spawn_mode = match parts[1] {
                    "spawn_blocking" => Ok(RocksDbSpawnMode::SpawnBlocking),
                    "block_in_place" => Ok(RocksDbSpawnMode::BlockInPlace),
                    "runtime" => Ok(RocksDbSpawnMode::get_spawn_mode_from_runtime()),
                    _ => Err(anyhow!("Failed to parse {} as a spawn_mode", parts[1])),
                }?;
                let namespace = if parts.len() == 2 {
                    DEFAULT_NAMESPACE.to_string()
                } else {
                    parts[2].to_string()
                };
                let storage_config = StorageConfig::RocksDb { path, spawn_mode };
                return Ok(StorageConfigNamespace {
                    storage_config,
                    namespace,
                });
            }
            bail!("We should have one, two or three parts");
        }
        #[cfg(feature = "dynamodb")]
        if let Some(s) = input.strip_prefix(DYNAMO_DB) {
            let mut parts = s.splitn(2, ':');
            let namespace = parts
                .next()
                .ok_or_else(|| anyhow!("Missing DynamoDB table name, e.g. {DYNAMO_DB}TABLE"))?
                .to_string();
            let use_dynamodb_local = match parts.next() {
                None | Some("env") => false,
                Some("dynamodb_local") => true,
                Some(unknown) => {
                    bail!(
                        "Invalid DynamoDB endpoint {unknown:?}. \
                        Expected {DYNAMO_DB}TABLE:[env|dynamodb_local]"
                    );
                }
            };
            let storage_config = StorageConfig::DynamoDb { use_dynamodb_local };
            return Ok(StorageConfigNamespace {
                storage_config,
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
                            let address = parts.next().ok_or_else(|| {
                                anyhow!("Failed to find address for {s}. {parse_error}")
                            })?;
                            let port_str = parts.next().ok_or_else(|| {
                                anyhow!("Failed to find port for {s}. {parse_error}")
                            })?;
                            let port = NonZeroU16::from_str(port_str).map_err(|_| {
                                anyhow!(
                                    "Failed to find parse port {port_str} for {s}. {parse_error}",
                                )
                            })?;
                            if uri.is_some() {
                                bail!("The uri has already been assigned");
                            }
                            uri = Some(format!("{}:{}", &address, port));
                        }
                        _ if part.starts_with("table") => {
                            if namespace.is_some() {
                                bail!("The namespace has already been assigned");
                            }
                            namespace = Some(part.to_string());
                        }
                        _ => {
                            bail!("the entry \"{part}\" is not matching");
                        }
                    }
                }
            }
            let uri = uri.unwrap_or("localhost:9042".to_string());
            let namespace = namespace.unwrap_or(DEFAULT_NAMESPACE.to_string());
            let storage_config = StorageConfig::ScyllaDb { uri };
            debug!("ScyllaDB connection info: {:?}", storage_config);
            return Ok(StorageConfigNamespace {
                storage_config,
                namespace,
            });
        }
        #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
        if let Some(s) = input.strip_prefix(DUAL_ROCKS_DB_SCYLLA_DB) {
            let parts = s.split(':').collect::<Vec<_>>();
            if parts.len() != 5 && parts.len() != 6 {
                bail!(
                    "For DualRocksDbScyllaDb, the formatting has to be dualrocksdbscylladb:directory:mode:tcp:hostname:port:namespace"
                );
            }
            let path = Path::new(parts[0]);
            let path = path.to_path_buf();
            let path_with_guard = PathWithGuard::new(path);
            let spawn_mode = match parts[1] {
                "spawn_blocking" => Ok(RocksDbSpawnMode::SpawnBlocking),
                "block_in_place" => Ok(RocksDbSpawnMode::BlockInPlace),
                "runtime" => Ok(RocksDbSpawnMode::get_spawn_mode_from_runtime()),
                _ => Err(anyhow!("Failed to parse {} as a spawn_mode", parts[1])),
            }?;
            let protocol = parts[2];
            if protocol != "tcp" {
                bail!("The only allowed protocol is tcp");
            }
            let address = parts[3];
            let port_str = parts[4];
            let port = NonZeroU16::from_str(port_str)
                .map_err(|_| anyhow!("Failed to find parse port {port_str} for {s}"))?;
            let uri = format!("{}:{}", &address, port);
            let storage_config = StorageConfig::DualRocksDbScyllaDb {
                path_with_guard,
                spawn_mode,
                uri,
            };
            let namespace = if parts.len() == 5 {
                DEFAULT_NAMESPACE.to_string()
            } else {
                parts[5].to_string()
            };
            return Ok(StorageConfigNamespace {
                storage_config,
                namespace,
            });
        }
        error!("available storage: memory");
        #[cfg(feature = "storage-service")]
        error!("Also available is linera-storage-service");
        #[cfg(feature = "rocksdb")]
        error!("Also available is RocksDB");
        #[cfg(feature = "dynamodb")]
        error!("Also available is DynamoDB");
        #[cfg(feature = "scylladb")]
        error!("Also available is ScyllaDB");
        #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
        error!("Also available is DualRocksDbScyllaDb");
        Err(anyhow!("The input has not matched: {input}"))
    }
}

impl StorageConfigNamespace {
    /// The addition of the common config to get a full configuration
    pub async fn add_common_config(
        &self,
        common_config: CommonStoreConfig,
    ) -> Result<StoreConfig, anyhow::Error> {
        let namespace = self.namespace.clone();
        match &self.storage_config {
            #[cfg(feature = "storage-service")]
            StorageConfig::Service { endpoint } => {
                let endpoint = endpoint.clone();
                let inner_config = ServiceStoreInternalConfig {
                    endpoint,
                    common_config: common_config.reduced(),
                };
                let config = ServiceStoreConfig {
                    inner_config,
                    storage_cache_config: common_config.storage_cache_config,
                };
                Ok(StoreConfig::Service(config, namespace))
            }
            StorageConfig::Memory => {
                let config = MemoryStoreConfig {
                    common_config: common_config.reduced(),
                };
                Ok(StoreConfig::Memory(config, namespace))
            }
            #[cfg(feature = "rocksdb")]
            StorageConfig::RocksDb { path, spawn_mode } => {
                let path_buf = path.to_path_buf();
                let path_with_guard = PathWithGuard::new(path_buf);
                let config = RocksDbStoreConfig::new(*spawn_mode, path_with_guard, common_config);
                Ok(StoreConfig::RocksDb(config, namespace))
            }
            #[cfg(feature = "dynamodb")]
            StorageConfig::DynamoDb { use_dynamodb_local } => {
                let config = DynamoDbStoreConfig::new(*use_dynamodb_local, common_config);
                Ok(StoreConfig::DynamoDb(config, namespace))
            }
            #[cfg(feature = "scylladb")]
            StorageConfig::ScyllaDb { uri } => {
                let config = ScyllaDbStoreConfig::new(uri.to_string(), common_config);
                Ok(StoreConfig::ScyllaDb(config, namespace))
            }
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StorageConfig::DualRocksDbScyllaDb {
                path_with_guard,
                spawn_mode,
                uri,
            } => {
                let first_config = RocksDbStoreConfig::new(
                    *spawn_mode,
                    path_with_guard.clone(),
                    common_config.clone(),
                );
                let second_config = ScyllaDbStoreConfig::new(uri.to_string(), common_config);
                let config = DualStoreConfig {
                    first_config,
                    second_config,
                };
                Ok(StoreConfig::DualRocksDbScyllaDb(config, namespace))
            }
        }
    }
}

impl fmt::Display for StorageConfigNamespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let namespace = &self.namespace;
        match &self.storage_config {
            #[cfg(feature = "storage-service")]
            StorageConfig::Service { endpoint } => {
                write!(f, "service:tcp:{}:{}", endpoint, namespace)
            }
            StorageConfig::Memory => {
                write!(f, "memory:{}", namespace)
            }
            #[cfg(feature = "rocksdb")]
            StorageConfig::RocksDb { path, spawn_mode } => {
                let spawn_mode = spawn_mode.to_string();
                write!(f, "rocksdb:{}:{}:{}", path.display(), spawn_mode, namespace)
            }
            #[cfg(feature = "dynamodb")]
            StorageConfig::DynamoDb { use_dynamodb_local } => match use_dynamodb_local {
                true => write!(f, "dynamodb:{}:dynamodb_local", namespace),
                false => write!(f, "dynamodb:{}:env", namespace),
            },
            #[cfg(feature = "scylladb")]
            StorageConfig::ScyllaDb { uri } => {
                write!(f, "scylladb:tcp:{}:{}", uri, namespace)
            }
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StorageConfig::DualRocksDbScyllaDb {
                path_with_guard,
                spawn_mode,
                uri,
            } => {
                write!(
                    f,
                    "dualrocksdbscylladb:{}:{}:tcp:{}:{}",
                    path_with_guard.path_buf.display(),
                    spawn_mode,
                    uri,
                    namespace
                )
            }
        }
    }
}

#[async_trait]
pub trait Runnable {
    type Output;

    async fn run<S>(self, storage: S) -> Self::Output
    where
        S: Storage + Clone + Send + Sync + 'static;
}

#[async_trait]
pub trait RunnableWithStore {
    type Output;

    async fn run<S>(
        self,
        config: S::Config,
        namespace: String,
    ) -> Result<Self::Output, anyhow::Error>
    where
        S: KeyValueStore + Clone + Send + Sync + 'static,
        S::Error: Send + Sync;
}

impl StoreConfig {
    #[allow(unused_variables)]
    pub async fn run_with_storage<Job>(
        self,
        genesis_config: &GenesisConfig,
        wasm_runtime: Option<WasmRuntime>,
        job: Job,
    ) -> Result<Job::Output, anyhow::Error>
    where
        Job: Runnable,
    {
        match self {
            StoreConfig::Memory(config, namespace) => {
                let store_config = MemoryStoreConfig::new(config.common_config.max_stream_queries);
                let mut storage = DbStorage::<MemoryStore, _>::maybe_create_and_connect(
                    &store_config,
                    &namespace,
                    wasm_runtime,
                )
                .await?;
                // Memory storage must be initialized every time.
                genesis_config.initialize_storage(&mut storage).await?;
                Ok(job.run(storage).await)
            }
            #[cfg(feature = "storage-service")]
            StoreConfig::Service(config, namespace) => {
                let storage =
                    DbStorage::<ServiceStoreClient, _>::connect(&config, &namespace, wasm_runtime)
                        .await?;
                Ok(job.run(storage).await)
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config, namespace) => {
                let storage =
                    DbStorage::<RocksDbStore, _>::connect(&config, &namespace, wasm_runtime)
                        .await?;
                Ok(job.run(storage).await)
            }
            #[cfg(feature = "dynamodb")]
            StoreConfig::DynamoDb(config, namespace) => {
                let storage =
                    DbStorage::<DynamoDbStore, _>::connect(&config, &namespace, wasm_runtime)
                        .await?;
                Ok(job.run(storage).await)
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config, namespace) => {
                let storage =
                    DbStorage::<ScyllaDbStore, _>::connect(&config, &namespace, wasm_runtime)
                        .await?;
                Ok(job.run(storage).await)
            }
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StoreConfig::DualRocksDbScyllaDb(config, namespace) => {
                let storage = DbStorage::<
                    DualStore<RocksDbStore, ScyllaDbStore, ChainStatesFirstAssignment>,
                    _,
                >::connect(&config, &namespace, wasm_runtime)
                .await?;
                Ok(job.run(storage).await)
            }
        }
    }

    #[allow(unused_variables)]
    pub async fn run_with_store<Job>(self, job: Job) -> Result<Job::Output, anyhow::Error>
    where
        Job: RunnableWithStore,
    {
        match self {
            StoreConfig::Memory(_, _) => {
                Err(anyhow!("Cannot run admin operations on the memory store"))
            }
            #[cfg(feature = "storage-service")]
            StoreConfig::Service(config, namespace) => {
                Ok(job.run::<ServiceStoreClient>(config, namespace).await?)
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(config, namespace) => {
                Ok(job.run::<RocksDbStore>(config, namespace).await?)
            }
            #[cfg(feature = "dynamodb")]
            StoreConfig::DynamoDb(config, namespace) => {
                Ok(job.run::<DynamoDbStore>(config, namespace).await?)
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(config, namespace) => {
                Ok(job.run::<ScyllaDbStore>(config, namespace).await?)
            }
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            StoreConfig::DualRocksDbScyllaDb(config, namespace) => Ok(job
                .run::<DualStore<RocksDbStore, ScyllaDbStore, ChainStatesFirstAssignment>>(
                    config, namespace,
                )
                .await?),
        }
    }

    pub async fn initialize(self, config: &GenesisConfig) -> Result<(), anyhow::Error> {
        self.run_with_store(InitializeStorageJob(config)).await
    }
}

struct InitializeStorageJob<'a>(&'a GenesisConfig);

#[async_trait]
impl RunnableWithStore for InitializeStorageJob<'_> {
    type Output = ();

    async fn run<S>(
        self,
        config: S::Config,
        namespace: String,
    ) -> Result<Self::Output, anyhow::Error>
    where
        S: KeyValueStore + Clone + Send + Sync + 'static,
        S::Error: Send + Sync,
    {
        let mut storage =
            DbStorage::<S, _>::maybe_create_and_connect(&config, &namespace, None).await?;
        self.0.initialize_storage(&mut storage).await?;
        Ok(())
    }
}

#[test]
fn test_memory_storage_config_from_str() {
    assert_eq!(
        StorageConfigNamespace::from_str("memory:").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::Memory,
            namespace: "".into()
        }
    );
    assert_eq!(
        StorageConfigNamespace::from_str("memory").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::Memory,
            namespace: DEFAULT_NAMESPACE.into()
        }
    );
    assert_eq!(
        StorageConfigNamespace::from_str("memory:table_linera").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::Memory,
            namespace: DEFAULT_NAMESPACE.into()
        }
    );
}

#[cfg(feature = "storage-service")]
#[test]
fn test_shared_store_config_from_str() {
    assert_eq!(
        StorageConfigNamespace::from_str("service:tcp:127.0.0.1:8942:linera").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::Service {
                endpoint: "127.0.0.1:8942".to_string()
            },
            namespace: "linera".into()
        }
    );
    assert!(StorageConfigNamespace::from_str("service:tcp:127.0.0.1:8942").is_err());
    assert!(StorageConfigNamespace::from_str("service:tcp:127.0.0.1:linera").is_err());
}

#[cfg(feature = "rocksdb")]
#[test]
fn test_rocks_db_storage_config_from_str() {
    assert!(StorageConfigNamespace::from_str("rocksdb_foo.db").is_err());
    assert_eq!(
        StorageConfigNamespace::from_str("rocksdb:foo.db").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::RocksDb {
                path: "foo.db".into(),
                spawn_mode: RocksDbSpawnMode::SpawnBlocking,
            },
            namespace: DEFAULT_NAMESPACE.to_string()
        }
    );
    assert_eq!(
        StorageConfigNamespace::from_str("rocksdb:foo.db:block_in_place").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::RocksDb {
                path: "foo.db".into(),
                spawn_mode: RocksDbSpawnMode::BlockInPlace,
            },
            namespace: DEFAULT_NAMESPACE.to_string()
        }
    );
    assert_eq!(
        StorageConfigNamespace::from_str("rocksdb:foo.db:block_in_place:chosen_namespace").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::RocksDb {
                path: "foo.db".into(),
                spawn_mode: RocksDbSpawnMode::BlockInPlace,
            },
            namespace: "chosen_namespace".into()
        }
    );
}

#[cfg(feature = "dynamodb")]
#[test]
fn test_aws_storage_config_from_str() {
    assert_eq!(
        StorageConfigNamespace::from_str("dynamodb:table").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::DynamoDb {
                use_dynamodb_local: false
            },
            namespace: "table".to_string()
        }
    );
    assert_eq!(
        StorageConfigNamespace::from_str("dynamodb:table:env").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::DynamoDb {
                use_dynamodb_local: false
            },
            namespace: "table".to_string()
        }
    );
    assert_eq!(
        StorageConfigNamespace::from_str("dynamodb:table:dynamodb_local").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::DynamoDb {
                use_dynamodb_local: true
            },
            namespace: "table".to_string()
        }
    );
    assert!(StorageConfigNamespace::from_str("dynamodb").is_err());
    assert!(StorageConfigNamespace::from_str("dynamodb:").is_err());
    assert!(StorageConfigNamespace::from_str("dynamodb:1").is_err());
    assert!(StorageConfigNamespace::from_str("dynamodb:wrong:endpoint").is_err());
}

#[cfg(feature = "scylladb")]
#[test]
fn test_scylla_db_storage_config_from_str() {
    assert_eq!(
        StorageConfigNamespace::from_str("scylladb:").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::ScyllaDb {
                uri: "localhost:9042".to_string()
            },
            namespace: DEFAULT_NAMESPACE.to_string()
        }
    );
    assert_eq!(
        StorageConfigNamespace::from_str("scylladb:tcp:db_hostname:230:table_other_storage")
            .unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::ScyllaDb {
                uri: "db_hostname:230".to_string()
            },
            namespace: "table_other_storage".to_string()
        }
    );
    assert_eq!(
        StorageConfigNamespace::from_str("scylladb:tcp:db_hostname:230").unwrap(),
        StorageConfigNamespace {
            storage_config: StorageConfig::ScyllaDb {
                uri: "db_hostname:230".to_string()
            },
            namespace: DEFAULT_NAMESPACE.to_string()
        }
    );
    assert!(StorageConfigNamespace::from_str("scylladb:-10").is_err());
    assert!(StorageConfigNamespace::from_str("scylladb:70000").is_err());
    assert!(StorageConfigNamespace::from_str("scylladb:230:234").is_err());
    assert!(StorageConfigNamespace::from_str("scylladb:tcp:address1").is_err());
    assert!(StorageConfigNamespace::from_str("scylladb:tcp:address1:tcp:/address2").is_err());
    assert!(StorageConfigNamespace::from_str("scylladb:wrong").is_err());
}
