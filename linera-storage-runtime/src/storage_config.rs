// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, path::PathBuf, str::FromStr};

use anyhow::{anyhow, bail};
use linera_storage::DEFAULT_NAMESPACE;
#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::{PathWithGuard, RocksDbSpawnMode};
use tracing::error;
#[cfg(all(feature = "rocksdb", feature = "scylladb"))]
use {linera_views::backends::dual::DualStoreConfig, std::path::Path};
#[cfg(feature = "scylladb")]
use {std::num::NonZeroU16, tracing::debug};

use crate::{CommonStorageOptions, StoreConfig};

/// The description of a storage implementation.
#[derive(Clone, Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub enum InnerStorageConfig {
    /// The memory description.
    Memory {
        /// The path to the genesis configuration. This is needed because we reinitialize
        /// memory databases from the genesis config everytime.
        genesis_path: PathBuf,
    },
    /// The storage service description.
    #[cfg(feature = "storage-service")]
    Service {
        /// The endpoint used.
        endpoint: String,
    },
    /// The RocksDB description.
    #[cfg(feature = "rocksdb")]
    RocksDb {
        /// The path used.
        path: PathBuf,
        /// Whether to use `block_in_place` or `spawn_blocking`.
        spawn_mode: RocksDbSpawnMode,
    },
    /// The DynamoDB description.
    #[cfg(feature = "dynamodb")]
    DynamoDb {
        /// Whether to use the DynamoDB Local system
        use_dynamodb_local: bool,
    },
    /// The ScyllaDB description.
    #[cfg(feature = "scylladb")]
    ScyllaDb {
        /// The URI for accessing the database.
        uri: String,
    },
    #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
    DualRocksDbScyllaDb {
        /// The path used.
        path_with_guard: PathWithGuard,
        /// Whether to use `block_in_place` or `spawn_blocking`.
        spawn_mode: RocksDbSpawnMode,
        /// The URI for accessing the database.
        uri: String,
    },
}

/// The description of a storage implementation.
#[derive(Clone, Debug)]
#[cfg_attr(any(test), derive(Eq, PartialEq))]
pub struct StorageConfig {
    /// The inner storage config.
    pub inner_storage_config: InnerStorageConfig,
    /// The namespace used
    pub namespace: String,
}

const MEMORY: &str = "memory:";
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

impl FromStr for StorageConfig {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Some(s) = input.strip_prefix(MEMORY) {
            let parts = s.split(':').collect::<Vec<_>>();
            if parts.len() == 1 {
                let genesis_path = parts[0].to_string().into();
                let namespace = DEFAULT_NAMESPACE.to_string();
                let inner_storage_config = InnerStorageConfig::Memory { genesis_path };
                return Ok(StorageConfig {
                    inner_storage_config,
                    namespace,
                });
            }
            if parts.len() != 2 {
                bail!("We should have one genesis config path and one optional namespace");
            }
            let genesis_path = parts[0].to_string().into();
            let namespace = parts[1].to_string();
            let inner_storage_config = InnerStorageConfig::Memory { genesis_path };
            return Ok(StorageConfig {
                inner_storage_config,
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
            let inner_storage_config = InnerStorageConfig::Service { endpoint };
            return Ok(StorageConfig {
                inner_storage_config,
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
                let inner_storage_config = InnerStorageConfig::RocksDb { path, spawn_mode };
                return Ok(StorageConfig {
                    inner_storage_config,
                    namespace,
                });
            }
            if parts.len() == 2 || parts.len() == 3 {
                let path = parts[0].to_string().into();
                let spawn_mode_name = parts
                    .get(1)
                    .copied()
                    .expect("validated by the parts length check above");
                let spawn_mode = match spawn_mode_name {
                    "spawn_blocking" => Ok(RocksDbSpawnMode::SpawnBlocking),
                    "block_in_place" => Ok(RocksDbSpawnMode::BlockInPlace),
                    "runtime" => Ok(RocksDbSpawnMode::get_spawn_mode_from_runtime()),
                    _ => Err(anyhow!(
                        "Failed to parse {} as a spawn_mode",
                        spawn_mode_name
                    )),
                }?;
                let namespace = if parts.len() == 2 {
                    DEFAULT_NAMESPACE.to_string()
                } else {
                    parts[2].to_string()
                };
                let inner_storage_config = InnerStorageConfig::RocksDb { path, spawn_mode };
                return Ok(StorageConfig {
                    inner_storage_config,
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
            let inner_storage_config = InnerStorageConfig::DynamoDb { use_dynamodb_local };
            return Ok(StorageConfig {
                inner_storage_config,
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
            let uri = uri.unwrap_or_else(|| "localhost:9042".to_string());
            let namespace = namespace.unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());
            let inner_storage_config = InnerStorageConfig::ScyllaDb { uri };
            debug!("ScyllaDB connection info: {:?}", inner_storage_config);
            return Ok(StorageConfig {
                inner_storage_config,
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
            let spawn_mode_name = parts
                .get(1)
                .copied()
                .expect("validated by the parts length check above");
            let spawn_mode = match spawn_mode_name {
                "spawn_blocking" => Ok(RocksDbSpawnMode::SpawnBlocking),
                "block_in_place" => Ok(RocksDbSpawnMode::BlockInPlace),
                "runtime" => Ok(RocksDbSpawnMode::get_spawn_mode_from_runtime()),
                _ => Err(anyhow!(
                    "Failed to parse {} as a spawn_mode",
                    spawn_mode_name
                )),
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
            let inner_storage_config = InnerStorageConfig::DualRocksDbScyllaDb {
                path_with_guard,
                spawn_mode,
                uri,
            };
            let namespace = if parts.len() == 5 {
                DEFAULT_NAMESPACE.to_string()
            } else {
                parts[5].to_string()
            };
            return Ok(StorageConfig {
                inner_storage_config,
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

impl StorageConfig {
    #[allow(unused_variables)]
    pub fn maybe_append_shard_path(&mut self, shard: usize) -> std::io::Result<()> {
        match &mut self.inner_storage_config {
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            InnerStorageConfig::DualRocksDbScyllaDb {
                path_with_guard,
                spawn_mode: _,
                uri: _,
            } => {
                let shard_str = format!("shard_{shard}");
                path_with_guard.path_buf.push(shard_str);
                std::fs::create_dir_all(&path_with_guard.path_buf)
            }
            _ => Ok(()),
        }
    }

    /// The addition of the common config to get a full configuration
    pub fn add_common_storage_options(
        &self,
        options: &CommonStorageOptions,
    ) -> Result<StoreConfig, anyhow::Error> {
        let namespace = self.namespace.clone();
        match &self.inner_storage_config {
            InnerStorageConfig::Memory { genesis_path } => {
                let config = linera_views::memory::MemoryStoreConfig {
                    max_stream_queries: options.storage_max_stream_queries,
                    kill_on_drop: false,
                };
                let genesis_path = genesis_path.clone();
                Ok(StoreConfig::Memory {
                    config,
                    namespace,
                    genesis_path,
                })
            }
            #[cfg(feature = "storage-service")]
            InnerStorageConfig::Service { endpoint } => {
                let inner_config =
                    linera_storage_service::common::StorageServiceStoreInternalConfig {
                        endpoint: endpoint.clone(),
                        max_concurrent_queries: options.storage_max_concurrent_queries,
                        max_stream_queries: options.storage_max_stream_queries,
                    };
                let config = linera_storage_service::common::StorageServiceStoreConfig {
                    inner_config,
                    storage_cache_config: options.views_storage_cache_config(),
                };
                Ok(StoreConfig::StorageService { config, namespace })
            }
            #[cfg(feature = "rocksdb")]
            InnerStorageConfig::RocksDb { path, spawn_mode } => {
                let path_with_guard = PathWithGuard::new(path.to_path_buf());
                let inner_config = linera_views::rocks_db::RocksDbStoreInternalConfig {
                    spawn_mode: *spawn_mode,
                    path_with_guard,
                    max_stream_queries: options.storage_max_stream_queries,
                };
                let config = linera_views::rocks_db::RocksDbStoreConfig {
                    inner_config,
                    storage_cache_config: options.views_storage_cache_config(),
                };
                Ok(StoreConfig::RocksDb { config, namespace })
            }
            #[cfg(feature = "dynamodb")]
            InnerStorageConfig::DynamoDb { use_dynamodb_local } => {
                let inner_config = linera_views::dynamo_db::DynamoDbStoreInternalConfig {
                    use_dynamodb_local: *use_dynamodb_local,
                    max_concurrent_queries: options.storage_max_concurrent_queries,
                    max_stream_queries: options.storage_max_stream_queries,
                };
                let config = linera_views::dynamo_db::DynamoDbStoreConfig {
                    inner_config,
                    storage_cache_config: options.views_storage_cache_config(),
                };
                Ok(StoreConfig::DynamoDb { config, namespace })
            }
            #[cfg(feature = "scylladb")]
            InnerStorageConfig::ScyllaDb { uri } => {
                let inner_config = linera_views::scylla_db::ScyllaDbStoreInternalConfig {
                    uri: uri.clone(),
                    max_stream_queries: options.storage_max_stream_queries,
                    max_concurrent_queries: options.storage_max_concurrent_queries,
                    replication_factor: options.storage_replication_factor,
                };
                let config = linera_views::scylla_db::ScyllaDbStoreConfig {
                    inner_config,
                    storage_cache_config: options.views_storage_cache_config(),
                };
                Ok(StoreConfig::ScyllaDb { config, namespace })
            }
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            InnerStorageConfig::DualRocksDbScyllaDb {
                path_with_guard,
                spawn_mode,
                uri,
            } => {
                let inner_config = linera_views::rocks_db::RocksDbStoreInternalConfig {
                    spawn_mode: *spawn_mode,
                    path_with_guard: path_with_guard.clone(),
                    max_stream_queries: options.storage_max_stream_queries,
                };
                let first_config = linera_views::rocks_db::RocksDbStoreConfig {
                    inner_config,
                    storage_cache_config: options.views_storage_cache_config(),
                };

                let inner_config = linera_views::scylla_db::ScyllaDbStoreInternalConfig {
                    uri: uri.clone(),
                    max_stream_queries: options.storage_max_stream_queries,
                    max_concurrent_queries: options.storage_max_concurrent_queries,
                    replication_factor: options.storage_replication_factor,
                };
                let second_config = linera_views::scylla_db::ScyllaDbStoreConfig {
                    inner_config,
                    storage_cache_config: options.views_storage_cache_config(),
                };

                let config = DualStoreConfig {
                    first_config,
                    second_config,
                };
                Ok(StoreConfig::DualRocksDbScyllaDb { config, namespace })
            }
        }
    }
}

impl fmt::Display for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let namespace = &self.namespace;
        match &self.inner_storage_config {
            #[cfg(feature = "storage-service")]
            InnerStorageConfig::Service { endpoint } => {
                write!(f, "service:tcp:{endpoint}:{namespace}")
            }
            InnerStorageConfig::Memory { genesis_path } => {
                write!(f, "memory:{}:{}", genesis_path.display(), namespace)
            }
            #[cfg(feature = "rocksdb")]
            InnerStorageConfig::RocksDb { path, spawn_mode } => {
                let spawn_mode = spawn_mode.to_string();
                write!(f, "rocksdb:{}:{}:{}", path.display(), spawn_mode, namespace)
            }
            #[cfg(feature = "dynamodb")]
            InnerStorageConfig::DynamoDb { use_dynamodb_local } => match use_dynamodb_local {
                true => write!(f, "dynamodb:{namespace}:dynamodb_local"),
                false => write!(f, "dynamodb:{namespace}:env"),
            },
            #[cfg(feature = "scylladb")]
            InnerStorageConfig::ScyllaDb { uri } => {
                write!(f, "scylladb:tcp:{uri}:{namespace}")
            }
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            InnerStorageConfig::DualRocksDbScyllaDb {
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

#[test]
fn test_memory_storage_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("memory:path/to/genesis.json").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::Memory {
                genesis_path: PathBuf::from("path/to/genesis.json")
            },
            namespace: DEFAULT_NAMESPACE.into()
        }
    );
    assert_eq!(
        StorageConfig::from_str("memory:path/to/genesis.json:namespace").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::Memory {
                genesis_path: PathBuf::from("path/to/genesis.json")
            },
            namespace: "namespace".into()
        }
    );
    assert!(StorageConfig::from_str("memory").is_err(),);
}

#[cfg(feature = "storage-service")]
#[test]
fn test_shared_store_config_from_str() {
    assert_eq!(
        StorageConfig::from_str("service:tcp:127.0.0.1:8942:linera").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::Service {
                endpoint: "127.0.0.1:8942".to_string()
            },
            namespace: "linera".into()
        }
    );
    assert!(StorageConfig::from_str("service:tcp:127.0.0.1:8942").is_err());
    assert!(StorageConfig::from_str("service:tcp:127.0.0.1:linera").is_err());
}

#[cfg(feature = "rocksdb")]
#[test]
fn test_rocks_db_storage_config_from_str() {
    assert!(StorageConfig::from_str("rocksdb_foo.db").is_err());
    assert_eq!(
        StorageConfig::from_str("rocksdb:foo.db").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::RocksDb {
                path: "foo.db".into(),
                spawn_mode: RocksDbSpawnMode::SpawnBlocking,
            },
            namespace: DEFAULT_NAMESPACE.to_string()
        }
    );
    assert_eq!(
        StorageConfig::from_str("rocksdb:foo.db:block_in_place").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::RocksDb {
                path: "foo.db".into(),
                spawn_mode: RocksDbSpawnMode::BlockInPlace,
            },
            namespace: DEFAULT_NAMESPACE.to_string()
        }
    );
    assert_eq!(
        StorageConfig::from_str("rocksdb:foo.db:block_in_place:chosen_namespace").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::RocksDb {
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
        StorageConfig::from_str("dynamodb:table").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::DynamoDb {
                use_dynamodb_local: false
            },
            namespace: "table".to_string()
        }
    );
    assert_eq!(
        StorageConfig::from_str("dynamodb:table:env").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::DynamoDb {
                use_dynamodb_local: false
            },
            namespace: "table".to_string()
        }
    );
    assert_eq!(
        StorageConfig::from_str("dynamodb:table:dynamodb_local").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::DynamoDb {
                use_dynamodb_local: true
            },
            namespace: "table".to_string()
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
        StorageConfig {
            inner_storage_config: InnerStorageConfig::ScyllaDb {
                uri: "localhost:9042".to_string()
            },
            namespace: DEFAULT_NAMESPACE.to_string()
        }
    );
    assert_eq!(
        StorageConfig::from_str("scylladb:tcp:db_hostname:230:table_other_storage").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::ScyllaDb {
                uri: "db_hostname:230".to_string()
            },
            namespace: "table_other_storage".to_string()
        }
    );
    assert_eq!(
        StorageConfig::from_str("scylladb:tcp:db_hostname:230").unwrap(),
        StorageConfig {
            inner_storage_config: InnerStorageConfig::ScyllaDb {
                uri: "db_hostname:230".to_string()
            },
            namespace: DEFAULT_NAMESPACE.to_string()
        }
    );
    assert!(StorageConfig::from_str("scylladb:-10").is_err());
    assert!(StorageConfig::from_str("scylladb:70000").is_err());
    assert!(StorageConfig::from_str("scylladb:230:234").is_err());
    assert!(StorageConfig::from_str("scylladb:tcp:address1").is_err());
    assert!(StorageConfig::from_str("scylladb:tcp:address1:tcp:/address2").is_err());
    assert!(StorageConfig::from_str("scylladb:wrong").is_err());
}
