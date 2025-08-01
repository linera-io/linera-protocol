// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use async_lock::RwLock;
use linera_storage_service::common::{KeyPrefix, MAX_PAYLOAD_SIZE};
use linera_views::{
    batch::Batch,
    memory::{MemoryDatabase, MemoryStoreConfig},
    store::{KeyValueDatabase, ReadableKeyValueStore, WritableKeyValueStore},
};
#[cfg(with_rocksdb)]
use linera_views::{
    lru_caching::StorageCacheConfig,
    rocks_db::{
        PathWithGuard, RocksDbDatabase, RocksDbSpawnMode, RocksDbStoreConfig,
        RocksDbStoreInternalConfig,
    },
};
use serde::Serialize;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, instrument};
use tracing_subscriber::fmt::format::FmtSpan;

use crate::key_value_store::{
    statement::Operation,
    storage_service_server::{StorageService, StorageServiceServer},
    KeyValue, OptValue, ReplyContainsKey, ReplyContainsKeys, ReplyExistsNamespace,
    ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix, ReplyListAll, ReplyListRootKeys,
    ReplyReadMultiValues, ReplyReadValue, ReplySpecificChunk, RequestContainsKey,
    RequestContainsKeys, RequestCreateNamespace, RequestDeleteNamespace, RequestExistsNamespace,
    RequestFindKeyValuesByPrefix, RequestFindKeysByPrefix, RequestListRootKeys,
    RequestReadMultiValues, RequestReadValue, RequestSpecificChunk, RequestWriteBatchExtended,
};

pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

enum LocalStore {
    Memory(<MemoryDatabase as KeyValueDatabase>::Store),
    /// The RocksDB key value store
    #[cfg(with_rocksdb)]
    RocksDb(<RocksDbDatabase as KeyValueDatabase>::Store),
}

#[derive(Default)]
struct BigRead {
    num_processed_chunks: usize,
    chunks: Vec<Vec<u8>>,
}

#[derive(Default)]
struct PendingBigReads {
    index: i64,
    big_reads: BTreeMap<i64, BigRead>,
}

struct StorageServer {
    store: LocalStore,
    pending_big_puts: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    pending_big_reads: Arc<RwLock<PendingBigReads>>,
}

impl StorageServer {
    pub async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Status> {
        match &self.store {
            LocalStore::Memory(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|e| Status::unknown(format!("Memory error {:?} at read_value_bytes", e))),
            #[cfg(with_rocksdb)]
            LocalStore::RocksDb(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|e| Status::unknown(format!("RocksDB error {:?} at read_value_bytes", e))),
        }
    }

    pub async fn contains_key(&self, key: &[u8]) -> Result<bool, Status> {
        match &self.store {
            LocalStore::Memory(store) => store
                .contains_key(key)
                .await
                .map_err(|e| Status::unknown(format!("Memory error {:?} at contains_key", e))),
            #[cfg(with_rocksdb)]
            LocalStore::RocksDb(store) => store
                .contains_key(key)
                .await
                .map_err(|e| Status::unknown(format!("RocksDB error {:?} at contains_key", e))),
        }
    }

    pub async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Status> {
        match &self.store {
            LocalStore::Memory(store) => store
                .contains_keys(keys)
                .await
                .map_err(|e| Status::unknown(format!("Memory error {:?} at contains_keys", e))),
            #[cfg(with_rocksdb)]
            LocalStore::RocksDb(store) => store
                .contains_keys(keys)
                .await
                .map_err(|e| Status::unknown(format!("RocksDB error {:?} at contains_keys", e))),
        }
    }

    pub async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Status> {
        match &self.store {
            LocalStore::Memory(store) => store.read_multi_values_bytes(keys).await.map_err(|e| {
                Status::unknown(format!("Memory error {:?} at read_multi_values_bytes", e))
            }),
            #[cfg(with_rocksdb)]
            LocalStore::RocksDb(store) => store.read_multi_values_bytes(keys).await.map_err(|e| {
                Status::unknown(format!("RocksDB error {:?} at read_multi_values_bytes", e))
            }),
        }
    }

    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Status> {
        match &self.store {
            LocalStore::Memory(store) => store.find_keys_by_prefix(key_prefix).await.map_err(|e| {
                Status::unknown(format!("Memory error {:?} at find_keys_by_prefix", e))
            }),
            #[cfg(with_rocksdb)]
            LocalStore::RocksDb(store) => {
                store.find_keys_by_prefix(key_prefix).await.map_err(|e| {
                    Status::unknown(format!("RocksDB error {:?} at find_keys_by_prefix", e))
                })
            }
        }
    }

    pub async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Status> {
        match &self.store {
            LocalStore::Memory(store) => {
                store
                    .find_key_values_by_prefix(key_prefix)
                    .await
                    .map_err(|e| {
                        Status::unknown(format!(
                            "Memory error {:?} at find_key_values_by_prefix",
                            e
                        ))
                    })
            }
            #[cfg(with_rocksdb)]
            LocalStore::RocksDb(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(|e| {
                    Status::unknown(format!(
                        "RocksDB error {:?} at find_key_values_by_prefix",
                        e
                    ))
                }),
        }
    }

    pub async fn write_batch(&self, batch: Batch) -> Result<(), Status> {
        match &self.store {
            LocalStore::Memory(store) => store
                .write_batch(batch)
                .await
                .map_err(|e| Status::unknown(format!("Memory error {:?} at write_batch", e))),
            #[cfg(with_rocksdb)]
            LocalStore::RocksDb(store) => store
                .write_batch(batch)
                .await
                .map_err(|e| Status::unknown(format!("RocksDB error {:?} at write_batch", e))),
        }
    }

    pub async fn list_all(&self) -> Result<Vec<Vec<u8>>, Status> {
        self.find_keys_by_prefix(&[KeyPrefix::Namespace as u8])
            .await
    }

    pub async fn list_root_keys(&self, namespace: &[u8]) -> Result<Vec<Vec<u8>>, Status> {
        let mut full_key = vec![KeyPrefix::RootKey as u8];
        full_key.extend(namespace);
        self.find_keys_by_prefix(&full_key).await
    }

    pub async fn delete_all(&self) -> Result<(), Status> {
        let mut batch = Batch::new();
        batch.delete_key_prefix(vec![KeyPrefix::Key as u8]);
        batch.delete_key_prefix(vec![KeyPrefix::Namespace as u8]);
        batch.delete_key_prefix(vec![KeyPrefix::RootKey as u8]);
        self.write_batch(batch).await
    }

    pub async fn exists_namespace(&self, namespace: &[u8]) -> Result<bool, Status> {
        let mut full_key = vec![KeyPrefix::Namespace as u8];
        full_key.extend(namespace);
        self.contains_key(&full_key).await
    }

    pub async fn create_namespace(&self, namespace: &[u8]) -> Result<(), Status> {
        let mut full_key = vec![KeyPrefix::Namespace as u8];
        full_key.extend(namespace);
        let mut batch = Batch::new();
        batch.put_key_value_bytes(full_key, vec![]);
        self.write_batch(batch).await
    }

    pub async fn delete_namespace(&self, namespace: &[u8]) -> Result<(), Status> {
        let mut batch = Batch::new();
        let mut full_key = vec![KeyPrefix::Namespace as u8];
        full_key.extend(namespace);
        batch.delete_key(full_key);
        let mut key_prefix = vec![KeyPrefix::Key as u8];
        key_prefix.extend(namespace);
        batch.delete_key_prefix(key_prefix);
        let mut key_prefix = vec![KeyPrefix::RootKey as u8];
        key_prefix.extend(namespace);
        batch.delete_key_prefix(key_prefix);
        self.write_batch(batch).await
    }

    pub async fn insert_pending_read<S: Serialize>(&self, value: S) -> (i64, i32) {
        let value = bcs::to_bytes(&value).unwrap();
        let chunks = value
            .chunks(MAX_PAYLOAD_SIZE)
            .map(|x| x.to_vec())
            .collect::<Vec<_>>();
        let num_chunks = chunks.len() as i32;
        let mut pending_big_reads = self.pending_big_reads.write().await;
        let message_index = pending_big_reads.index;
        pending_big_reads.index += 1;
        let big_read = BigRead {
            num_processed_chunks: 0,
            chunks,
        };
        pending_big_reads.big_reads.insert(message_index, big_read);
        (message_index, num_chunks)
    }
}

#[derive(clap::Parser)]
#[command(
    name = "linera-storage-server",
    version = linera_version::VersionInfo::default_clap_str(),
    about = "A server providing storage service",
)]
enum StorageServerOptions {
    #[command(name = "memory")]
    Memory {
        /// The storage namespace.
        #[arg(long, default_value = "linera_storage_service")]
        namespace: String,
        /// The storage service address.
        #[arg(long)]
        endpoint: String,
        /// Preferred buffer size for async streams.
        #[arg(long, default_value = "10")]
        max_stream_queries: usize,
    },

    #[cfg(with_rocksdb)]
    #[command(name = "rocksdb")]
    RocksDb {
        /// The storage namespace.
        #[arg(long, default_value = "linera_storage_service")]
        namespace: String,
        /// The storage service address.
        #[arg(long)]
        endpoint: String,
        /// Path to the rocksdb database.
        #[arg(long)]
        path: String,
        /// Preferred buffer size for async streams.
        #[arg(long, default_value = "10")]
        max_stream_queries: usize,
        /// The maximum size of the cache, in bytes (keys size + value sizes)
        #[arg(long, default_value = "10000000")]
        max_cache_size: usize,
        /// The maximum size of an entry size, in bytes
        #[arg(long, default_value = "1000000")]
        max_entry_size: usize,
        /// The maximum number of entries in the cache.
        #[arg(long, default_value = "1000")]
        max_cache_entries: usize,
    },
}

#[tonic::async_trait]
impl StorageService for StorageServer {
    #[instrument(target = "store_server", skip_all, err, fields(key_len = ?request.get_ref().key.len()))]
    async fn process_read_value(
        &self,
        request: Request<RequestReadValue>,
    ) -> Result<Response<ReplyReadValue>, Status> {
        let request = request.into_inner();
        let RequestReadValue { key } = request;
        let value = self.read_value_bytes(&key).await?;
        let size = match &value {
            None => 0,
            Some(value) => value.len(),
        };
        let response = if size < MAX_PAYLOAD_SIZE {
            ReplyReadValue {
                value,
                message_index: 0,
                num_chunks: 0,
            }
        } else {
            let (message_index, num_chunks) = self.insert_pending_read(value).await;
            ReplyReadValue {
                value: None,
                message_index,
                num_chunks,
            }
        };
        Ok(Response::new(response))
    }

    #[instrument(target = "store_server", skip_all, err, fields(key_len = ?request.get_ref().key.len()))]
    async fn process_contains_key(
        &self,
        request: Request<RequestContainsKey>,
    ) -> Result<Response<ReplyContainsKey>, Status> {
        let request = request.into_inner();
        let RequestContainsKey { key } = request;
        let test = self.contains_key(&key).await?;
        let response = ReplyContainsKey { test };
        Ok(Response::new(response))
    }

    #[instrument(target = "store_server", skip_all, err, fields(key_len = ?request.get_ref().keys.len()))]
    async fn process_contains_keys(
        &self,
        request: Request<RequestContainsKeys>,
    ) -> Result<Response<ReplyContainsKeys>, Status> {
        let request = request.into_inner();
        let RequestContainsKeys { keys } = request;
        let tests = self.contains_keys(keys).await?;
        let response = ReplyContainsKeys { tests };
        Ok(Response::new(response))
    }

    #[instrument(target = "store_server", skip_all, err, fields(n_keys = ?request.get_ref().keys.len()))]
    async fn process_read_multi_values(
        &self,
        request: Request<RequestReadMultiValues>,
    ) -> Result<Response<ReplyReadMultiValues>, Status> {
        let request = request.into_inner();
        let RequestReadMultiValues { keys } = request;
        let values = self.read_multi_values_bytes(keys.clone()).await?;
        let size = values
            .iter()
            .map(|x| match x {
                None => 0,
                Some(entry) => entry.len(),
            })
            .sum::<usize>();
        let response = if size < MAX_PAYLOAD_SIZE {
            let values = values
                .into_iter()
                .map(|value| OptValue { value })
                .collect::<Vec<_>>();
            ReplyReadMultiValues {
                values,
                message_index: 0,
                num_chunks: 0,
            }
        } else {
            let (message_index, num_chunks) = self.insert_pending_read(values).await;
            ReplyReadMultiValues {
                values: Vec::default(),
                message_index,
                num_chunks,
            }
        };
        Ok(Response::new(response))
    }

    #[instrument(target = "store_server", skip_all, err, fields(key_prefix_len = ?request.get_ref().key_prefix.len()))]
    async fn process_find_keys_by_prefix(
        &self,
        request: Request<RequestFindKeysByPrefix>,
    ) -> Result<Response<ReplyFindKeysByPrefix>, Status> {
        let request = request.into_inner();
        let RequestFindKeysByPrefix { key_prefix } = request;
        let keys = self.find_keys_by_prefix(&key_prefix).await?;
        let size = keys.iter().map(|x| x.len()).sum::<usize>();
        let response = if size < MAX_PAYLOAD_SIZE {
            ReplyFindKeysByPrefix {
                keys,
                message_index: 0,
                num_chunks: 0,
            }
        } else {
            let (message_index, num_chunks) = self.insert_pending_read(keys).await;
            ReplyFindKeysByPrefix {
                keys: Vec::default(),
                message_index,
                num_chunks,
            }
        };
        Ok(Response::new(response))
    }

    #[instrument(target = "store_server", skip_all, err, fields(key_prefix_len = ?request.get_ref().key_prefix.len()))]
    async fn process_find_key_values_by_prefix(
        &self,
        request: Request<RequestFindKeyValuesByPrefix>,
    ) -> Result<Response<ReplyFindKeyValuesByPrefix>, Status> {
        let request = request.into_inner();
        let RequestFindKeyValuesByPrefix { key_prefix } = request;
        let key_values = self.find_key_values_by_prefix(&key_prefix).await?;
        let size = key_values
            .iter()
            .map(|x| x.0.len() + x.1.len())
            .sum::<usize>();
        let response = if size < MAX_PAYLOAD_SIZE {
            let key_values = key_values
                .into_iter()
                .map(|x| KeyValue {
                    key: x.0,
                    value: x.1,
                })
                .collect::<Vec<_>>();
            ReplyFindKeyValuesByPrefix {
                key_values,
                message_index: 0,
                num_chunks: 0,
            }
        } else {
            let (message_index, num_chunks) = self.insert_pending_read(key_values).await;
            ReplyFindKeyValuesByPrefix {
                key_values: Vec::default(),
                message_index,
                num_chunks,
            }
        };
        Ok(Response::new(response))
    }

    #[instrument(target = "store_server", skip_all, err, fields(n_statements = ?request.get_ref().statements.len()))]
    async fn process_write_batch_extended(
        &self,
        request: Request<RequestWriteBatchExtended>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let RequestWriteBatchExtended { statements } = request;
        let mut batch = Batch::default();
        for statement in statements {
            match statement.operation.unwrap() {
                Operation::Delete(key) => {
                    batch.delete_key(key);
                }
                Operation::Put(key_value) => {
                    batch.put_key_value_bytes(key_value.key, key_value.value);
                }
                Operation::Append(key_value_append) => {
                    let mut pending_big_puts = self.pending_big_puts.write().await;
                    match pending_big_puts.get_mut(&key_value_append.key) {
                        None => {
                            pending_big_puts
                                .insert(key_value_append.key.clone(), key_value_append.value);
                        }
                        Some(value) => {
                            value.extend(key_value_append.value);
                        }
                    }
                    if key_value_append.last {
                        let value = pending_big_puts.remove(&key_value_append.key).unwrap();
                        batch.put_key_value_bytes(key_value_append.key, value);
                    }
                }
                Operation::DeletePrefix(key_prefix) => {
                    batch.delete_key_prefix(key_prefix);
                }
            }
        }
        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }
        Ok(Response::new(()))
    }

    #[instrument(target = "store_server", skip_all, err, fields(message_index = ?request.get_ref().message_index, index = ?request.get_ref().index))]
    async fn process_specific_chunk(
        &self,
        request: Request<RequestSpecificChunk>,
    ) -> Result<Response<ReplySpecificChunk>, Status> {
        let request = request.into_inner();
        let RequestSpecificChunk {
            message_index,
            index,
        } = request;
        let mut pending_big_reads = self.pending_big_reads.write().await;
        let Some(entry) = pending_big_reads.big_reads.get_mut(&message_index) else {
            return Err(Status::not_found("process_specific_chunk"));
        };
        let index = index as usize;
        let chunk = entry.chunks[index].clone();
        entry.num_processed_chunks += 1;
        if entry.chunks.len() == entry.num_processed_chunks {
            pending_big_reads.big_reads.remove(&message_index);
        }
        let response = ReplySpecificChunk { chunk };
        Ok(Response::new(response))
    }

    #[instrument(target = "store_server", skip_all, err, fields(namespace = ?request.get_ref().namespace))]
    async fn process_create_namespace(
        &self,
        request: Request<RequestCreateNamespace>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let RequestCreateNamespace { namespace } = request;
        self.create_namespace(&namespace).await?;
        Ok(Response::new(()))
    }

    #[instrument(target = "store_server", skip_all, err, fields(namespace = ?request.get_ref().namespace))]
    async fn process_exists_namespace(
        &self,
        request: Request<RequestExistsNamespace>,
    ) -> Result<Response<ReplyExistsNamespace>, Status> {
        let request = request.into_inner();
        let RequestExistsNamespace { namespace } = request;
        let exists = self.exists_namespace(&namespace).await?;
        let response = ReplyExistsNamespace { exists };
        Ok(Response::new(response))
    }

    #[instrument(target = "store_server", skip_all, err, fields(namespace = ?request.get_ref().namespace))]
    async fn process_delete_namespace(
        &self,
        request: Request<RequestDeleteNamespace>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let RequestDeleteNamespace { namespace } = request;
        self.delete_namespace(&namespace).await?;
        Ok(Response::new(()))
    }

    #[instrument(target = "store_server", skip_all, err, fields(list_all = "list_all"))]
    async fn process_list_all(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ReplyListAll>, Status> {
        let namespaces = self.list_all().await?;
        let response = ReplyListAll { namespaces };
        Ok(Response::new(response))
    }

    #[instrument(
        target = "store_server",
        skip_all,
        err,
        fields(list_all = "list_root_keys")
    )]
    async fn process_list_root_keys(
        &self,
        request: Request<RequestListRootKeys>,
    ) -> Result<Response<ReplyListRootKeys>, Status> {
        let request = request.into_inner();
        let RequestListRootKeys { namespace } = request;
        let root_keys = self.list_root_keys(&namespace).await?;
        let response = ReplyListRootKeys { root_keys };
        Ok(Response::new(response))
    }

    #[instrument(
        target = "store_server",
        skip_all,
        err,
        fields(delete_all = "delete_all")
    )]
    async fn process_delete_all(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        self.delete_all().await?;
        Ok(Response::new(()))
    }
}

#[tokio::main]
async fn main() {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    let internal_event_filter = {
        match std::env::var_os("RUST_LOG_SPAN_EVENTS") {
            Some(mut value) => {
                value.make_ascii_lowercase();
                let value = value
                    .to_str()
                    .expect("test-log: RUST_LOG_SPAN_EVENTS must be valid UTF-8");
                value
                    .split(',')
                    .map(|filter| match filter.trim() {
                        "new" => FmtSpan::NEW,
                        "enter" => FmtSpan::ENTER,
                        "exit" => FmtSpan::EXIT,
                        "close" => FmtSpan::CLOSE,
                        "active" => FmtSpan::ACTIVE,
                        "full" => FmtSpan::FULL,
                        _ => panic!("test-log: RUST_LOG_SPAN_EVENTS must contain filters separated by `,`.\n\t\
                                     For example: `active` or `new,close`\n\t\
                                     Supported filters: new, enter, exit, close, active, full\n\t\
                                     Got: {}", value),
                    })
                    .fold(FmtSpan::NONE, |acc, filter| filter | acc)
            }
            None => FmtSpan::NONE,
        }
    };
    tracing_subscriber::fmt()
        .with_span_events(internal_event_filter)
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let options = <StorageServerOptions as clap::Parser>::parse();
    let (store, endpoint) = match options {
        StorageServerOptions::Memory {
            namespace,
            endpoint,
            max_stream_queries,
        } => {
            let config = MemoryStoreConfig {
                max_stream_queries,
                kill_on_drop: false,
            };
            let database = MemoryDatabase::maybe_create_and_connect(&config, &namespace)
                .await
                .unwrap();
            let store = database.open_shared(&[]).unwrap();
            let store = LocalStore::Memory(store);
            (store, endpoint)
        }

        #[cfg(with_rocksdb)]
        StorageServerOptions::RocksDb {
            namespace,
            endpoint,
            path,
            max_stream_queries,
            max_cache_size,
            max_entry_size,
            max_cache_entries,
        } => {
            let path_buf = path.into();
            let path_with_guard = PathWithGuard::new(path_buf);
            let spawn_mode = RocksDbSpawnMode::get_spawn_mode_from_runtime();
            let inner_config = RocksDbStoreInternalConfig {
                spawn_mode,
                path_with_guard,
                max_stream_queries,
            };
            let storage_cache_config = StorageCacheConfig {
                max_cache_size,
                max_entry_size,
                max_cache_entries,
            };
            let config = RocksDbStoreConfig {
                inner_config,
                storage_cache_config,
            };
            let database = RocksDbDatabase::maybe_create_and_connect(&config, &namespace)
                .await
                .expect("store");
            let store = database.open_shared(&[]).expect("Failed to open store");
            let store = LocalStore::RocksDb(store);
            (store, endpoint)
        }
    };
    let pending_big_puts = Arc::new(RwLock::new(BTreeMap::default()));
    let pending_big_reads = Arc::new(RwLock::new(PendingBigReads::default()));
    let store = StorageServer {
        store,
        pending_big_puts,
        pending_big_reads,
    };
    let endpoint = endpoint.parse().unwrap();
    info!("Starting linera_storage_service on endpoint={}", endpoint);
    Server::builder()
        .add_service(StorageServiceServer::new(store))
        .serve(endpoint)
        .await
        .expect("a successful running of the server");
}
