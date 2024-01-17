// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::key_value_store::{
    statement::Operation,
    store_processor_server::{StoreProcessor, StoreProcessorServer},
    store_reply::Reply,
    store_request::Query,
    KeyValue, KeyValues, Keys, OptValue, OptValues, StoreReply, StoreRequest,
};
use linera_service::storage::{StorageConfig, StoreConfig};
use linera_views::{
    common::{CommonStoreConfig, ReadableKeyValueStore, WritableKeyValueStore},
    memory::MemoryStore,
    rocks_db::RocksDbStore,
    views::ViewError,
};
use tonic::{transport::Server, Request, Response, Status};

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

pub enum SharedStoreServer {
    Memory(MemoryStore),
    /// The RocksDb key value store
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbStore),
}

impl SharedStoreServer {
    async fn new(store_config: StoreConfig) -> Result<Self, ViewError> {
        match store_config {
            StoreConfig::Memory(memory_store_config) => {
                let store = MemoryStore::new(memory_store_config);
                Ok(SharedStoreServer::Memory(store))
            }
            #[cfg(feature = "rocksdb")]
            StoreConfig::RocksDb(rocksdb_store_config) => {
                let store = RocksDbStore::new(rocksdb_store_config).await?;
                Ok(SharedStoreServer::RocksDb(store.0))
            }
            #[cfg(feature = "aws")]
            StoreConfig::DynamoDb(_dynamodb_store_config) => {
                panic!("DynamoDb not supported in the shared system since it is already shared");
            }
            #[cfg(feature = "scylladb")]
            StoreConfig::ScyllaDb(_scylladb_store_config) => {
                panic!("ScyllaDb not supported in the shared system since it is already shared");
            }
        }
    }

    pub async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Status> {
        match self {
            SharedStoreServer::Memory(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|_e| Status::not_found("read_value_bytes")),
            #[cfg(feature = "rocksdb")]
            SharedStoreServer::RocksDb(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|_e| Status::not_found("read_value_bytes")),
        }
    }

    pub async fn contains_key(&self, key: &[u8]) -> Result<bool, Status> {
        match self {
            SharedStoreServer::Memory(store) => store
                .contains_key(key)
                .await
                .map_err(|_e| Status::not_found("contains_key")),
            #[cfg(feature = "rocksdb")]
            SharedStoreServer::RocksDb(store) => store
                .contains_key(key)
                .await
                .map_err(|_e| Status::not_found("contains_key")),
        }
    }

    pub async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Status> {
        match self {
            SharedStoreServer::Memory(store) => store
                .read_multi_values_bytes(keys)
                .await
                .map_err(|_e| Status::not_found("read_multi_values_bytes")),
            #[cfg(feature = "rocksdb")]
            SharedStoreServer::RocksDb(store) => store
                .read_multi_values_bytes(keys)
                .await
                .map_err(|_e| Status::not_found("read_multi_values_bytes")),
        }
    }

    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Status> {
        match self {
            SharedStoreServer::Memory(store) => store
                .find_keys_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_keys_by_prefix")),
            #[cfg(feature = "rocksdb")]
            SharedStoreServer::RocksDb(store) => store
                .find_keys_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_keys_by_prefix")),
        }
    }

    pub async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Status> {
        match self {
            SharedStoreServer::Memory(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_key_values_by_prefix")),
            #[cfg(feature = "rocksdb")]
            SharedStoreServer::RocksDb(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_key_values_by_prefix")),
        }
    }

    pub async fn write_batch(
        &self,
        batch: linera_views::batch::Batch,
        base_key: &[u8],
    ) -> Result<(), Status> {
        match self {
            SharedStoreServer::Memory(store) => store
                .write_batch(batch, base_key)
                .await
                .map_err(|_e| Status::not_found("write_batch")),
            #[cfg(feature = "rocksdb")]
            SharedStoreServer::RocksDb(store) => store
                .write_batch(batch, base_key)
                .await
                .map_err(|_e| Status::not_found("write_batch")),
        }
    }

    pub async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Status> {
        match self {
            SharedStoreServer::Memory(store) => store
                .clear_journal(base_key)
                .await
                .map_err(|_e| Status::not_found("clear_journal")),
            #[cfg(feature = "rocksdb")]
            SharedStoreServer::RocksDb(store) => store
                .clear_journal(base_key)
                .await
                .map_err(|_e| Status::not_found("clear_journal")),
        }
    }
}

#[derive(clap::Parser)]
struct SharedStoreServerOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[arg(long = "storage")]
    storage_config: String,

    #[arg(long = "endpoint")]
    endpoint: String,
}

#[tonic::async_trait]
impl StoreProcessor for SharedStoreServer {
    async fn store_process(
        &self,
        request: Request<StoreRequest>,
    ) -> Result<Response<StoreReply>, Status> {
        use crate::key_value_store::UnitType;
        let request = request.into_inner();
        let response = match request.query.unwrap() {
            Query::ReadValue(key) => {
                let value = self.read_value_bytes(&key).await?;
                let value = OptValue { value };
                StoreReply {
                    reply: Some(Reply::ReadValue(value)),
                }
            }
            Query::ContainsKey(key) => {
                let test = self.contains_key(&key).await?;
                StoreReply {
                    reply: Some(Reply::ContainsKey(test)),
                }
            }
            Query::ReadMultiValues(keys) => {
                let values = self.read_multi_values_bytes(keys.keys).await?;
                let values = values
                    .into_iter()
                    .map(|value| OptValue { value })
                    .collect::<Vec<_>>();
                let values = OptValues { values };
                StoreReply {
                    reply: Some(Reply::ReadMultiValues(values)),
                }
            }
            Query::FindKeysByPrefix(key_prefix) => {
                let keys = self.find_keys_by_prefix(&key_prefix).await?;
                let keys = Keys { keys };
                StoreReply {
                    reply: Some(Reply::FindKeysByPrefix(keys)),
                }
            }
            Query::FindKeyValuesByPrefix(key_prefix) => {
                let key_values = self.find_key_values_by_prefix(&key_prefix).await?;
                let key_values = key_values
                    .into_iter()
                    .map(|x| KeyValue {
                        key: x.0,
                        value: x.1,
                    })
                    .collect::<Vec<_>>();
                let key_values = KeyValues { key_values };
                StoreReply {
                    reply: Some(Reply::FindKeyValuesByPrefix(key_values)),
                }
            }
            Query::WriteBatch(batch) => {
                let mut new_batch = linera_views::batch::Batch::default();
                for statement in batch.statements {
                    match statement.operation.unwrap() {
                        Operation::Delete(key) => {
                            new_batch.delete_key(key);
                        }
                        Operation::Put(key_value) => {
                            new_batch.put_key_value_bytes(key_value.key, key_value.value);
                        }
                        Operation::DeletePrefix(key_prefix) => {
                            new_batch.delete_key_prefix(key_prefix);
                        }
                    }
                }
                let base_key = batch.base_key;
                self.write_batch(new_batch, &base_key).await?;
                StoreReply {
                    reply: Some(Reply::WriteBatch(UnitType {})),
                }
            }
            Query::ClearJournal(base_key) => {
                self.clear_journal(&base_key).await?;
                StoreReply {
                    reply: Some(Reply::ClearJournal(UnitType {})),
                }
            }
        };
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = <SharedStoreServerOptions as clap::Parser>::parse();

    let storage_config = options.storage_config;
    let common_config = CommonStoreConfig::default();
    let storage_config: StorageConfig = storage_config.parse()?;
    let full_storage_config = storage_config.add_common_config(common_config).await?;

    let endpoint = options.endpoint;
    let endpoint = endpoint.parse().unwrap();
    let shared_store = SharedStoreServer::new(full_storage_config)
        .await
        .expect("A shared_store");

    Server::builder()
        .add_service(StoreProcessorServer::new(shared_store))
        .serve(endpoint)
        .await?;

    Ok(())
}
