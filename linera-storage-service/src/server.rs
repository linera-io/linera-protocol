// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::key_value_store::{
    statement::Operation,
    store_processor_server::{StoreProcessor, StoreProcessorServer},
    KeyValue, OptValue, ReplyClearJournal, ReplyContainsKey, ReplyCreateNamespace, ReplyDeleteAll,
    ReplyDeleteNamespace, ReplyExistNamespace, ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix,
    ReplyListAll, ReplyReadMultiValues, ReplyReadValue, ReplyWriteBatch, RequestClearJournal,
    RequestContainsKey, RequestCreateNamespace, RequestDeleteAll, RequestDeleteNamespace,
    RequestExistNamespace, RequestFindKeyValuesByPrefix, RequestFindKeysByPrefix, RequestListAll,
    RequestReadMultiValues, RequestReadValue, RequestWriteBatch,
};
use linera_views::{
    batch::Batch,
    common::{AdminKeyValueStore, CommonStoreConfig, ReadableKeyValueStore, WritableKeyValueStore},
    memory::{create_memory_store_stream_queries, MemoryStore},
    rocks_db::{RocksDbStore, RocksDbStoreConfig},
};
use tonic::{transport::Server, Request, Response, Status};

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

pub enum ServiceStoreServer {
    Memory(MemoryStore),
    /// The RocksDb key value store
    #[cfg(feature = "rocksdb")]
    RocksDb(RocksDbStore),
}

impl ServiceStoreServer {
    pub async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Status> {
        match self {
            ServiceStoreServer::Memory(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|_e| Status::not_found("read_value_bytes")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|_e| Status::not_found("read_value_bytes")),
        }
    }

    pub async fn contains_key(&self, key: &[u8]) -> Result<bool, Status> {
        match self {
            ServiceStoreServer::Memory(store) => store
                .contains_key(key)
                .await
                .map_err(|_e| Status::not_found("contains_key")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
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
            ServiceStoreServer::Memory(store) => store
                .read_multi_values_bytes(keys)
                .await
                .map_err(|_e| Status::not_found("read_multi_values_bytes")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .read_multi_values_bytes(keys)
                .await
                .map_err(|_e| Status::not_found("read_multi_values_bytes")),
        }
    }

    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Status> {
        match self {
            ServiceStoreServer::Memory(store) => store
                .find_keys_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_keys_by_prefix")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
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
            ServiceStoreServer::Memory(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_key_values_by_prefix")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(|_e| Status::not_found("find_key_values_by_prefix")),
        }
    }

    pub async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Status> {
        match self {
            ServiceStoreServer::Memory(store) => store
                .write_batch(batch, base_key)
                .await
                .map_err(|_e| Status::not_found("write_batch")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .write_batch(batch, base_key)
                .await
                .map_err(|_e| Status::not_found("write_batch")),
        }
    }

    pub async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Status> {
        match self {
            ServiceStoreServer::Memory(store) => store
                .clear_journal(base_key)
                .await
                .map_err(|_e| Status::not_found("clear_journal")),
            #[cfg(feature = "rocksdb")]
            ServiceStoreServer::RocksDb(store) => store
                .clear_journal(base_key)
                .await
                .map_err(|_e| Status::not_found("clear_journal")),
        }
    }

    pub async fn list_all(&self) -> Result<Vec<Vec<u8>>, Status> {
        self.find_keys_by_prefix(&[1]).await
    }

    pub async fn delete_all(&self) -> Result<(), Status> {
        let mut batch = Batch::new();
        batch.delete_key_prefix(vec![0]);
        batch.delete_key_prefix(vec![1]);
        let base_key = vec![];
        self.write_batch(batch, &base_key).await?;
        self.clear_journal(&base_key).await
    }

    pub async fn exist_namespace(&self, namespace: &[u8]) -> Result<bool, Status> {
        let mut full_key = vec![1];
        full_key.extend(namespace);
        self.contains_key(&full_key).await
    }

    pub async fn create_namespace(&self, namespace: &[u8]) -> Result<(), Status> {
        let mut full_key = vec![1];
        full_key.extend(namespace);
        let mut batch = Batch::new();
        batch.put_key_value_bytes(full_key, vec![]);
        let base_key = vec![];
        self.write_batch(batch, &base_key).await?;
        self.clear_journal(&base_key).await
    }

    pub async fn delete_namespace(&self, namespace: &[u8]) -> Result<(), Status> {
        let mut batch = Batch::new();
        let mut full_key = vec![1];
        full_key.extend(namespace);
        batch.delete_key(full_key);
        let mut key_prefix = vec![0];
        key_prefix.extend(namespace);
        batch.delete_key_prefix(key_prefix);
        let base_key = vec![];
        self.write_batch(batch, &base_key).await?;
        self.clear_journal(&base_key).await
    }
}

#[derive(clap::Parser)]
enum ServiceStoreServerOptions {
    #[command(name = "memory")]
    Memory {
        #[arg(long = "endpoint")]
        endpoint: String,
    },

    #[command(name = "rocksdb")]
    RocksDb {
        #[arg(long = "endpoint")]
        path: String,
        #[arg(long = "endpoint")]
        endpoint: String,
    },
}

#[tonic::async_trait]
impl StoreProcessor for ServiceStoreServer {
    async fn process_read_value(
        &self,
        request: Request<RequestReadValue>,
    ) -> Result<Response<ReplyReadValue>, Status> {
        let request = request.into_inner();
        let RequestReadValue { key } = request;
        let value = self.read_value_bytes(&key).await?;
        let response = ReplyReadValue { value };
        Ok(Response::new(response))
    }

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

    async fn process_read_multi_values(
        &self,
        request: Request<RequestReadMultiValues>,
    ) -> Result<Response<ReplyReadMultiValues>, Status> {
        let request = request.into_inner();
        let RequestReadMultiValues { keys } = request;
        let values = self.read_multi_values_bytes(keys).await?;
        let values = values
            .into_iter()
            .map(|value| OptValue { value })
            .collect::<Vec<_>>();
        let response = ReplyReadMultiValues { values };
        Ok(Response::new(response))
    }

    async fn process_find_keys_by_prefix(
        &self,
        request: Request<RequestFindKeysByPrefix>,
    ) -> Result<Response<ReplyFindKeysByPrefix>, Status> {
        let request = request.into_inner();
        let RequestFindKeysByPrefix { key_prefix } = request;
        let keys = self.find_keys_by_prefix(&key_prefix).await?;
        let response = ReplyFindKeysByPrefix { keys };
        Ok(Response::new(response))
    }

    async fn process_find_key_values_by_prefix(
        &self,
        request: Request<RequestFindKeyValuesByPrefix>,
    ) -> Result<Response<ReplyFindKeyValuesByPrefix>, Status> {
        let request = request.into_inner();
        let RequestFindKeyValuesByPrefix { key_prefix } = request;
        let key_values = self.find_key_values_by_prefix(&key_prefix).await?;
        let key_values = key_values
            .into_iter()
            .map(|x| KeyValue {
                key: x.0,
                value: x.1,
            })
            .collect::<Vec<_>>();
        let response = ReplyFindKeyValuesByPrefix { key_values };
        Ok(Response::new(response))
    }

    async fn process_write_batch(
        &self,
        request: Request<RequestWriteBatch>,
    ) -> Result<Response<ReplyWriteBatch>, Status> {
        let request = request.into_inner();
        let RequestWriteBatch {
            statements,
            base_key,
        } = request;
        let mut batch = Batch::default();
        for statement in statements {
            match statement.operation.unwrap() {
                Operation::Delete(key) => {
                    batch.delete_key(key);
                }
                Operation::Put(key_value) => {
                    batch.put_key_value_bytes(key_value.key, key_value.value);
                }
                Operation::DeletePrefix(key_prefix) => {
                    batch.delete_key_prefix(key_prefix);
                }
            }
        }
        self.write_batch(batch, &base_key).await?;
        let response = ReplyWriteBatch {};
        Ok(Response::new(response))
    }

    async fn process_clear_journal(
        &self,
        request: Request<RequestClearJournal>,
    ) -> Result<Response<ReplyClearJournal>, Status> {
        let request = request.into_inner();
        let RequestClearJournal { base_key } = request;
        self.clear_journal(&base_key).await?;
        let response = ReplyClearJournal {};
        Ok(Response::new(response))
    }

    async fn process_create_namespace(
        &self,
        request: Request<RequestCreateNamespace>,
    ) -> Result<Response<ReplyCreateNamespace>, Status> {
        let request = request.into_inner();
        let RequestCreateNamespace { namespace } = request;
        self.create_namespace(&namespace).await?;
        let response = ReplyCreateNamespace {};
        Ok(Response::new(response))
    }

    async fn process_exist_namespace(
        &self,
        request: Request<RequestExistNamespace>,
    ) -> Result<Response<ReplyExistNamespace>, Status> {
        let request = request.into_inner();
        let RequestExistNamespace { namespace } = request;
        let test = self.exist_namespace(&namespace).await?;
        let response = ReplyExistNamespace { test };
        Ok(Response::new(response))
    }

    async fn process_delete_namespace(
        &self,
        request: Request<RequestDeleteNamespace>,
    ) -> Result<Response<ReplyDeleteNamespace>, Status> {
        let request = request.into_inner();
        let RequestDeleteNamespace { namespace } = request;
        self.delete_namespace(&namespace).await?;
        let response = ReplyDeleteNamespace {};
        Ok(Response::new(response))
    }

    async fn process_list_all(
        &self,
        _request: Request<RequestListAll>,
    ) -> Result<Response<ReplyListAll>, Status> {
        let namespaces = self.list_all().await?;
        let response = ReplyListAll { namespaces };
        Ok(Response::new(response))
    }

    async fn process_delete_all(
        &self,
        _request: Request<RequestDeleteAll>,
    ) -> Result<Response<ReplyDeleteAll>, Status> {
        self.delete_all().await?;
        let response = ReplyDeleteAll {};
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() {
    let options = <ServiceStoreServerOptions as clap::Parser>::parse();
    let common_config = CommonStoreConfig::default();
    let (shared_store, endpoint) = match options {
        ServiceStoreServerOptions::Memory { endpoint } => {
            let store = create_memory_store_stream_queries(common_config.max_stream_queries);
            (ServiceStoreServer::Memory(store), endpoint)
        }
        ServiceStoreServerOptions::RocksDb { path, endpoint } => {
            let path_buf = path.into();
            let config = RocksDbStoreConfig {
                path_buf,
                common_config,
            };
            let namespace = "linera";
            let store = RocksDbStore::maybe_create_and_connect(&config, namespace)
                .await
                .expect("store");
            (ServiceStoreServer::RocksDb(store), endpoint)
        }
    };
    let endpoint = endpoint.parse().unwrap();
    Server::builder()
        .add_service(StoreProcessorServer::new(shared_store))
        .serve(endpoint)
        .await
        .expect("a successful running of the server");
}
