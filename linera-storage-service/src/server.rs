// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::blocks_in_conditions)]

use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Weak},
};

use futures::stream::Stream;
use linera_views::{
    batch::Batch,
    memory::MemoryStore,
    store::{CommonStoreConfig, ReadableKeyValueStore, WritableKeyValueStore},
};
#[cfg(with_rocksdb)]
use linera_views::{
    rocks_db::{PathWithGuard, RocksDbSpawnMode, RocksDbStore, RocksDbStoreConfig},
    store::AdminKeyValueStore as _,
};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, instrument};

use crate::{
    common::{KeyPrefix, MAX_PAYLOAD_SIZE},
    key_value_store::{
        store_processor_server::{StoreProcessor, StoreProcessorServer},
        OptValue, ReplyContainsKey, ReplyContainsKeys, ReplyExistsNamespace,
        ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix, ReplyListAll, ReplyListRootKeys,
        ReplyReadMultiValues, ReplyReadValue, RequestContainsKey, RequestContainsKeys,
        RequestCreateNamespace, RequestDeleteNamespace, RequestExistsNamespace,
        RequestFindKeyValuesByPrefix, RequestFindKeysByPrefix, RequestListRootKeys,
        RequestReadMultiValues, RequestReadValue, RequestWriteBatch,
    },
};

pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

pub enum ServiceStoreServerInternal {
    Memory(MemoryStore),
    /// The RocksDB key value store
    #[cfg(with_rocksdb)]
    RocksDb(RocksDbStore),
}

pub struct ServiceStoreServer {
    store: ServiceStoreServerInternal,
    // Its basically free, since tonic holds an Arc
    // internally anyway.
    // Another way is to cast &self to raw pointers and use unsafe rust
    // that also will completely be ok as tasks are either scoped, joining
    // themselves at their functions end or else even then the raw pointers
    // will be backed by the tonic's internal Arc.
    self_ref: Weak<Self>,
}

impl ServiceStoreServer {
    pub fn new_from_store(store: ServiceStoreServerInternal) -> Arc<ServiceStoreServer> {
        Arc::new_cyclic(|weak| ServiceStoreServer {
            store,
            self_ref: weak.clone(),
        })
    }
}

impl ServiceStoreServer {
    pub async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Status> {
        match &self.store {
            ServiceStoreServerInternal::Memory(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|e| Status::unknown(format!("Memory error {:?} at read_value_bytes", e))),
            #[cfg(with_rocksdb)]
            ServiceStoreServerInternal::RocksDb(store) => store
                .read_value_bytes(key)
                .await
                .map_err(|e| Status::unknown(format!("RocksDB error {:?} at read_value_bytes", e))),
        }
    }

    pub async fn contains_key(&self, key: &[u8]) -> Result<bool, Status> {
        match &self.store {
            ServiceStoreServerInternal::Memory(store) => store
                .contains_key(key)
                .await
                .map_err(|e| Status::unknown(format!("Memory error {:?} at contains_key", e))),
            #[cfg(with_rocksdb)]
            ServiceStoreServerInternal::RocksDb(store) => store
                .contains_key(key)
                .await
                .map_err(|e| Status::unknown(format!("RocksDB error {:?} at contains_key", e))),
        }
    }

    pub async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Status> {
        match &self.store {
            ServiceStoreServerInternal::Memory(store) => store
                .contains_keys(keys)
                .await
                .map_err(|e| Status::unknown(format!("Memory error {:?} at contains_keys", e))),
            #[cfg(with_rocksdb)]
            ServiceStoreServerInternal::RocksDb(store) => store
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
            ServiceStoreServerInternal::Memory(store) => {
                store.read_multi_values_bytes(keys).await.map_err(|e| {
                    Status::unknown(format!("Memory error {:?} at read_multi_values_bytes", e))
                })
            }
            #[cfg(with_rocksdb)]
            ServiceStoreServerInternal::RocksDb(store) => {
                store.read_multi_values_bytes(keys).await.map_err(|e| {
                    Status::unknown(format!("RocksDB error {:?} at read_multi_values_bytes", e))
                })
            }
        }
    }

    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Status> {
        match &self.store {
            ServiceStoreServerInternal::Memory(store) => {
                store.find_keys_by_prefix(key_prefix).await.map_err(|e| {
                    Status::unknown(format!("Memory error {:?} at find_keys_by_prefix", e))
                })
            }
            #[cfg(with_rocksdb)]
            ServiceStoreServerInternal::RocksDb(store) => {
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
            ServiceStoreServerInternal::Memory(store) => store
                .find_key_values_by_prefix(key_prefix)
                .await
                .map_err(|e| {
                    Status::unknown(format!("Memory error {:?} at find_key_values_by_prefix", e))
                }),
            #[cfg(with_rocksdb)]
            ServiceStoreServerInternal::RocksDb(store) => store
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
            ServiceStoreServerInternal::Memory(store) => store
                .write_batch(batch)
                .await
                .map_err(|e| Status::unknown(format!("Memory error {:?} at write_batch", e))),
            #[cfg(with_rocksdb)]
            ServiceStoreServerInternal::RocksDb(store) => store
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
}

#[derive(clap::Parser)]
#[command(
    name = "linera-storage-server",
    version = linera_version::VersionInfo::default_clap_str(),
    about = "A server providing storage service",
)]
pub enum ServiceStoreServerOptions {
    #[command(name = "memory")]
    Memory {
        #[arg(long = "endpoint")]
        endpoint: String,
    },

    #[cfg(with_rocksdb)]
    #[command(name = "rocksdb")]
    RocksDb {
        #[arg(long = "path")]
        path: String,
        #[arg(long = "endpoint")]
        endpoint: String,
    },
}

#[tonic::async_trait]
impl StoreProcessor for ServiceStoreServer {
    type ProcessReadValueStream =
        Pin<Box<dyn Stream<Item = Result<ReplyReadValue, Status>> + Send + Sync + 'static>>;
    type ProcessReadMultiValuesStream =
        UnboundedReceiverStream<Result<ReplyReadMultiValues, Status>>;
    type ProcessFindKeysByPrefixStream =
        futures::stream::Iter<std::vec::IntoIter<Result<ReplyFindKeysByPrefix, Status>>>;
    type ProcessFindKeyValuesByPrefixStream =
        UnboundedReceiverStream<Result<ReplyFindKeyValuesByPrefix, Status>>;
    type ProcessListAllStream =
        futures::stream::Iter<std::vec::IntoIter<Result<ReplyListAll, Status>>>;

    #[instrument(target = "store_server", skip_all, err, fields(key_len = ?request.get_ref().key.len()))]
    async fn process_read_value(
        &self,
        request: Request<RequestReadValue>,
    ) -> Result<Response<Self::ProcessReadValueStream>, Status> {
        let request = request.into_inner();
        let RequestReadValue { key } = request;
        let value = self.read_value_bytes(&key).await?;

        let stream = match value {
            None => {
                let mut counter = 0;
                let closure = move || {
                    if 0 == counter {
                        counter = 1;
                        Some(Ok(ReplyReadValue { value: None }))
                    } else {
                        None
                    }
                };
                let closure: Box<
                    dyn FnMut() -> Option<Result<ReplyReadValue, Status>> + Send + Sync,
                > = Box::new(closure);
                std::iter::from_fn(closure)
            }

            Some(inner) => {
                let mut inner = VecDeque::from(inner);
                let closure = move || {
                    let chunk = inner
                        .drain(..MAX_PAYLOAD_SIZE.min(inner.len()))
                        .collect::<Vec<_>>();
                    if chunk.is_empty() {
                        return None;
                    }

                    Some(Ok(ReplyReadValue { value: Some(chunk) }))
                };
                let closure: Box<
                    dyn FnMut() -> Option<Result<ReplyReadValue, Status>> + Send + Sync,
                > = Box::new(closure);
                std::iter::from_fn(closure)
            }
        };

        Ok(Response::new(Box::pin(tokio_stream::iter(stream))))
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

    #[instrument(target = "store_server", skip_all, err)]
    async fn process_contains_keys(
        &self,
        request: Request<tonic::Streaming<RequestContainsKeys>>,
    ) -> Result<Response<ReplyContainsKeys>, Status> {
        let mut streamer = request.into_inner();
        let self_ref = self.self_ref.upgrade().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let producer_task = |self_ref: Arc<ServiceStoreServer>,
                             keys,
                             feedback: UnboundedSender<Status>| async move {
            let tests = self_ref.contains_keys(keys).await;
            match tests {
                Err(e) => {
                    let _ = feedback.send(e);
                    vec![]
                }
                Ok(tests) => tests,
            }
        };

        let consumer_task = async move {
            let mut handles = Vec::new();
            while let Some(message) = match streamer.message().await {
                Ok(Some(msg)) => Some(msg),
                Ok(None) => None,
                Err(err) => {
                    let _ = tx.send(err);
                    return vec![];
                }
            } {
                let RequestContainsKeys { keys } = message;
                let moved_self_ref = Arc::clone(&self_ref);
                let handle = tokio::task::spawn(producer_task(moved_self_ref, keys, tx.clone()));
                handles.push(handle);
            }

            let mut tests = Vec::new();
            for handle in handles {
                let some_tests = handle.await;
                match some_tests {
                    Err(e) => {
                        let _ = tx.send(Status::from_error(Box::new(e)));
                    }
                    Ok(mut some_tests) => tests.append(&mut some_tests),
                }
            }

            tests
        };

        let consumer_handle = tokio::task::spawn(consumer_task);
        if let Some(status) = rx.recv().await {
            return Err(status);
        }

        let tests = match consumer_handle.await {
            Err(e) => return Err(Status::from_error(Box::new(e))),
            Ok(tests) => tests,
        };
        tracing::info!(target = "store_server", key_len = tests.len());
        let response = ReplyContainsKeys { tests };
        Ok(Response::new(response))
    }

    // This function can be greatly compressed in a seperate pull request
    // TODO encode, merge and compress messages for efficiency
    #[instrument(target = "store_server", skip_all, err)]
    async fn process_read_multi_values(
        &self,
        request: Request<tonic::Streaming<RequestReadMultiValues>>,
    ) -> Result<Response<Self::ProcessReadMultiValuesStream>, Status> {
        let mut incoming_stream = request.into_inner();

        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let (task_tx, mut task_rx) = mpsc::unbounded_channel();
        let mut stream = Vec::new();

        let outgoing_task = async move {
            let streamer = |stream| {
                let _ = outgoing_tx.send(Ok(ReplyReadMultiValues { values: stream }));
            };

            while let Some(response) = task_rx.recv().await {
                let values: Vec<Option<Vec<u8>>> = match response {
                    Err(e) => {
                        let _ = outgoing_tx.send(Err(e));
                        return;
                    }
                    Ok(handle) => {
                        let result = handle.await;
                        match result {
                            Err(e) => {
                                let _ = outgoing_tx.send(Err(Status::from_error(Box::new(e))));
                                break;
                            }
                            Ok(Err(e)) => {
                                let _ = outgoing_tx.send(Err(e));
                                return;
                            }
                            Ok(Ok(values)) => values,
                        }
                    }
                };

                for value in values {
                    match value {
                        Some(mut value) => {
                            if MAX_PAYLOAD_SIZE > value.len() {
                                stream.push(OptValue {
                                    value: Some(1usize.to_be_bytes().to_vec()),
                                });
                                stream.push(OptValue { value: Some(value) });
                                streamer(stream);
                                stream = Vec::new();
                                continue;
                            }

                            let num_messages = value.len().div_ceil(MAX_PAYLOAD_SIZE);
                            stream.push(OptValue {
                                value: Some(num_messages.to_be_bytes().to_vec()),
                            });
                            for _ in 0..num_messages {
                                let drain_amount = MAX_PAYLOAD_SIZE.min(value.len());
                                stream.push(OptValue {
                                    value: Some(value.drain(..drain_amount).collect()),
                                });
                                streamer(stream);
                                stream = Vec::new();
                            }
                        }

                        None => streamer(vec![OptValue { value: None }]),
                    }
                }
            }
        };

        let self_ref = self.self_ref.upgrade().unwrap();
        let producer_task = |self_ref: Arc<ServiceStoreServer>, keys| async move {
            self_ref.read_multi_values_bytes(keys).await
        };

        let consumer_task = async move {
            while let Some(message) = incoming_stream.message().await.transpose() {
                match message {
                    Err(status) => {
                        let _ = task_tx.send(Err(status));
                        break;
                    }
                    Ok(RequestReadMultiValues { keys }) => {
                        let moved_self_ref = self_ref.clone();
                        let handle = tokio::task::spawn(producer_task(moved_self_ref, keys));
                        if task_tx.send(Ok(handle)).is_err() {
                            break;
                        }
                    }
                }
            }
        };

        std::mem::drop(tokio::task::spawn(consumer_task));
        std::mem::drop(tokio::task::spawn(outgoing_task));

        Ok(Response::new(UnboundedReceiverStream::new(outgoing_rx)))
    }

    #[instrument(target = "store_server", skip_all, err, fields(key_prefix_len = ?request.get_ref().key_prefix.len()))]
    async fn process_find_keys_by_prefix(
        &self,
        request: Request<RequestFindKeysByPrefix>,
    ) -> Result<Response<Self::ProcessFindKeysByPrefixStream>, Status> {
        let request = request.into_inner();
        let RequestFindKeysByPrefix { key_prefix } = request;
        let keys = self.find_keys_by_prefix(&key_prefix).await?;

        let mut stream = Vec::new();
        let mut grouped_keys = Vec::new();
        let mut chunk_size = 0;
        for key in keys {
            if MAX_PAYLOAD_SIZE > key.len() + chunk_size {
                chunk_size += key.len();
                grouped_keys.push(key);
                continue;
            }

            stream.push(Ok(ReplyFindKeysByPrefix { keys: grouped_keys }));
            grouped_keys = Vec::new();
            chunk_size = key.len();
            grouped_keys.push(key);
        }

        if !grouped_keys.is_empty() {
            stream.push(Ok(ReplyFindKeysByPrefix { keys: grouped_keys }));
        }

        Ok(Response::new(futures::stream::iter(stream)))
    }

    #[instrument(target = "store_server", skip_all, err, fields(key_prefix_len = ?request.get_ref().key_prefix.len()))]
    async fn process_find_key_values_by_prefix(
        &self,
        request: Request<RequestFindKeyValuesByPrefix>,
    ) -> Result<Response<Self::ProcessFindKeyValuesByPrefixStream>, Status> {
        let request = request.into_inner();
        let RequestFindKeyValuesByPrefix { key_prefix } = request;
        let key_values = self.find_key_values_by_prefix(&key_prefix).await?;

        // TODO Discuss the usage of bounded channels for backpressure.
        let (tx, rx) = mpsc::unbounded_channel();
        let mut stream = Vec::new();

        let spawner = move || {
            let streamer = |stream| {
                let _ = tx.send(Ok(ReplyFindKeyValuesByPrefix { key_values: stream }));
            };

            let extend_stream = |mut stream: Vec<u8>, mut value: Vec<u8>| {
                if MAX_PAYLOAD_SIZE - stream.len() >= value.len() {
                    stream.append(&mut value);
                    return stream;
                }

                stream.append(
                    &mut value
                        .drain(..MAX_PAYLOAD_SIZE - stream.len())
                        .collect::<Vec<_>>(),
                );
                streamer(stream);
                stream = Vec::new();

                loop {
                    if MAX_PAYLOAD_SIZE >= value.len() {
                        stream.append(&mut value);
                        break;
                    }

                    stream.append(&mut value.drain(..MAX_PAYLOAD_SIZE).collect::<Vec<_>>());
                    streamer(stream);
                    stream = Vec::new();
                }

                stream
            };

            for (key, value) in key_values {
                stream = extend_stream(stream, key.len().to_be_bytes().to_vec());
                stream = extend_stream(stream, value.len().to_be_bytes().to_vec());
                stream = extend_stream(stream, key);
                stream = extend_stream(stream, value);
            }

            streamer(stream);
        };

        let _ = std::thread::spawn(spawner);

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    #[instrument(target = "store_server", skip_all, err)]
    async fn process_write_batch(
        &self,
        request: Request<tonic::Streaming<RequestWriteBatch>>,
    ) -> Result<Response<()>, Status> {
        let mut incoming_stream = request.into_inner();
        let mut batch = Batch::default();

        let mut state = Vec::new();
        let mut key = Vec::new();
        let mut value = Vec::new();
        let mut len_bytes = Vec::with_capacity(8);
        while let Some(RequestWriteBatch { stream: message }) = incoming_stream.message().await? {
            let mut message = VecDeque::from(message);
            while !message.is_empty() || state.get(2).is_some_and(|len| 0 == *len) {
                match state.len() {
                    0 => {
                        let op_kind = message.pop_front().unwrap() as usize;
                        state.push(op_kind);
                        state.push(0);
                    }

                    2 => {
                        len_bytes.append(
                            &mut message
                                .drain(..(8 - len_bytes.len()).min(message.len()))
                                .collect::<Vec<u8>>(),
                        );
                        if 8 == len_bytes.len() {
                            state.push(usize::from_be_bytes(len_bytes.clone().try_into().unwrap()));
                            len_bytes.clear();
                        }
                    }

                    3 if 3 == state[0] && 1 == state[1] => {
                        value.append(
                            &mut message
                                .drain(..(state[2] - value.len()).min(message.len()))
                                .collect::<Vec<_>>(),
                        );
                        if state[2] == value.len() {
                            batch.put_key_value_bytes(key, value);
                            key = Vec::new();
                            value = Vec::new();
                            let _ = state.pop();
                            let _ = state.pop();
                            let _ = state.pop();
                        }
                    }

                    3 => {
                        key.append(
                            &mut message
                                .drain(..(state[2] - key.len()).min(message.len()))
                                .collect::<Vec<_>>(),
                        );
                        match (state[0], state[2] == key.len()) {
                            (1, true) => {
                                batch.delete_key(key);
                                key = Vec::new();
                                let _ = state.pop();
                                let _ = state.pop();
                                let _ = state.pop();
                            }

                            (2, true) => {
                                batch.delete_key_prefix(key);
                                key = Vec::new();
                                let _ = state.pop();
                                let _ = state.pop();
                                let _ = state.pop();
                            }

                            (3, true) => {
                                state[1] = 1;
                                let _ = state.pop();
                            }

                            (_, false) => {}
                            (_, true) => unreachable!(),
                        }
                    }

                    _ => unreachable!(),
                }
            }
        }

        tracing::info!(
            target = "store_server",
            n_statements = batch.operations.len()
        );

        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }

        Ok(Response::new(()))
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
    ) -> Result<Response<Self::ProcessListAllStream>, Status> {
        let namespaces = self.list_all().await?;

        let mut stream = Vec::new();
        let mut grouped_namespaces = Vec::new();
        let mut chunk_size = 0;
        for namespace in namespaces {
            if MAX_PAYLOAD_SIZE > namespace.len() + chunk_size {
                chunk_size += namespace.len();
                grouped_namespaces.push(namespace);
                continue;
            }

            stream.push(Ok(ReplyListAll {
                namespaces: grouped_namespaces,
            }));
            grouped_namespaces = Vec::new();
            chunk_size = namespace.len();
            grouped_namespaces.push(namespace);
        }

        if !grouped_namespaces.is_empty() {
            stream.push(Ok(ReplyListAll {
                namespaces: grouped_namespaces,
            }));
        }

        Ok(Response::new(futures::stream::iter(stream)))
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

pub async fn server(endpoint: &str) {
    let options = ServiceStoreServerOptions::Memory {
        endpoint: endpoint.to_owned(),
    };
    let common_config = CommonStoreConfig::default();
    let namespace = "linera_storage_service";
    let root_key = &[];
    let (store, endpoint) = match options {
        ServiceStoreServerOptions::Memory { endpoint } => {
            let store =
                MemoryStore::new(common_config.max_stream_queries, namespace, root_key).unwrap();
            let store = ServiceStoreServerInternal::Memory(store);
            (store, endpoint)
        }
        #[cfg(with_rocksdb)]
        ServiceStoreServerOptions::RocksDb { path, endpoint } => {
            let path_buf = path.into();
            let path_with_guard = PathWithGuard::new(path_buf);
            // The server is run in multi-threaded mode so we can use the block_in_place.
            let spawn_mode = RocksDbSpawnMode::get_spawn_mode_from_runtime();
            let config = RocksDbStoreConfig::new(spawn_mode, path_with_guard, common_config);
            let store = RocksDbStore::maybe_create_and_connect(&config, namespace, root_key)
                .await
                .expect("store");
            let store = ServiceStoreServerInternal::RocksDb(store);
            (store, endpoint)
        }
    };

    let heaped_store = Arc::new_cyclic(|weak| ServiceStoreServer {
        store,
        self_ref: weak.clone(),
    });

    let endpoint = endpoint.parse().unwrap();
    info!("Starting linera_storage_service on endpoint={}", endpoint);
    Server::builder()
        .add_service(StoreProcessorServer::from_arc(heaped_store))
        .serve(endpoint)
        .await
        .expect("a successful running of the server");
}
