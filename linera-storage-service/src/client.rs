// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_lock::{Semaphore, SemaphoreGuard};
use linera_base::ensure;
#[cfg(with_metrics)]
use linera_views::metering::MeteredStore;
#[cfg(with_testing)]
use linera_views::store::TestKeyValueStore;
use linera_views::{
    batch::{Batch, WriteOperation},
    lru_caching::LruCachingStore,
    store::{
        AdminKeyValueStore, CommonStoreInternalConfig, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tonic::transport::{Channel, Endpoint};

#[cfg(with_testing)]
use crate::common::storage_service_test_endpoint;
use crate::{
    common::{KeyPrefix, ServiceStoreError, ServiceStoreInternalConfig, MAX_PAYLOAD_SIZE},
    key_value_store::{
        store_processor_client::StoreProcessorClient, OptValue, ReplyContainsKey,
        ReplyContainsKeys, ReplyExistsNamespace, ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix,
        ReplyListAll, ReplyListRootKeys, ReplyReadMultiValues, ReplyReadValue, RequestContainsKey,
        RequestContainsKeys, RequestCreateNamespace, RequestDeleteNamespace,
        RequestExistsNamespace, RequestFindKeyValuesByPrefix, RequestFindKeysByPrefix,
        RequestListRootKeys, RequestReadMultiValues, RequestReadValue, RequestWriteBatch,
    },
};

// The maximum key size is set to 1M rather arbitrarily.
const MAX_KEY_SIZE: usize = 1000000;

// The shared store client.
// * Interior mutability is required for the client because
// accessing requires mutability while the KeyValueStore
// does not allow it.
// * The semaphore and max_stream_queries work as other
// stores.
//
// The encoding of namespaces is done by taking their
// serialization. This works because the set of serialization
// of strings is prefix free.
// The data is stored in the following way.
// * A `key` in a `namespace` is stored as
//   [`KeyPrefix::Key`] + namespace + key
// * An additional key with empty value is stored at
//   [`KeyPrefix::Namespace`] + namespace
//   is stored to indicate the existence of a namespace.
// * A key with empty value is stored at
//   [`KeyPrefix::RootKey`] + namespace + root_key
//   to indicate the existence of a root key.
#[derive(Clone)]
pub struct ServiceStoreClientInternal {
    channel: Channel,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    prefix_len: usize,
    start_key: Vec<u8>,
    root_key_written: Arc<AtomicBool>,
}

impl WithError for ServiceStoreClientInternal {
    type Error = ServiceStoreError;
}

impl ReadableKeyValueStore for ServiceStoreClientInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ServiceStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ServiceStoreError::KeyTooLong);
        let mut full_key = self.start_key.clone();
        full_key.extend(key);
        let query = RequestReadValue { key: full_key };
        let request = tonic::Request::new(query);
        let channel = self.channel.clone();
        let mut client = StoreProcessorClient::new(channel);
        let _guard = self.acquire().await;
        let response = client.process_read_value(request).await?;
        let mut incoming_stream = response.into_inner();

        let mut reformed_value = Vec::new();
        while let Some(ReplyReadValue { value }) = incoming_stream.message().await? {
            let Some(mut value) = value else {
                return Ok(None);
            };

            reformed_value.append(&mut value);
        }

        Ok(Some(reformed_value))
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ServiceStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ServiceStoreError::KeyTooLong);
        let mut full_key = self.start_key.clone();
        full_key.extend(key);
        let query = RequestContainsKey { key: full_key };
        let request = tonic::Request::new(query);
        let channel = self.channel.clone();
        let mut client = StoreProcessorClient::new(channel);
        let _guard = self.acquire().await;
        let response = client.process_contains_key(request).await?;
        let response = response.into_inner();
        let ReplyContainsKey { test } = response;
        Ok(test)
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, ServiceStoreError> {
        let mut full_keys = Vec::new();
        for key in keys {
            ensure!(key.len() <= MAX_KEY_SIZE, ServiceStoreError::KeyTooLong);
            let mut full_key = self.start_key.clone();
            full_key.extend(&key);
            full_keys.push(full_key);
        }

        let mut queries = Vec::new();
        let mut grouped_keys = Vec::new();
        let mut chunk_size = 0;
        for full_key in full_keys {
            if MAX_PAYLOAD_SIZE >= full_key.len() + chunk_size {
                chunk_size += full_key.len();
                grouped_keys.push(full_key);
                continue;
            }

            queries.push(RequestContainsKeys { keys: grouped_keys });
            grouped_keys = Vec::new();
            chunk_size = full_key.len();
            grouped_keys.push(full_key);
        }

        if !grouped_keys.is_empty() {
            queries.push(RequestContainsKeys { keys: grouped_keys });
        }

        let request = tonic::Request::new(tokio_stream::iter(queries));
        let channel = self.channel.clone();
        let mut client = StoreProcessorClient::new(channel);
        let _guard = self.acquire().await;
        let response = client.process_contains_keys(request).await?;
        let response = response.into_inner();
        let ReplyContainsKeys { tests } = response;
        Ok(tests)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ServiceStoreError> {
        let mut full_keys = Vec::new();
        for key in keys {
            ensure!(key.len() <= MAX_KEY_SIZE, ServiceStoreError::KeyTooLong);
            let mut full_key = self.start_key.clone();
            full_key.extend(&key);
            full_keys.push(full_key);
        }

        let (tx, rx) = mpsc::channel(4);

        let feeder = async move {
            let mut outgoing_message = Vec::new();
            let mut chunk_size = 0;
            for key in full_keys {
                if MAX_PAYLOAD_SIZE >= key.len() + chunk_size {
                    chunk_size += key.len();
                    outgoing_message.push(key);
                    continue;
                }

                let _ = tx
                    .send(RequestReadMultiValues {
                        keys: outgoing_message,
                    })
                    .await;
                outgoing_message = Vec::new();
                chunk_size = key.len();
                outgoing_message.push(key);
            }

            if !outgoing_message.is_empty() {
                let _ = tx
                    .send(RequestReadMultiValues {
                        keys: outgoing_message,
                    })
                    .await;
            }
        };

        std::mem::drop(tokio::task::spawn(feeder));

        let request = tonic::Request::new(ReceiverStream::new(rx));
        let channel = self.channel.clone();
        let mut client = StoreProcessorClient::new(channel);
        let _guard = self.acquire().await;
        let response = client.process_read_multi_values(request).await?;

        let mut incoming_stream = response.into_inner();
        let mut values = Vec::new();
        let mut num_messages = 0;
        let mut value = Vec::new();
        while let Some(ReplyReadMultiValues {
            values: incoming_values,
        }) = incoming_stream.message().await?
        {
            for OptValue {
                value: incoming_value,
            } in incoming_values
            {
                match incoming_value {
                    None => values.push(None),
                    Some(mut bytes) => {
                        if 0 == num_messages {
                            num_messages = usize::from_be_bytes(bytes.try_into().unwrap());
                            continue;
                        }

                        value.append(&mut bytes);
                        num_messages -= 1;

                        if 0 == num_messages {
                            values.push(Some(value));
                            value = Vec::new();
                        }
                    }
                }
            }
        }

        Ok(values)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, ServiceStoreError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            ServiceStoreError::KeyTooLong
        );
        let mut full_key_prefix = self.start_key.clone();
        full_key_prefix.extend(key_prefix);
        let query = RequestFindKeysByPrefix {
            key_prefix: full_key_prefix,
        };
        let request = tonic::Request::new(query);
        let channel = self.channel.clone();
        let mut client = StoreProcessorClient::new(channel);
        let _guard = self.acquire().await;
        let response = client.process_find_keys_by_prefix(request).await?;
        let mut incoming_stream = response.into_inner();

        let mut keys = Vec::new();
        while let Some(ReplyFindKeysByPrefix {
            keys: mut some_keys,
        }) = incoming_stream.message().await?
        {
            keys.append(&mut some_keys);
        }

        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ServiceStoreError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            ServiceStoreError::KeyTooLong
        );
        let mut full_key_prefix = self.start_key.clone();
        full_key_prefix.extend(key_prefix);
        let query = RequestFindKeyValuesByPrefix {
            key_prefix: full_key_prefix,
        };
        let request = tonic::Request::new(query);
        let channel = self.channel.clone();
        let mut client = StoreProcessorClient::new(channel);
        let _guard = self.acquire().await;
        let response = client.process_find_key_values_by_prefix(request).await?;

        let is_zero = |buf: &[u8]| buf.iter().max().is_some_and(|x| 0 == *x);

        let mut key_values = Vec::new();
        let mut key = Vec::new();
        let mut value = Vec::new();
        let mut key_len = Vec::with_capacity(8);
        let mut value_len = Vec::with_capacity(8);
        let mut incoming_stream = response.into_inner();
        while let Some(ReplyFindKeyValuesByPrefix {
            key_values: message,
        }) = incoming_stream.message().await?
        {
            let mut message = VecDeque::from(message);
            while !message.is_empty() || 8 == value_len.len() && is_zero(&value_len) {
                if 8 != key_len.len() {
                    key_len.extend(message.drain(..(8 - key_len.len()).min(message.len())));
                    continue;
                }

                if 8 != value_len.len() {
                    value_len.extend(message.drain(..(8 - value_len.len()).min(message.len())));
                    continue;
                }

                let expected_key_len = usize::from_be_bytes(key_len.clone().try_into().unwrap());
                if expected_key_len != key.len() {
                    key.append(
                        &mut message
                            .drain(..(expected_key_len - key.len()).min(message.len()))
                            .collect::<Vec<_>>(),
                    );
                }

                if expected_key_len != key.len() {
                    continue;
                }

                let expected_value_len =
                    usize::from_be_bytes(value_len.clone().try_into().unwrap());
                if expected_value_len != value.len() {
                    value.append(
                        &mut message
                            .drain(..(expected_value_len - value.len()).min(message.len()))
                            .collect::<Vec<_>>(),
                    );
                }

                if expected_value_len != value.len() {
                    continue;
                }

                key_values.push((key, value));

                key = Vec::new();
                value = Vec::new();
                key_len.clear();
                value_len.clear();
            }
        }

        Ok(key_values)
    }
}

impl WritableKeyValueStore for ServiceStoreClientInternal {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), ServiceStoreError> {
        if batch.operations.is_empty() {
            return Ok(());
        }

        let mut operations = Vec::new();

        if !self.root_key_written.fetch_or(true, Ordering::SeqCst) {
            let mut full_key = self.start_key.clone();
            full_key[0] = KeyPrefix::RootKey as u8;
            let operation = Operation::Put {
                key: full_key,
                value: vec![],
            };

            operations.push(operation);
        }

        for operation in batch.operations {
            let (key_len, operation) = self.get_operation(operation);
            ensure!(key_len <= MAX_KEY_SIZE, ServiceStoreError::KeyTooLong);
            operations.push(operation);
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let mut stream = Vec::new();

        let spawner = move || {
            let streamer = |stream| {
                let _ = tx.send(RequestWriteBatch { stream });
            };

            let extend_stream = |mut stream: Vec<u8>, mut value: Vec<u8>| {
                if MAX_PAYLOAD_SIZE - stream.len() >= value.len() {
                    stream.append(&mut value);
                    return stream;
                }

                let mut value = VecDeque::from(value);
                stream.append(
                    &mut value
                        .drain(..MAX_PAYLOAD_SIZE - stream.len())
                        .collect::<Vec<_>>(),
                );
                streamer(stream);
                stream = Vec::new();

                loop {
                    if MAX_PAYLOAD_SIZE >= value.len() {
                        stream.append(&mut Vec::from(value));
                        break;
                    }

                    stream.append(&mut value.drain(..MAX_PAYLOAD_SIZE).collect::<Vec<_>>());
                    streamer(stream);
                    stream = Vec::new();
                }

                stream
            };

            for operation in operations {
                match operation {
                    Operation::Delete { key } => {
                        stream = extend_stream(stream, vec![1u8]);
                        stream = extend_stream(stream, key.len().to_be_bytes().to_vec());
                        stream = extend_stream(stream, key);
                    }

                    Operation::DeletePrefix { key_prefix } => {
                        stream = extend_stream(stream, vec![2u8]);
                        stream = extend_stream(stream, key_prefix.len().to_be_bytes().to_vec());
                        stream = extend_stream(stream, key_prefix);
                    }

                    Operation::Put { key, value } => {
                        stream = extend_stream(stream, vec![3u8]);
                        stream = extend_stream(stream, key.len().to_be_bytes().to_vec());
                        stream = extend_stream(stream, key);
                        stream = extend_stream(stream, value.len().to_be_bytes().to_vec());
                        stream = extend_stream(stream, value);
                    }
                }
            }

            if !stream.is_empty() {
                streamer(stream);
            }
        };

        let _ = std::thread::spawn(spawner);

        let request = tonic::Request::new(UnboundedReceiverStream::new(rx));
        let channel = self.channel.clone();
        let mut client = StoreProcessorClient::new(channel);
        let _guard = self.acquire().await;
        let _response = client.process_write_batch(request).await?;

        Ok(())
    }

    async fn clear_journal(&self) -> Result<(), ServiceStoreError> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Delete { key: Vec<u8> },
    DeletePrefix { key_prefix: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

impl ServiceStoreClientInternal {
    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
        }
    }

    fn get_operation(&self, operation: WriteOperation) -> (usize, Operation) {
        match operation {
            WriteOperation::Delete { key } => {
                let mut full_key = self.start_key.clone();
                full_key.extend(key);
                (full_key.len(), Operation::Delete { key: full_key })
            }
            WriteOperation::Put { key, value } => {
                let mut full_key = self.start_key.clone();
                full_key.extend(key);
                (
                    full_key.len(),
                    Operation::Put {
                        key: full_key,
                        value,
                    },
                )
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                let mut full_key_prefix = self.start_key.clone();
                full_key_prefix.extend(key_prefix);
                (
                    full_key_prefix.len(),
                    Operation::DeletePrefix {
                        key_prefix: full_key_prefix,
                    },
                )
            }
        }
    }
}

impl AdminKeyValueStore for ServiceStoreClientInternal {
    type Config = ServiceStoreInternalConfig;

    fn get_name() -> String {
        "service store".to_string()
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, ServiceStoreError> {
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let namespace = bcs::to_bytes(namespace)?;
        let max_stream_queries = config.common_config.max_stream_queries;
        let mut start_key = vec![KeyPrefix::Key as u8];
        start_key.extend(&namespace);
        let prefix_len = namespace.len() + 1;
        let endpoint = config.http_address();
        let endpoint = Endpoint::from_shared(endpoint)?;
        let channel = endpoint.connect_lazy();
        Ok(Self {
            channel,
            semaphore,
            max_stream_queries,
            prefix_len,
            start_key,
            root_key_written: Arc::new(AtomicBool::new(false)),
        })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, ServiceStoreError> {
        let channel = self.channel.clone();
        let prefix_len = self.prefix_len;
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        let mut start_key = self.start_key[..prefix_len].to_vec();
        start_key.extend(root_key);
        Ok(Self {
            channel,
            semaphore,
            max_stream_queries,
            prefix_len,
            start_key,
            root_key_written: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ServiceStoreError> {
        let endpoint = config.http_address();
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let response = client.process_list_all(()).await?;
        let mut incoming_stream = response.into_inner();

        let mut namespaces = Vec::new();
        while let Some(ReplyListAll {
            namespaces: mut some_namespaces,
        }) = incoming_stream.message().await?
        {
            namespaces.append(&mut some_namespaces);
        }

        let namespaces = namespaces
            .into_iter()
            .map(|x| bcs::from_bytes(&x))
            .collect::<Result<_, _>>()?;
        Ok(namespaces)
    }

    async fn list_root_keys(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Vec<Vec<u8>>, ServiceStoreError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestListRootKeys { namespace };
        let request = tonic::Request::new(query);
        let endpoint = config.http_address();
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let response = client.process_list_root_keys(request).await?;
        let response = response.into_inner();
        let ReplyListRootKeys { root_keys } = response;
        Ok(root_keys)
    }

    async fn delete_all(config: &Self::Config) -> Result<(), ServiceStoreError> {
        let endpoint = config.http_address();
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_all(()).await?;
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, ServiceStoreError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestExistsNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = config.http_address();
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let response = client.process_exists_namespace(request).await?;
        let response = response.into_inner();
        let ReplyExistsNamespace { exists } = response;
        Ok(exists)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), ServiceStoreError> {
        if ServiceStoreClientInternal::exists(config, namespace).await? {
            return Err(ServiceStoreError::StoreAlreadyExists);
        }
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestCreateNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = config.http_address();
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_create_namespace(request).await?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), ServiceStoreError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestDeleteNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = config.http_address();
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_namespace(request).await?;
        Ok(())
    }
}

#[cfg(with_testing)]
impl TestKeyValueStore for ServiceStoreClientInternal {
    async fn new_test_config() -> Result<ServiceStoreInternalConfig, ServiceStoreError> {
        let endpoint = storage_service_test_endpoint()?;
        service_config_from_endpoint(&endpoint)
    }
}

/// Creates a `ServiceStoreConfig` from an endpoint.
pub fn service_config_from_endpoint(
    endpoint: &str,
) -> Result<ServiceStoreInternalConfig, ServiceStoreError> {
    let common_config = CommonStoreInternalConfig {
        max_concurrent_queries: None,
        max_stream_queries: 100,
    };
    let endpoint = endpoint.to_string();
    Ok(ServiceStoreInternalConfig {
        endpoint,
        common_config,
    })
}

/// Checks that endpoint is truly absent.
pub async fn storage_service_check_absence(endpoint: &str) -> Result<bool, ServiceStoreError> {
    let endpoint = Endpoint::from_shared(endpoint.to_string())?;
    let result = StoreProcessorClient::connect(endpoint).await;
    Ok(result.is_err())
}

/// Checks whether an endpoint is valid or not.
pub async fn storage_service_check_validity(endpoint: &str) -> Result<(), ServiceStoreError> {
    let config = service_config_from_endpoint(endpoint).unwrap();
    let namespace = "namespace";
    let store = ServiceStoreClientInternal::connect(&config, namespace).await?;
    let _value = store.read_value_bytes(&[42]).await?;
    Ok(())
}

/// The service store client with metrics
#[cfg(with_metrics)]
pub type ServiceStoreClient =
    MeteredStore<LruCachingStore<MeteredStore<ServiceStoreClientInternal>>>;

/// The service store client without metrics
#[cfg(not(with_metrics))]
pub type ServiceStoreClient = LruCachingStore<ServiceStoreClientInternal>;
