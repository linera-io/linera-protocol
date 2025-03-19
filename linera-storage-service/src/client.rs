// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_lock::{Semaphore, SemaphoreGuard};
use futures::future::join_all;
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
use serde::de::DeserializeOwned;
use tonic::transport::{Channel, Endpoint};

#[cfg(with_testing)]
use crate::common::storage_service_test_endpoint;
use crate::{
    common::{KeyPrefix, ServiceStoreError, ServiceStoreInternalConfig, MAX_PAYLOAD_SIZE},
    key_value_store::{
        statement::Operation, store_processor_client::StoreProcessorClient, KeyValue,
        KeyValueAppend, ReplyContainsKey, ReplyContainsKeys, ReplyExistsNamespace,
        ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix, ReplyListAll, ReplyListRootKeys,
        ReplyReadMultiValues, ReplyReadValue, ReplySpecificChunk, RequestContainsKey,
        RequestContainsKeys, RequestCreateNamespace, RequestDeleteNamespace,
        RequestExistsNamespace, RequestFindKeyValuesByPrefix, RequestFindKeysByPrefix,
        RequestListRootKeys, RequestReadMultiValues, RequestReadValue, RequestSpecificChunk,
        RequestWriteBatchExtended, Statement,
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
        let response = response.into_inner();
        let ReplyReadValue {
            value,
            message_index,
            num_chunks,
        } = response;
        if num_chunks == 0 {
            Ok(value)
        } else {
            self.read_entries(message_index, num_chunks).await
        }
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
        let query = RequestContainsKeys { keys: full_keys };
        let request = tonic::Request::new(query);
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
        let query = RequestReadMultiValues { keys: full_keys };
        let request = tonic::Request::new(query);
        let channel = self.channel.clone();
        let mut client = StoreProcessorClient::new(channel);
        let _guard = self.acquire().await;
        let response = client.process_read_multi_values(request).await?;
        let response = response.into_inner();
        let ReplyReadMultiValues {
            values,
            message_index,
            num_chunks,
        } = response;
        if num_chunks == 0 {
            let values = values.into_iter().map(|x| x.value).collect::<Vec<_>>();
            Ok(values)
        } else {
            self.read_entries(message_index, num_chunks).await
        }
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
        let response = response.into_inner();
        let ReplyFindKeysByPrefix {
            keys,
            message_index,
            num_chunks,
        } = response;
        if num_chunks == 0 {
            Ok(keys)
        } else {
            self.read_entries(message_index, num_chunks).await
        }
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
        let response = response.into_inner();
        let ReplyFindKeyValuesByPrefix {
            key_values,
            message_index,
            num_chunks,
        } = response;
        if num_chunks == 0 {
            let key_values = key_values
                .into_iter()
                .map(|x| (x.key, x.value))
                .collect::<Vec<_>>();
            Ok(key_values)
        } else {
            self.read_entries(message_index, num_chunks).await
        }
    }
}

impl WritableKeyValueStore for ServiceStoreClientInternal {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), ServiceStoreError> {
        if batch.operations.is_empty() {
            return Ok(());
        }
        let mut statements = Vec::new();
        let mut chunk_size = 0;

        if !self.root_key_written.fetch_or(true, Ordering::SeqCst) {
            let mut full_key = self.start_key.clone();
            full_key[0] = KeyPrefix::RootKey as u8;
            let operation = Operation::Put(KeyValue {
                key: full_key,
                value: vec![],
            });
            let statement = Statement {
                operation: Some(operation),
            };
            statements.push(statement);
            chunk_size += self.start_key.len();
        }

        let root_key_len = self.start_key.len() - self.prefix_len;
        for operation in batch.operations {
            let (key_len, value_len) = match &operation {
                WriteOperation::Delete { key } => (key.len(), 0),
                WriteOperation::Put { key, value } => (key.len(), value.len()),
                WriteOperation::DeletePrefix { key_prefix } => (key_prefix.len(), 0),
            };
            let operation_size = key_len + value_len + root_key_len;
            ensure!(key_len <= MAX_KEY_SIZE, ServiceStoreError::KeyTooLong);
            if operation_size + chunk_size < MAX_PAYLOAD_SIZE {
                let statement = self.get_statement(operation);
                statements.push(statement);
                chunk_size += operation_size;
            } else {
                self.submit_statements(mem::take(&mut statements)).await?;
                chunk_size = 0;
                if operation_size > MAX_PAYLOAD_SIZE {
                    // One single operation is especially big. So split it in chunks.
                    let WriteOperation::Put { key, value } = operation else {
                        // Only the put can go over the limit
                        unreachable!();
                    };
                    let mut full_key = self.start_key.clone();
                    full_key.extend(key);
                    let value_chunks = value
                        .chunks(MAX_PAYLOAD_SIZE)
                        .map(|x| x.to_vec())
                        .collect::<Vec<_>>();
                    let num_chunks = value_chunks.len();
                    for (i_chunk, value) in value_chunks.into_iter().enumerate() {
                        let last = i_chunk + 1 == num_chunks;
                        let operation = Operation::Append(KeyValueAppend {
                            key: full_key.clone(),
                            value,
                            last,
                        });
                        statements = vec![Statement {
                            operation: Some(operation),
                        }];
                        self.submit_statements(mem::take(&mut statements)).await?;
                    }
                } else {
                    // The operation is small enough, it is just that we have many so we need to split.
                    let statement = self.get_statement(operation);
                    statements.push(statement);
                    chunk_size = operation_size;
                }
            }
        }
        self.submit_statements(mem::take(&mut statements)).await
    }

    async fn clear_journal(&self) -> Result<(), ServiceStoreError> {
        Ok(())
    }
}

impl ServiceStoreClientInternal {
    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
        }
    }

    async fn submit_statements(&self, statements: Vec<Statement>) -> Result<(), ServiceStoreError> {
        if !statements.is_empty() {
            let query = RequestWriteBatchExtended { statements };
            let request = tonic::Request::new(query);
            let channel = self.channel.clone();
            let mut client = StoreProcessorClient::new(channel);
            let _guard = self.acquire().await;
            let _response = client.process_write_batch_extended(request).await?;
        }
        Ok(())
    }

    fn get_statement(&self, operation: WriteOperation) -> Statement {
        let operation = match operation {
            WriteOperation::Delete { key } => {
                let mut full_key = self.start_key.clone();
                full_key.extend(key);
                Operation::Delete(full_key)
            }
            WriteOperation::Put { key, value } => {
                let mut full_key = self.start_key.clone();
                full_key.extend(key);
                Operation::Put(KeyValue {
                    key: full_key,
                    value,
                })
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                let mut full_key_prefix = self.start_key.clone();
                full_key_prefix.extend(key_prefix);
                Operation::DeletePrefix(full_key_prefix)
            }
        };
        Statement {
            operation: Some(operation),
        }
    }

    async fn read_single_entry(
        &self,
        message_index: i64,
        index: i32,
    ) -> Result<Vec<u8>, ServiceStoreError> {
        let channel = self.channel.clone();
        let query = RequestSpecificChunk {
            message_index,
            index,
        };
        let request = tonic::Request::new(query);
        let mut client = StoreProcessorClient::new(channel);
        let response = client.process_specific_chunk(request).await?;
        let response = response.into_inner();
        let ReplySpecificChunk { chunk } = response;
        Ok(chunk)
    }

    async fn read_entries<S: DeserializeOwned>(
        &self,
        message_index: i64,
        num_chunks: i32,
    ) -> Result<S, ServiceStoreError> {
        let mut handles = Vec::new();
        for index in 0..num_chunks {
            let handle = self.read_single_entry(message_index, index);
            handles.push(handle);
        }
        let mut value = Vec::new();
        for chunk in join_all(handles).await {
            let chunk = chunk?;
            value.extend(chunk);
        }
        Ok(bcs::from_bytes(&value)?)
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
        let response = response.into_inner();
        let ReplyListAll { namespaces } = response;
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
            return Err(ServiceStoreError::StoreAlreadyExist);
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
