// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{mem, sync::Arc};

use async_lock::{RwLock, RwLockWriteGuard, Semaphore, SemaphoreGuard};
use linera_base::ensure;
#[cfg(with_metrics)]
use linera_views::metering::MeteredStore;
#[cfg(with_testing)]
use linera_views::random::generate_test_namespace;
use linera_views::{
    batch::{Batch, WriteOperation},
    lru_caching::LruCachingStore,
    store::{
        AdminKeyValueStore, CommonStoreConfig, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
    },
};
use serde::de::DeserializeOwned;
use tonic::transport::{Channel, Endpoint};

#[cfg(with_metrics)]
use crate::common::{LRU_STORAGE_SERVICE_METRICS, STORAGE_SERVICE_METRICS};
use crate::{
    common::{
        storage_service_test_endpoint, KeyTag, ServiceStoreConfig, ServiceStoreError,
        MAX_PAYLOAD_SIZE,
    },
    key_value_store::{
        statement::Operation, store_processor_client::StoreProcessorClient, KeyValue,
        KeyValueAppend, ReplyContainsKey, ReplyContainsKeys, ReplyExistsNamespace,
        ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix, ReplyListAll, ReplyReadMultiValues,
        ReplyReadValue, ReplySpecificChunk, RequestContainsKey, RequestContainsKeys,
        RequestCreateNamespace, RequestDeleteAll, RequestDeleteNamespace, RequestExistsNamespace,
        RequestFindKeyValuesByPrefix, RequestFindKeysByPrefix, RequestListAll,
        RequestReadMultiValues, RequestReadValue, RequestSpecificChunk, RequestWriteBatchExtended,
        Statement,
    },
};

// The maximum key size is set to 1M rather arbitrarily.
const MAX_KEY_SIZE: usize = 1000000;

// The shared store client.
// * Interior mutability is required for client because
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
//   [KeyTag::Key] + [namespace] + [key]
// * An additional key with empty value is stored at
//   [KeyTag::Namespace] + [namespace]
// is stored to indicate the existence of a namespace.
#[derive(Clone)]
pub struct ServiceStoreClientInternal {
    client: Arc<RwLock<StoreProcessorClient<Channel>>>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    cache_size: usize,
    namespace: Vec<u8>,
    root_key: Vec<u8>,
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
        let mut full_key = self.namespace.clone();
        full_key.extend(&self.root_key);
        full_key.extend(key);
        let query = RequestReadValue { key: full_key };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
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
            Self::read_entries(client, message_index, num_chunks).await
        }
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ServiceStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ServiceStoreError::KeyTooLong);
        let mut full_key = self.namespace.clone();
        full_key.extend(&self.root_key);
        full_key.extend(key);
        let query = RequestContainsKey { key: full_key };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
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
            let mut full_key = self.namespace.clone();
            full_key.extend(&self.root_key);
            full_key.extend(&key);
            full_keys.push(full_key);
        }
        let query = RequestContainsKeys { keys: full_keys };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
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
            let mut full_key = self.namespace.clone();
            full_key.extend(&self.root_key);
            full_key.extend(&key);
            full_keys.push(full_key);
        }
        let query = RequestReadMultiValues { keys: full_keys };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
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
            Self::read_entries(client, message_index, num_chunks).await
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
        let mut full_key_prefix = self.namespace.clone();
        full_key_prefix.extend(&self.root_key);
        full_key_prefix.extend(key_prefix);
        let query = RequestFindKeysByPrefix {
            key_prefix: full_key_prefix,
        };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
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
            Self::read_entries(client, message_index, num_chunks).await
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
        let mut full_key_prefix = self.namespace.clone();
        full_key_prefix.extend(&self.root_key);
        full_key_prefix.extend(key_prefix);
        let query = RequestFindKeyValuesByPrefix {
            key_prefix: full_key_prefix,
        };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
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
            Self::read_entries(client, message_index, num_chunks).await
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
        for operation in batch.operations {
            let (key_len, value_len) = match &operation {
                WriteOperation::Delete { key } => (key.len(), 0),
                WriteOperation::Put { key, value } => (key.len(), value.len()),
                WriteOperation::DeletePrefix { key_prefix } => (key_prefix.len(), 0),
            };
            let operation_size = key_len + value_len + self.root_key.len();
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
                    let mut full_key = self.namespace.clone();
                    full_key.extend(&self.root_key);
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

    fn namespace_as_vec(namespace: &str) -> Result<Vec<u8>, ServiceStoreError> {
        let mut key = vec![KeyTag::Key as u8];
        bcs::serialize_into(&mut key, namespace)?;
        Ok(key)
    }

    async fn submit_statements(&self, statements: Vec<Statement>) -> Result<(), ServiceStoreError> {
        if !statements.is_empty() {
            let query = RequestWriteBatchExtended { statements };
            let request = tonic::Request::new(query);
            let mut client = self.client.write().await;
            let _guard = self.acquire().await;
            let _response = client.process_write_batch_extended(request).await?;
        }
        Ok(())
    }

    fn get_statement(&self, operation: WriteOperation) -> Statement {
        let operation = match operation {
            WriteOperation::Delete { key } => {
                let mut full_key = self.namespace.clone();
                full_key.extend(&self.root_key);
                full_key.extend(key);
                Operation::Delete(full_key)
            }
            WriteOperation::Put { key, value } => {
                let mut full_key = self.namespace.clone();
                full_key.extend(&self.root_key);
                full_key.extend(key);
                Operation::Put(KeyValue {
                    key: full_key,
                    value,
                })
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                let mut full_key_prefix = self.namespace.clone();
                full_key_prefix.extend(&self.root_key);
                full_key_prefix.extend(key_prefix);
                Operation::DeletePrefix(full_key_prefix)
            }
        };
        Statement {
            operation: Some(operation),
        }
    }

    async fn read_entries<S: DeserializeOwned>(
        mut client: RwLockWriteGuard<'_, StoreProcessorClient<Channel>>,
        message_index: i64,
        num_chunks: i32,
    ) -> Result<S, ServiceStoreError> {
        let mut value = Vec::new();
        for index in 0..num_chunks {
            let query = RequestSpecificChunk {
                message_index,
                index,
            };
            let request = tonic::Request::new(query);
            let response = client.process_specific_chunk(request).await?;
            let response = response.into_inner();
            let ReplySpecificChunk { chunk } = response;
            value.extend(chunk);
        }
        Ok(bcs::from_bytes(&value)?)
    }
}

impl AdminKeyValueStore for ServiceStoreClientInternal {
    type Config = ServiceStoreConfig;

    async fn new_test_config() -> Result<ServiceStoreConfig, ServiceStoreError> {
        let endpoint = storage_service_test_endpoint()?;
        service_config_from_endpoint(&endpoint)
    }

    async fn new_benchmark_config() -> Result<ServiceStoreConfig, ServiceStoreError> {
        Self::new_test_config().await
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, ServiceStoreError> {
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let client = StoreProcessorClient::connect(endpoint).await?;
        let client = Arc::new(RwLock::new(client));
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.common_config.max_stream_queries;
        let cache_size = config.common_config.cache_size;
        let namespace = Self::namespace_as_vec(namespace)?;
        let root_key = root_key.to_vec();
        Ok(Self {
            client,
            semaphore,
            max_stream_queries,
            cache_size,
            namespace,
            root_key,
        })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, ServiceStoreError> {
        let client = self.client.clone();
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        let cache_size = self.cache_size;
        let namespace = self.namespace.clone();
        let root_key = root_key.to_vec();
        Ok(Self {
            client,
            semaphore,
            max_stream_queries,
            cache_size,
            namespace,
            root_key,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ServiceStoreError> {
        let query = RequestListAll {};
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let response = client.process_list_all(request).await?;
        let response = response.into_inner();
        let ReplyListAll { namespaces } = response;
        let namespaces = namespaces
            .into_iter()
            .map(|x| bcs::from_bytes(&x))
            .collect::<Result<_, _>>()?;
        Ok(namespaces)
    }

    async fn delete_all(config: &Self::Config) -> Result<(), ServiceStoreError> {
        let query = RequestDeleteAll {};
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_all(request).await?;
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, ServiceStoreError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestExistsNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let response = client.process_exists_namespace(request).await?;
        let response = response.into_inner();
        let ReplyExistsNamespace { exists } = response;
        Ok(exists)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), ServiceStoreError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestCreateNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_create_namespace(request).await?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), ServiceStoreError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestDeleteNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_namespace(request).await?;
        Ok(())
    }
}

/// Creates the `CommonStoreConfig` for the `ServiceStoreClientInternal`.
pub fn create_service_store_common_config() -> CommonStoreConfig {
    let max_stream_queries = 100;
    let cache_size = 10; // unused
    CommonStoreConfig {
        max_concurrent_queries: None,
        max_stream_queries,
        cache_size,
    }
}

/// Creates a `ServiceStoreConfig` from an endpoint.
pub fn service_config_from_endpoint(
    endpoint: &str,
) -> Result<ServiceStoreConfig, ServiceStoreError> {
    let common_config = create_service_store_common_config();
    let endpoint = endpoint.to_string();
    Ok(ServiceStoreConfig {
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
    let root_key = &[];
    let store = ServiceStoreClientInternal::connect(&config, namespace, root_key).await?;
    let _value = store.read_value_bytes(&[42]).await?;
    Ok(())
}

/// Creates a test store with an endpoint. The namespace is random.
#[cfg(with_testing)]
pub async fn create_service_test_store() -> Result<ServiceStoreClientInternal, ServiceStoreError> {
    let config = ServiceStoreClientInternal::new_test_config().await?;
    let namespace = generate_test_namespace();
    let root_key = &[];
    ServiceStoreClientInternal::connect(&config, &namespace, root_key).await
}

#[derive(Clone)]
pub struct ServiceStoreClient {
    #[cfg(with_metrics)]
    store: MeteredStore<LruCachingStore<MeteredStore<ServiceStoreClientInternal>>>,
    #[cfg(not(with_metrics))]
    store: LruCachingStore<ServiceStoreClientInternal>,
}

impl WithError for ServiceStoreClient {
    type Error = ServiceStoreError;
}

impl ReadableKeyValueStore for ServiceStoreClient {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ServiceStoreError> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ServiceStoreError> {
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, ServiceStoreError> {
        self.store.contains_keys(keys).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ServiceStoreError> {
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, ServiceStoreError> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ServiceStoreError> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl WritableKeyValueStore for ServiceStoreClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), ServiceStoreError> {
        self.store.write_batch(batch).await
    }

    async fn clear_journal(&self) -> Result<(), ServiceStoreError> {
        self.store.clear_journal().await
    }
}

impl AdminKeyValueStore for ServiceStoreClient {
    type Config = ServiceStoreConfig;

    async fn new_test_config() -> Result<ServiceStoreConfig, ServiceStoreError> {
        ServiceStoreClientInternal::new_test_config().await
    }

    async fn new_benchmark_config() -> Result<ServiceStoreConfig, ServiceStoreError> {
        ServiceStoreClientInternal::new_benchmark_config().await
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, ServiceStoreError> {
        let cache_size = config.common_config.cache_size;
        let store = ServiceStoreClientInternal::connect(config, namespace, root_key).await?;
        Ok(ServiceStoreClient::from_inner(store, cache_size))
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, ServiceStoreError> {
        let store = self.inner().clone_with_root_key(root_key)?;
        let cache_size = self.inner().cache_size;
        Ok(ServiceStoreClient::from_inner(store, cache_size))
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ServiceStoreError> {
        ServiceStoreClientInternal::list_all(config).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), ServiceStoreError> {
        ServiceStoreClientInternal::delete_all(config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, ServiceStoreError> {
        ServiceStoreClientInternal::exists(config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), ServiceStoreError> {
        ServiceStoreClientInternal::create(config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), ServiceStoreError> {
        ServiceStoreClientInternal::delete(config, namespace).await
    }
}

impl ServiceStoreClient {
    #[cfg(with_metrics)]
    fn inner(&self) -> &ServiceStoreClientInternal {
        &self.store.store.store.store
    }

    #[cfg(not(with_metrics))]
    fn inner(&self) -> &ServiceStoreClientInternal {
        &self.store.store
    }

    fn from_inner(store: ServiceStoreClientInternal, cache_size: usize) -> ServiceStoreClient {
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&STORAGE_SERVICE_METRICS, store);
        let store = LruCachingStore::new(store, cache_size);
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&LRU_STORAGE_SERVICE_METRICS, store);
        Self { store }
    }
}
