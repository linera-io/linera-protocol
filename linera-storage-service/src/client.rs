// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(any(test, feature = "test"))]
use linera_views::{lru_caching::TEST_CACHE_SIZE, test_utils::generate_test_namespace};

use crate::{
    common::{SharedContextError, SharedStoreConfig},
    key_value_store::{
        statement::Operation, store_processor_client::StoreProcessorClient, KeyValue,
        ReplyContainsKey, ReplyExistNamespace, ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix,
        ReplyListAll, ReplyReadMultiValues, ReplyReadValue, RequestClearJournal,
        RequestContainsKey, RequestCreateNamespace, RequestDeleteAll, RequestDeleteNamespace,
        RequestExistNamespace, RequestFindKeyValuesByPrefix, RequestFindKeysByPrefix,
        RequestListAll, RequestReadMultiValues, RequestReadValue, RequestWriteBatch, Statement,
    },
};
use async_lock::{RwLock, Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use linera_views::{
    batch::Batch,
    common::{
        AdminKeyValueStore, CommonStoreConfig, KeyValueStore, ReadableKeyValueStore,
        WritableKeyValueStore,
    },
};
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};

/// The number of concurrent queries of a test shared store
#[cfg(any(test, feature = "test"))]
const TEST_SHARED_STORE_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of concurrent stream queries
#[cfg(any(test, feature = "test"))]
const TEST_SHARED_STORE_MAX_STREAM_QUERIES: usize = 10;

/// The shared store client.
/// * Interior mutability is required for client because
/// accessing requires mutability while the KeyValueStore
/// does not allow it.
/// * The semaphore and max_stream_queries work as other
/// stores.
///
/// The encoding of namespaces is done by taking their
/// serialization. This works because the set of serialization
/// of strings is prefix free.
/// The data is stored in the following way.
/// * A `key` in a `namespace` is stored as
///   [0] + [namespace] + [key]
/// * An additional key with empty value is stored at
///   [1] + [namespace]
/// is stored to indicate the existence of a namespace.
#[derive(Clone)]
pub struct SharedStoreClient {
    client: Arc<RwLock<StoreProcessorClient<Channel>>>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    namespace: Vec<u8>,
}

#[async_trait]
impl ReadableKeyValueStore<SharedContextError> for SharedStoreClient {
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SharedContextError> {
        let mut full_key = self.namespace.clone();
        full_key.extend(key);
        let query = RequestReadValue { key: full_key };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let response = client.process_read_value(request).await?;
        let response = response.get_ref();
        let ReplyReadValue { value } = response;
        Ok(value.clone())
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, SharedContextError> {
        let mut full_key = self.namespace.clone();
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

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, SharedContextError> {
        let mut full_keys = Vec::new();
        for key in keys {
            let mut full_key = self.namespace.clone();
            full_key.extend(&key);
            full_keys.push(full_key);
        }
        let query = RequestReadMultiValues { keys: full_keys };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let response = client.process_read_multi_values(request).await?;
        let response = response.into_inner();
        let ReplyReadMultiValues { values } = response;
        let values = values.into_iter().map(|x| x.value).collect::<Vec<_>>();
        Ok(values)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, SharedContextError> {
        let mut full_key_prefix = self.namespace.clone();
        full_key_prefix.extend(key_prefix);
        let query = RequestFindKeysByPrefix {
            key_prefix: full_key_prefix,
        };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let response = client.process_find_keys_by_prefix(request).await?;
        let response = response.into_inner();
        let ReplyFindKeysByPrefix { keys } = response;
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SharedContextError> {
        let mut full_key_prefix = self.namespace.clone();
        full_key_prefix.extend(key_prefix);
        let query = RequestFindKeyValuesByPrefix {
            key_prefix: full_key_prefix,
        };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let response = client.process_find_key_values_by_prefix(request).await?;
        let response = response.into_inner();
        let ReplyFindKeyValuesByPrefix { key_values } = response;
        let key_values = key_values
            .into_iter()
            .map(|x| (x.key, x.value))
            .collect::<Vec<_>>();
        Ok(key_values)
    }
}

#[async_trait]
impl WritableKeyValueStore<SharedContextError> for SharedStoreClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), SharedContextError> {
        use crate::client::Operation;
        use linera_views::batch::WriteOperation;
        let mut statements = Vec::new();
        for operation in batch.operations {
            let operation = match operation {
                WriteOperation::Delete { key } => {
                    let mut full_key = self.namespace.clone();
                    full_key.extend(key);
                    Operation::Delete(full_key)
                }
                WriteOperation::Put { key, value } => {
                    let mut full_key = self.namespace.clone();
                    full_key.extend(key);
                    Operation::Put(KeyValue {
                        key: full_key,
                        value,
                    })
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    let mut full_key_prefix = self.namespace.clone();
                    full_key_prefix.extend(key_prefix);
                    Operation::DeletePrefix(full_key_prefix)
                }
            };
            let statement = Statement {
                operation: Some(operation),
            };
            statements.push(statement);
        }
        let query = RequestWriteBatch {
            statements,
            base_key: base_key.to_vec(),
        };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let _response = client.process_write_batch(request).await?;
        Ok(())
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), SharedContextError> {
        let mut full_base_key = self.namespace.clone();
        full_base_key.extend(base_key);
        let query = RequestClearJournal {
            base_key: full_base_key,
        };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let _response = client.process_clear_journal(request).await?;
        Ok(())
    }
}

impl KeyValueStore for SharedStoreClient {
    type Error = SharedContextError;
}

impl SharedStoreClient {
    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
        }
    }

    fn namespace_as_vec(namespace: &str) -> Result<Vec<u8>, SharedContextError> {
        let mut key = vec![0];
        bcs::serialize_into(&mut key, namespace)?;
        Ok(key)
    }
}

#[async_trait]
impl AdminKeyValueStore<SharedContextError> for SharedStoreClient {
    type Config = SharedStoreConfig;

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, SharedContextError> {
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?;
        let client = StoreProcessorClient::connect(endpoint).await?;
        let client = Arc::new(RwLock::new(client));
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.common_config.max_stream_queries;
        let namespace = Self::namespace_as_vec(namespace)?;
        Ok(SharedStoreClient {
            client,
            semaphore,
            max_stream_queries,
            namespace,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, SharedContextError> {
        let query = RequestListAll {};
        let request = tonic::Request::new(query);
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?;
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

    async fn delete_all(config: &Self::Config) -> Result<(), SharedContextError> {
        let query = RequestDeleteAll {};
        let request = tonic::Request::new(query);
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_all(request).await?;
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, SharedContextError> {
        let namespace = Self::namespace_as_vec(namespace)?;
        let query = RequestExistNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let response = client.process_exist_namespace(request).await?;
        let response = response.into_inner();
        let ReplyExistNamespace { test } = response;
        Ok(test)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), SharedContextError> {
        let namespace = Self::namespace_as_vec(namespace)?;
        let query = RequestCreateNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_create_namespace(request).await?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), SharedContextError> {
        let namespace = Self::namespace_as_vec(namespace)?;
        let query = RequestDeleteNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_namespace(request).await?;
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
pub fn create_shared_store_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: Some(TEST_SHARED_STORE_MAX_CONCURRENT_QUERIES),
        max_stream_queries: TEST_SHARED_STORE_MAX_STREAM_QUERIES,
        cache_size: TEST_CACHE_SIZE,
    }
}

#[cfg(any(test, feature = "test"))]
pub async fn create_shared_test_store(
    endpoint: String,
) -> Result<SharedStoreClient, SharedContextError> {
    let common_config = create_shared_store_common_config();
    let namespace = generate_test_namespace();
    let endpoint = format!("http://{}", endpoint);
    let config = SharedStoreConfig {
        endpoint,
        common_config,
    };
    SharedStoreClient::connect(&config, &namespace).await
}

#[cfg(any(test, feature = "test"))]
pub(crate) async fn storage_service_check_endpoint(
    endpoint: String,
) -> Result<(), SharedContextError> {
    let store = create_shared_test_store(endpoint).await?;
    let _value = store.read_value_bytes(&[0]).await?;
    Ok(())
}
