// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::{ServiceContextError, ServiceStoreConfig},
    key_value_store::{
        statement::Operation, store_processor_client::StoreProcessorClient, KeyValue, KeyValueAppend,
        ReplyContainsKey, ReplyExistsNamespace, ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix,
        ReplyListAll, ReplyReadMultiValues, ReplyReadValue,
        RequestContainsKey, RequestCreateNamespace, RequestDeleteAll, RequestDeleteNamespace,
        RequestExistsNamespace, RequestFindKeyValuesByPrefix, RequestFindKeysByPrefix,
        RequestListAll, RequestReadMultiValues, RequestReadValue, RequestWriteBatchExtended, Statement,
    },
};
use async_lock::{RwLock, Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use linera_views::{
    batch::Batch,
    common::{
        AdminKeyValueStore, CommonStoreConfig, KeyValueStore, ReadableKeyValueStore,
        WritableKeyValueStore, MIN_VIEW_TAG,
    },
};
use linera_views::batch::WriteOperation;
use std::mem;
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};

#[cfg(any(test, feature = "test"))]
use linera_views::test_utils::generate_test_namespace;

// The maximal block size on GRPC is 4M.
// That size is the one that shows up everywhere and in particular
// for tonic.
// We decrease the 4194304 to 4000000 for safety reasons.
const MAX_GRPC_REQUEST_SIZE: usize = 4000000;

/// Key tags to create the sub keys used for storing data on storage.
#[repr(u8)]
pub(crate) enum KeyTag {
    /// Prefix for the storage of the keys of the map
    Key = MIN_VIEW_TAG,
}

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
pub struct ServiceStoreClient {
    client: Arc<RwLock<StoreProcessorClient<Channel>>>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    namespace: Vec<u8>,
}

#[async_trait]
impl ReadableKeyValueStore<ServiceContextError> for ServiceStoreClient {
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ServiceContextError> {
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

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ServiceContextError> {
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
    ) -> Result<Vec<Option<Vec<u8>>>, ServiceContextError> {
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
    ) -> Result<Vec<Vec<u8>>, ServiceContextError> {
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ServiceContextError> {
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
impl WritableKeyValueStore<ServiceContextError> for ServiceStoreClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ServiceContextError> {
        use crate::client::Operation;
        use linera_views::batch::WriteOperation;
        let mut statements = Vec::new();
        let mut block_size = 0;
        for operation in batch.operations {
            let operation_size = match &operation {
                WriteOperation::Delete { key } => {
                    key.len()
                }
                WriteOperation::Put { key, value } => {
                    key.len() + value.len()
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    key_prefix.len()
                }
            };
            if operation_size + block_size < MAX_GRPC_REQUEST_SIZE {
                let statement = self.get_statement(operation);
                statements.push(statement);
                block_size += operation_size;
            } else {
                self.submit_statements(mem::take(&mut statements)).await?;
                block_size = 0;
                if operation_size > MAX_GRPC_REQUEST_SIZE {
                    // One single operation is especially big. So split it in blocks.
                    let WriteOperation::Put { key, value } = operation else {
                        // Only the put can go over the limit
                        unreachable!();
                    };
                    let mut full_key = self.namespace.clone();
                    full_key.extend(key);
                    let value_blocks = value.chunks(MAX_GRPC_REQUEST_SIZE).collect::<Vec<_>>();
                    let n_block = value_blocks.len();
                    for i_block in 0..n_block {
                        let last = i_block + 1 == n_block;
                        let value = value_blocks[i_block].to_vec();
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
                    block_size = operation_size;
                }
            }
        }
        self.submit_statements(mem::take(&mut statements)).await
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), ServiceContextError> {
        Ok(())
    }
}

impl KeyValueStore for ServiceStoreClient {
    type Error = ServiceContextError;
}

impl ServiceStoreClient {
    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
        }
    }

    fn namespace_as_vec(namespace: &str) -> Result<Vec<u8>, ServiceContextError> {
        let mut key = vec![KeyTag::Key as u8];
        bcs::serialize_into(&mut key, namespace)?;
        Ok(key)
    }

    async fn submit_statements(&self, statements: Vec<Statement>) -> Result<(), ServiceContextError> {
        println!("submit_statements |statements|={}", statements.len());
        if statements.len() > 0 {
            let query = RequestWriteBatchExtended {
                statements,
            };
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
        Statement {
            operation: Some(operation),
        }
    }
}

#[async_trait]
impl AdminKeyValueStore for ServiceStoreClient {
    type Error = ServiceContextError;
    type Config = ServiceStoreConfig;

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, ServiceContextError> {
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let client = StoreProcessorClient::connect(endpoint).await?;
        let client = Arc::new(RwLock::new(client));
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.common_config.max_stream_queries;
        let namespace = Self::namespace_as_vec(namespace)?;
        Ok(ServiceStoreClient {
            client,
            semaphore,
            max_stream_queries,
            namespace,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ServiceContextError> {
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

    async fn delete_all(config: &Self::Config) -> Result<(), ServiceContextError> {
        let query = RequestDeleteAll {};
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_delete_all(request).await?;
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, ServiceContextError> {
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

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), ServiceContextError> {
        let namespace = bcs::to_bytes(namespace)?;
        let query = RequestCreateNamespace { namespace };
        let request = tonic::Request::new(query);
        let endpoint = format!("http://{}", config.endpoint);
        let endpoint = Endpoint::from_shared(endpoint)?;
        let mut client = StoreProcessorClient::connect(endpoint).await?;
        let _response = client.process_create_namespace(request).await?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), ServiceContextError> {
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

/// Creates the `CommonStoreConfig` for the `ServiceStoreClient`.
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
) -> Result<ServiceStoreConfig, ServiceContextError> {
    let common_config = create_service_store_common_config();
    let endpoint = endpoint.to_string();
    Ok(ServiceStoreConfig {
        endpoint,
        common_config,
    })
}

/// Checks that endpoint is truly absent.
pub async fn storage_service_check_absence(endpoint: &str) -> Result<bool, ServiceContextError> {
    let endpoint = Endpoint::from_shared(endpoint.to_string())?;
    let result = StoreProcessorClient::connect(endpoint).await;
    Ok(result.is_err())
}

/// Checks whether an endpoint is valid or not.
pub async fn storage_service_check_validity(endpoint: &str) -> Result<(), ServiceContextError> {
    let config = service_config_from_endpoint(endpoint).unwrap();
    let namespace = "namespace";
    let store = ServiceStoreClient::connect(&config, namespace).await?;
    let _value = store.read_value_bytes(&[42]).await?;
    Ok(())
}

/// Creates a test store with an endpoint. The namespace is random.
#[cfg(any(test, feature = "test"))]
pub async fn create_service_test_store(
    endpoint: &str,
) -> Result<ServiceStoreClient, ServiceContextError> {
    let config = service_config_from_endpoint(endpoint).unwrap();
    let namespace = generate_test_namespace();
    ServiceStoreClient::connect(&config, &namespace).await
}
