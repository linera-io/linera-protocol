// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(any(test, feature = "test"))]
use linera_views::lru_caching::TEST_CACHE_SIZE;

use crate::{
    common::{SharedContextError, SharedStoreConfig},
    key_value_store::{
        statement::Operation, store_processor_client::StoreProcessorClient, KeyValue,
        ReplyContainsKey, ReplyFindKeyValuesByPrefix, ReplyFindKeysByPrefix, ReplyReadMultiValues,
        ReplyReadValue, RequestClearJournal, RequestContainsKey, RequestFindKeyValuesByPrefix,
        RequestFindKeysByPrefix, RequestReadMultiValues, RequestReadValue, RequestWriteBatch,
        Statement,
    },
};
use async_lock::{RwLock, Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use linera_views::{
    batch::Batch,
    common::{CommonStoreConfig, KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore},
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
pub struct SharedStoreClient {
    client: Arc<RwLock<StoreProcessorClient<Channel>>>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
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
        let query = RequestReadValue { key: key.to_vec() };
        let request = tonic::Request::new(query);
        let mut client = self.client.write().await;
        let _guard = self.acquire().await;
        let response = client.process_read_value(request).await?;
        let response = response.get_ref();
        let ReplyReadValue { value } = response;
        Ok(value.clone())
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, SharedContextError> {
        let query = RequestContainsKey { key: key.to_vec() };
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
        let query = RequestReadMultiValues { keys };
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
        let query = RequestFindKeysByPrefix {
            key_prefix: key_prefix.to_vec(),
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
        let query = RequestFindKeyValuesByPrefix {
            key_prefix: key_prefix.to_vec(),
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
                WriteOperation::Delete { key } => Operation::Delete(key),
                WriteOperation::Put { key, value } => Operation::Put(KeyValue { key, value }),
                WriteOperation::DeletePrefix { key_prefix } => Operation::DeletePrefix(key_prefix),
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
        let query = RequestClearJournal {
            base_key: base_key.to_vec(),
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

    async fn new_internal(
        endpoint: String,
        common_config: CommonStoreConfig,
    ) -> Result<Self, SharedContextError> {
        let endpoint = Endpoint::from_shared(endpoint)?;
        let client = StoreProcessorClient::connect(endpoint).await?;
        let client = Arc::new(RwLock::new(client));
        let semaphore = common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = common_config.max_stream_queries;
        Ok(SharedStoreClient {
            client,
            semaphore,
            max_stream_queries,
        })
    }

    pub async fn new(config: SharedStoreConfig) -> Result<Self, SharedContextError> {
        Self::new_internal(config.endpoint, config.common_config).await
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
    let endpoint = format!("http://{}", endpoint);
    let store_config = SharedStoreConfig {
        endpoint,
        common_config,
    };
    SharedStoreClient::new(store_config).await
}
