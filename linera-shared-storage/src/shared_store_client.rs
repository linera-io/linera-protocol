// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//use tonic::{transport::Server, Request, Response, Status};
use crate::key_value_store::{
    statement::Operation,
    store_processor_client::{StoreProcessorClient},
    store_reply::Reply,
    store_request::Query,
    KeyValue, Keys, StoreReply, StoreRequest,
};
use crate::key_value_store::BatchBaseKey;
use crate::key_value_store::Statement;
use async_lock::RwLock;
use std::sync::Arc;
use tonic::transport::Channel;
//use linera_service::storage::StoreConfig;
use linera_views::views::ViewError;
use linera_views::common::{KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore};
use linera_views::batch::Batch;
use async_trait::async_trait;

pub struct SharedStoreClient {
    client: Arc<RwLock<StoreProcessorClient<Channel>>>,
    max_stream_queries: usize,
}

#[async_trait]
impl ReadableKeyValueStore<ViewError> for SharedStoreClient {
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        let query = Some(Query::ReadValue(key.to_vec()));
        let request = tonic::Request::new(StoreRequest {
            query
        });
        let mut client = self.client.write().await;
        let response = client.store_process(request).await.unwrap();
        let response = response.get_ref();
        let StoreReply { reply } = response;
        let Some(Reply::ReadValue(value)) = reply else {
            unreachable!();
        };
        Ok(value.value.clone())
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ViewError> {
        let query = Some(Query::ContainsKey(key.to_vec()));
        let request = tonic::Request::new(StoreRequest {
            query
        });
        let mut client = self.client.write().await;
        let response = client.store_process(request).await.unwrap();
        let response = response.get_ref();
        let StoreReply { reply } = response;
        let Some(Reply::ContainsKey(value)) = reply else {
            unreachable!();
        };
        Ok(*value)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ViewError> {
        let query = Some(Query::ReadMultiValues(Keys { keys }));
        let request = tonic::Request::new(StoreRequest {
            query
        });
        let mut client = self.client.write().await;
        let response = client.store_process(request).await.unwrap();
        let response = response.get_ref();
        let StoreReply { reply } = response;
        let Some(Reply::ReadMultiValues(values)) = reply else {
            unreachable!();
        };
        let values = values.values.clone().into_iter().map(|x| x.value).collect::<Vec<_>>();
        Ok(values)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, ViewError> {
        let query = Some(Query::FindKeysByPrefix(key_prefix.to_vec()));
        let request = tonic::Request::new(StoreRequest {
            query
        });
        let mut client = self.client.write().await;
        let response = client.store_process(request).await.unwrap();
        let response = response.get_ref();
        let StoreReply { reply } = response;
        let Some(Reply::FindKeysByPrefix(keys)) = reply else {
            unreachable!();
        };
        Ok(keys.keys.clone())
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let query = Some(Query::FindKeyValuesByPrefix(key_prefix.to_vec()));
        let request = tonic::Request::new(StoreRequest {
            query
        });
        let mut client = self.client.write().await;
        let response = client.store_process(request).await.unwrap();
        let response = response.get_ref();
        let StoreReply { reply } = response;
        let Some(Reply::FindKeyValuesByPrefix(key_values)) = reply else {
            unreachable!();
        };
        let key_values = key_values.key_values.clone().into_iter().map(|x| (x.key, x.value)).collect::<Vec<_>>();
        Ok(key_values)
    }
}

#[async_trait]
impl WritableKeyValueStore<ViewError> for SharedStoreClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), ViewError> {
        use linera_views::batch::WriteOperation;
        use crate::shared_store_client::Operation;
        let mut statements = Vec::new();
        for operation in batch.operations {
            let operation = match operation {
                WriteOperation::Delete { key} => {
                    Operation::DeleteKey(key)
                },
                WriteOperation::Put { key, value } => {
                    Operation::InsertKeyValue(KeyValue { key, value })
                },
                WriteOperation::DeletePrefix { key_prefix } => {
                    Operation::DeleteKeyPrefix(key_prefix)
                },
            };
            let statement = Statement { operation: Some(operation) };
            statements.push(statement);
        }
        let batch_base_key = BatchBaseKey { statements, base_key: base_key.to_vec() };
        let query = Some(Query::WriteBatch(batch_base_key));
        let request = tonic::Request::new(StoreRequest {
            query
        });
        let mut client = self.client.write().await;
        let response = client.store_process(request).await.unwrap();
        let response = response.get_ref();
        let StoreReply { reply } = response;
        let Some(Reply::WriteBatch(_unit)) = reply else {
            unreachable!();
        };
        Ok(())
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), ViewError> {
        let query = Some(Query::ClearJournal(base_key.to_vec()));
        let request = tonic::Request::new(StoreRequest {
            query
        });
        let mut client = self.client.write().await;
        let response = client.store_process(request).await.unwrap();
        let response = response.get_ref();
        let StoreReply { reply } = response;
        let Some(Reply::WriteBatch(_unit)) = reply else {
            unreachable!();
        };
        Ok(())
    }
}

impl KeyValueStore for SharedStoreClient {
    type Error = ViewError;
}







impl SharedStoreClient {
    pub async fn new(endpoint: String) -> Result<Self, ViewError> {
        // endpoint can be for example "http://[::1]:50051"
        let input = "http://[::1]:50051";
        let client = StoreProcessorClient::connect(input).await.unwrap();
        let client = Arc::new(RwLock::new(client));
        let max_stream_queries = 10;
        Ok(SharedStoreClient { client, max_stream_queries })
    }
}

