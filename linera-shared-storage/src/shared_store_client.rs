// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//use tonic::{transport::Server, Request, Response, Status};
use crate::key_value_store::{
    statement::Operation,
    store_processor_client::{StoreProcessorClient},
    store_reply::Reply,
    store_request::Query,
    KeyValue, KeyValues, Keys, OptValue, OptValues, StoreReply, StoreRequest,
};
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
        let mut client = self.client.write().await;
        let query = Some(Query::ReadValue(key.to_vec()));
        let request = tonic::Request::new(StoreRequest {
            query
        });
        let response = client.store_process(request).await.unwrap();
        let response = response.get_ref();
        let StoreReply { reply } = response;
        let Some(Reply::ReadValue(value)) = reply else {
            unreachable!();
        };
        Ok(value.value.clone())
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ViewError> {
        let mut client = self.client.write().await;
        let query = Some(Query::ContainsKey(key.to_vec()));
        let request = tonic::Request::new(StoreRequest {
            query
        });
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
        todo!()
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, ViewError> {
        todo!()
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        todo!()
    }
}

#[async_trait]
impl WritableKeyValueStore<ViewError> for SharedStoreClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
        todo!()
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), ViewError> {
        todo!()
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

