// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tonic::{transport::Server, Request, Response, Status};
use crate::key_value_store::{
    statement::Operation,
    store_processor_server::{StoreProcessor, StoreProcessorServer},
    store_reply::Reply,
    store_request::Query,
    KeyValue, KeyValues, Keys, OptValue, OptValues, StoreReply, StoreRequest,
};
use linera_service::storage::StoreConfig;
use linera_views::views::ViewError;
use linera_views::common::{ReadableKeyValueStore, WritableKeyValueStore};

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

pub struct SharedStoreClient {
    max_stream_queries: usize,
}

impl ReadableKeyValueStore<ViewError> for SharedStoreClient {
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        todo!()
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ViewError> {
        todo!()
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

impl WritableKeyValueStore<ViewError> for SharedStoreClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), ViewError> {
    }
}

impl KeyValueStore for SharedStoreClient {
    type Error = ViewError;
}
