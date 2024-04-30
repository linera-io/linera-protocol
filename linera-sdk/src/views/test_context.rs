// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! [`Context`] implementation for unit tests.

use async_trait::async_trait;
use linera_views::{
    batch::Batch,
    common::Context,
    memory::{create_memory_context, MemoryContext},
};

/// Implementation of [`Context`] to be used for data storage by Linera application unit tests.
#[cfg(with_testing)]
#[derive(Clone)]
pub struct ViewStorageContext(MemoryContext<()>);

impl Default for ViewStorageContext {
    fn default() -> Self {
        ViewStorageContext(create_memory_context())
    }
}

#[async_trait]
impl Context for ViewStorageContext {
    const MAX_VALUE_SIZE: usize = MemoryContext::<()>::MAX_VALUE_SIZE;
    const MAX_KEY_SIZE: usize = MemoryContext::<()>::MAX_KEY_SIZE;

    type Extra = ();
    type Error = <MemoryContext<()> as Context>::Error;
    type Keys = <MemoryContext<()> as Context>::Keys;
    type KeyValues = <MemoryContext<()> as Context>::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.0.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.0.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.0.contains_key(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.0.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.0.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.0.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        self.0.write_batch(batch).await
    }

    fn extra(&self) -> &() {
        self.0.extra()
    }

    fn clone_with_base_key(&self, base_key: Vec<u8>) -> Self {
        ViewStorageContext(self.0.clone_with_base_key(base_key))
    }

    fn base_key(&self) -> Vec<u8> {
        self.0.base_key()
    }
}
