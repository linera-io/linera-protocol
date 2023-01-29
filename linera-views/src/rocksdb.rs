// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::common::{get_upper_bound, Batch, ContextFromDb, KeyValueOperations, WriteOperation};
use async_trait::async_trait;
use std::{
    ops::{Bound, Bound::Excluded},
    sync::Arc,
};
use thiserror::Error;

pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;
pub type RocksdbContainer = Arc<DB>;

/// An implementation of [`crate::common::Context`] based on Rocksdb
pub type RocksdbContext<E> = ContextFromDb<E, RocksdbContainer>;

#[async_trait]
impl KeyValueOperations for RocksdbContainer {
    type Error = RocksdbContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksdbContextError> {
        let db = self.clone();
        let key = key.to_vec();
        Ok(tokio::task::spawn_blocking(move || db.get(&key)).await??)
    }

    async fn find_stripped_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, RocksdbContextError> {
        let db = self.clone();
        let prefix = key_prefix.to_vec();
        let len = prefix.len();
        let keys = tokio::task::spawn_blocking(move || {
            let mut iter = db.raw_iterator();
            let mut keys = Vec::new();
            iter.seek(&prefix);
            let mut next_key = iter.key();
            while let Some(key) = next_key {
                if !key.starts_with(&prefix) {
                    break;
                }
                keys.push(key[len..].to_vec());
                iter.next();
                next_key = iter.key();
            }
            keys
        })
        .await?;
        Ok(keys)
    }

    async fn find_stripped_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, RocksdbContextError> {
        let db = self.clone();
        let prefix = key_prefix.to_vec();
        let len = prefix.len();
        let key_values = tokio::task::spawn_blocking(move || {
            let mut iter = db.raw_iterator();
            let mut key_values = Vec::new();
            iter.seek(&prefix);
            let mut next_key = iter.key();
            while let Some(key) = next_key {
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some(value) = iter.value() {
                    let key_value = (key[len..].to_vec(), value.to_vec());
                    key_values.push(key_value);
                }
                iter.next();
                next_key = iter.key();
            }
            key_values
        })
        .await?;
        Ok(key_values)
    }

    async fn write_batch(&self, mut batch: Batch) -> Result<(), RocksdbContextError> {
        let db = self.clone();
        // NOTE: The delete_range functionality of rocksdb needs to have an upper bound in order to work.
        // Thus in order to have the system working, we need to handle the unlikely case of having to
        // delete a key starting with [255, ...., 255]
        let len = batch.operations.len();
        let mut keys = Vec::new();
        for i in 0..len {
            let op = batch.operations.get(i).unwrap();
            if let WriteOperation::DeletePrefix { key_prefix } = op {
                if get_upper_bound(key_prefix) == Bound::Unbounded {
                    for short_key in self.find_stripped_keys_by_prefix(key_prefix).await? {
                        let mut key = key_prefix.clone();
                        key.extend_from_slice(&short_key);
                        keys.push(key);
                    }
                }
            }
        }
        for key in keys {
            batch.operations.push(WriteOperation::Delete { key });
        }
        tokio::task::spawn_blocking(move || -> Result<(), RocksdbContextError> {
            let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
            for e_ent in batch.operations {
                match e_ent {
                    WriteOperation::Delete { key } => inner_batch.delete(&key),
                    WriteOperation::Put { key, value } => inner_batch.put(&key, value),
                    WriteOperation::DeletePrefix { key_prefix } => {
                        if let Excluded(upper_bound) = get_upper_bound(&key_prefix) {
                            inner_batch.delete_range(key_prefix, upper_bound);
                        }
                    }
                }
            }
            db.write(inner_batch)?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

impl<E: Clone + Send + Sync> RocksdbContext<E> {
    pub fn new(db: RocksdbContainer, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            db,
            base_key,
            extra,
        }
    }
}

#[derive(Error, Debug)]
pub enum RocksdbContextError {
    #[error("tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    #[error("Rocksdb error: {0}")]
    Rocksdb(#[from] rocksdb::Error),

    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),
}

impl From<RocksdbContextError> for crate::views::ViewError {
    fn from(error: RocksdbContextError) -> Self {
        Self::ContextError {
            backend: "rocksdb".to_string(),
            error: error.to_string(),
        }
    }
}
