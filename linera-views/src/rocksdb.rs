// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::common::{get_interval_kernel, Batch, ContextFromDb, KeyValueOperations, SimpleKeyIterator, WriteOperation};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;
pub type RocksdbContainer = Arc<DB>;

/// An implementation of [`crate::views::Context`] based on Rocksdb
pub type RocksdbContext<E> = ContextFromDb<E, RocksdbContainer>;

#[async_trait]
impl KeyValueOperations for RocksdbContainer {
    type Error = RocksdbContextError;
    type KeyIterator = SimpleKeyIterator<RocksdbContextError>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksdbContextError> {
        let db = self.clone();
        let key = key.to_vec();
        Ok(tokio::task::spawn_blocking(move || db.get(&key)).await??)
    }

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyIterator, RocksdbContextError> {
        let db = self.clone();
        let prefix = key_prefix.to_vec();
        let keys = tokio::task::spawn_blocking(move || {
            let mut iter = db.raw_iterator();
            let mut keys = Vec::new();
            iter.seek(&prefix);
            let mut next_key = iter.key();
            while let Some(key) = next_key {
                if !key.starts_with(&prefix) {
                    break;
                }
                keys.push(key.to_vec());
                iter.next();
                next_key = iter.key();
            }
            keys
        })
        .await?;
        Ok(SimpleKeyIterator::new(keys))
    }

    async fn write_batch(&self, mut batch: Batch) -> Result<(), RocksdbContextError> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), RocksdbContextError> {
            let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
            for e_ent in batch.operations {
                match e_ent {
                    WriteOperation::Delete { key } => inner_batch.delete(&key),
                    WriteOperation::Put { key, value } => inner_batch.put(&key, value),
                    WriteOperation::DeletePrefix { key_prefix } => {
                        let pair = get_interval_kernel(key_prefix);
                        let lower_bound = pair.0;
                        let upper_bound = match pair.1 {
                            None => Vec::new(), // That is a hack, not sure to work.
                            Some(val) => val,
                        };
                        inner_batch.delete_range(lower_bound, upper_bound);
                    },
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
