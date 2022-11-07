// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    impl_context,
    hash::HashingContext,
    views::{Context, ViewError},
    common::{WriteOperation, Batch, KeyValueOperations},
};
//use linera_views::common::KeyValueOperations;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use thiserror::Error;

pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;
pub type RocksdbContainer = Arc<DB>;

/// An implementation of [`crate::views::Context`] based on Rocksdb
#[derive(Debug, Clone)]
pub struct RocksdbContext<E> {
    db: RocksdbContainer,
    base_key: Vec<u8>,
    extra: E,
}

#[async_trait]
impl KeyValueOperations for RocksdbContainer {
    type E = RocksdbContextError;
    async fn read_key<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<V>, RocksdbContextError> {
        let db = self.clone();
        let key = key.to_vec();
        let value = tokio::task::spawn_blocking(move || db.get(&key)).await??;
        match &value {
            None => Ok(None),
            Some(bytes) => Ok(Some(bcs::from_bytes(bytes)?)),
        }
    }

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksdbContextError> {
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
        Ok(keys)
    }

    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, RocksdbContextError> {
        let len = key_prefix.len();
        let mut keys = Vec::new();
        for key in self.find_keys_with_prefix(key_prefix).await? {
            keys.push(bcs::from_bytes(&key[len..])?);
        }
        Ok(keys)
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), RocksdbContextError> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || -> Result<(), RocksdbContextError> {
            let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
            for e_ent in batch.operations {
		match e_ent {
                    WriteOperation::Delete { key } => inner_batch.delete(&key),
                    WriteOperation::Put { key, value } => inner_batch.put(&key, value),
                }
            }
            db.write(inner_batch)?;
	    Ok(())
        })
        .await??;
        Ok(())
    }

}

impl<E> RocksdbContext<E> {
    pub fn new(db: RocksdbContainer, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            db,
            base_key,
            extra,
        }
    }
}


impl_context!{RocksdbContext,RocksdbContextError}

/*
#[async_trait]
impl<E> Context for RocksdbContext<E>
where
    E: Clone + Send + Sync,
{
    type Extra = E;
    type Error = RocksdbContextError;

    fn extra(&self) -> &E {
        &self.extra
    }

    fn base_key(&self) -> Vec<u8> {
        self.base_key.clone()
    }

    fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>,RocksdbContextError> {
        let mut key = self.base_key.clone();
        bcs::serialize_into(&mut key, index)?;
        assert!(
            key.len() > self.base_key.len(),
            "Empty indices are not allowed"
        );
        Ok(key)
    }

    async fn read_key<V: DeserializeOwned>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<V>, RocksdbContextError> {
        self.db.read_key(key).await
    }

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksdbContextError> {
        self.db.find_keys_with_prefix(key_prefix).await
    }

    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, RocksdbContextError> {
        self.db.get_sub_keys(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        self.db.write_batch(batch).await?;
        Ok(())
    }

    fn clone_self(&self, base_key: Vec<u8>) -> Self {
        Self {
            db: self.db.clone(),
            base_key,
            extra: self.extra.clone(),
        }
    }
}
 */

impl<E> HashingContext for RocksdbContext<E>
where
    E: Clone + Send + Sync,
{
    type Hasher = sha2::Sha512;
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
            backend: "memory".to_string(),
            error: error.to_string(),
        }
    }
}
