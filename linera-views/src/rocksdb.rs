// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    hash::HashingContext,
    views::{
        AppendOnlyLogOperations, CollectionOperations, Context, MapOperations, QueueOperations,
        RegisterOperations, ScopedOperations, ViewError,
    },
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{ops::Range, sync::Arc};
use thiserror::Error;
use tokio::sync::OwnedMutexGuard;

pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// An implementation of [`crate::views::Context`] based on Rocksdb
#[derive(Debug, Clone)]
pub struct RocksdbContext {
    db: Arc<DB>,
    lock: Arc<OwnedMutexGuard<()>>,
    base_key: Vec<u8>,
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueOperations {
    async fn read_key<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<V>, RocksdbViewError>;

    async fn write_key<V: Serialize + Sync>(
        &self,
        key: &[u8],
        value: &V,
    ) -> Result<(), RocksdbViewError>;

    async fn delete_key(&self, key: &[u8]) -> Result<(), RocksdbViewError>;

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksdbViewError>;

    async fn count_keys(&self) -> Result<usize, RocksdbViewError>;
}

/// Low-level, blocking write operations.
trait WriteOperations {
    fn write_key<V: Serialize>(&mut self, key: &[u8], value: &V) -> Result<(), RocksdbViewError>;

    fn delete_key(&mut self, key: &[u8]);
}

#[async_trait]
impl KeyValueOperations for Arc<DB> {
    async fn read_key<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<V>, RocksdbViewError> {
        let db = self.clone();
        let key = key.to_vec();
        let value = tokio::task::spawn_blocking(move || db.get(&key)).await??;
        match &value {
            None => Ok(None),
            Some(bytes) => Ok(Some(bcs::from_bytes(bytes)?)),
        }
    }

    async fn write_key<V: Serialize + Sync>(
        &self,
        key: &[u8],
        value: &V,
    ) -> Result<(), RocksdbViewError> {
        let db = self.clone();
        let key = key.to_vec();
        let bytes = bcs::to_bytes(value)?;
        tokio::task::spawn_blocking(move || db.put(&key, bytes)).await??;
        Ok(())
    }

    async fn delete_key(&self, key: &[u8]) -> Result<(), RocksdbViewError> {
        let db = self.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || db.delete(&key)).await??;
        Ok(())
    }

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksdbViewError> {
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

    async fn count_keys(&self) -> Result<usize, RocksdbViewError> {
        let db = self.clone();
        let size =
            tokio::task::spawn_blocking(move || db.iterator(rocksdb::IteratorMode::Start).count())
                .await?;
        Ok(size)
    }
}

#[async_trait]
impl<'a> WriteOperations for rocksdb::WriteBatchWithTransaction<false> {
    fn write_key<V: Serialize>(&mut self, key: &[u8], value: &V) -> Result<(), RocksdbViewError> {
        let bytes = bcs::to_bytes(value)?;
        self.put(&key, bytes);
        Ok(())
    }

    fn delete_key(&mut self, key: &[u8]) {
        self.delete(&key);
    }
}

impl RocksdbContext {
    pub fn new(db: Arc<DB>, lock: OwnedMutexGuard<()>, base_key: Vec<u8>) -> Self {
        Self {
            db,
            lock: Arc::new(lock),
            base_key,
        }
    }

    fn derive_key<I: Serialize>(&self, index: &I) -> Vec<u8> {
        let mut key = self.base_key.clone();
        bcs::serialize_into(&mut key, index).expect("serialization should not fail");
        assert!(
            key.len() > self.base_key.len(),
            "Empty indices are not allowed"
        );
        key
    }
}

pub struct MyBatch(rocksdb::WriteBatchWithTransaction<false>);

impl std::ops::Deref for MyBatch {
    type Target = rocksdb::WriteBatchWithTransaction<false>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MyBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

unsafe impl Sync for MyBatch {}

#[async_trait]
impl Context for RocksdbContext {
    type Batch = MyBatch;
    type Error = RocksdbViewError;

    async fn run_with_batch<F>(&self, builder: F) -> Result<(), Self::Error>
    where
        F: FnOnce(&mut Self::Batch) -> futures::future::BoxFuture<Result<(), Self::Error>>
            + Send
            + Sync,
    {
        dbg!("run_with_batch");
        let mut batch = MyBatch(rocksdb::WriteBatchWithTransaction::default());
        dbg!("building batch");
        builder(&mut batch).await?;
        let db = self.db.clone();
        dbg!("applying batch");
        tokio::task::spawn_blocking(move || db.write(batch.0)).await??;
        dbg!("done");
        Ok(())
    }
}

#[async_trait]
impl ScopedOperations for RocksdbContext {
    fn clone_with_scope(&self, index: u64) -> Self {
        Self {
            db: self.db.clone(),
            lock: self.lock.clone(),
            base_key: self.derive_key(&index),
        }
    }
}

#[async_trait]
impl<T> RegisterOperations<T> for RocksdbContext
where
    T: Default + Serialize + DeserializeOwned + Send + Sync + 'static + std::fmt::Debug,
{
    async fn get(&mut self) -> Result<T, RocksdbViewError> {
        let value = self.db.read_key(&self.base_key).await?.unwrap_or_default();
        println!("Read {:?} -> {:?}", self.base_key, value);
        Ok(value)
    }

    async fn set(&mut self, batch: &mut Self::Batch, value: T) -> Result<(), RocksdbViewError> {
        println!(
            "adding register value to batch: {:?} -> {:?}",
            self.base_key, value
        );
        batch.write_key(&self.base_key, &value)?;
        Ok(())
    }

    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), Self::Error> {
        batch.delete_key(&self.base_key);
        Ok(())
    }
}

#[async_trait]
impl<T> AppendOnlyLogOperations<T> for RocksdbContext
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn count(&mut self) -> Result<usize, RocksdbViewError> {
        let count = self.db.read_key(&self.base_key).await?.unwrap_or_default();
        Ok(count)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, RocksdbViewError> {
        self.db.read_key(&self.derive_key(&index)).await
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, RocksdbViewError> {
        let mut values = Vec::new();
        for i in range {
            match self.db.read_key(&self.derive_key(&i)).await? {
                None => {
                    return Ok(values);
                }
                Some(value) => {
                    values.push(value);
                }
            }
        }
        Ok(values)
    }

    async fn append(
        &mut self,
        stored_count: usize,
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), RocksdbViewError> {
        if values.is_empty() {
            return Ok(());
        }
        let mut count = stored_count;
        for value in values {
            batch.write_key(&self.derive_key(&count), &value)?;
            count += 1;
        }
        batch.write_key(&self.base_key, &count)?;
        Ok(())
    }

    async fn delete(
        &mut self,
        stored_count: usize,
        batch: &mut Self::Batch,
    ) -> Result<(), Self::Error> {
        batch.delete_key(&self.base_key);
        for index in 0..stored_count {
            batch.delete_key(&self.derive_key(&index));
        }
        Ok(())
    }
}

#[async_trait]
impl<T> QueueOperations<T> for RocksdbContext
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error> {
        let range = self.db.read_key(&self.base_key).await?.unwrap_or_default();
        Ok(range)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        Ok(self.db.read_key(&self.derive_key(&index)).await?)
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::new();
        for i in range {
            match self.db.read_key(&self.derive_key(&i)).await? {
                None => {
                    return Ok(values);
                }
                Some(value) => {
                    values.push(value);
                }
            }
        }
        Ok(values)
    }

    async fn delete_front(
        &mut self,
        range: &mut Range<usize>,
        batch: &mut Self::Batch,
        count: usize,
    ) -> Result<(), Self::Error> {
        if count == 0 {
            return Ok(());
        }
        let deletion_range = range.clone().take(count);
        range.start += count;
        batch.write_key(&self.base_key, &range)?;
        for i in deletion_range {
            batch.delete_key(&self.derive_key(&i));
        }
        Ok(())
    }

    async fn append_back(
        &mut self,
        range: &mut Range<usize>,
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        if values.is_empty() {
            return Ok(());
        }
        for value in values {
            batch.write_key(&self.derive_key(&range.end), &value)?;
            range.end += 1;
        }
        batch.write_key(&self.base_key, &range)
    }

    async fn delete(
        &mut self,
        range: Range<usize>,
        batch: &mut Self::Batch,
    ) -> Result<(), RocksdbViewError> {
        batch.delete_key(&self.base_key);
        for i in range {
            batch.delete_key(&self.derive_key(&i));
        }
        Ok(())
    }
}

#[async_trait]
impl<I, V> MapOperations<I, V> for RocksdbContext
where
    I: Eq + Ord + Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get(&mut self, index: &I) -> Result<Option<V>, RocksdbViewError> {
        Ok(self.db.read_key(&self.derive_key(index)).await?)
    }

    async fn insert(
        &mut self,
        batch: &mut Self::Batch,
        index: I,
        value: V,
    ) -> Result<(), RocksdbViewError> {
        batch.write_key(&self.derive_key(&index), &value)?;
        Ok(())
    }

    async fn remove(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), RocksdbViewError> {
        batch.delete_key(&self.derive_key(&index));
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, RocksdbViewError> {
        let len = self.base_key.len();
        let mut keys = Vec::new();
        for key in self.db.find_keys_with_prefix(&self.base_key).await? {
            keys.push(bcs::from_bytes(&key[len..])?);
        }
        Ok(keys)
    }

    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), RocksdbViewError> {
        for key in self.db.find_keys_with_prefix(&self.base_key).await? {
            batch.delete_key(&key);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
enum CollectionKey<I> {
    Index(I),
    Subview(I),
}

#[async_trait]
impl<I> CollectionOperations<I> for RocksdbContext
where
    I: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    fn clone_with_scope(&self, index: &I) -> Self {
        Self {
            db: self.db.clone(),
            lock: self.lock.clone(),
            base_key: self.derive_key(&CollectionKey::Subview(index)),
        }
    }

    async fn add_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        batch.write_key(&self.derive_key(&CollectionKey::Index(index)), &())?;
        Ok(())
    }

    async fn remove_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        batch.delete_key(&self.derive_key(&CollectionKey::Index(index)));
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        // Hack: the BCS-serialization of `CollectionKey::Index(value)` for any `value` must
        // start with that of `CollectionKey::Index(())`, that is, the enum tag.
        let base = self.derive_key(&CollectionKey::Index(()));
        let len = base.len();
        let mut keys = Vec::new();
        for bytes in self.db.find_keys_with_prefix(&base).await? {
            match bcs::from_bytes(&bytes[len..]) {
                Ok(key) => {
                    keys.push(key);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(keys)
    }
}

impl HashingContext for RocksdbContext {
    type Hasher = sha2::Sha512;
}

#[derive(Error, Debug)]
pub enum RocksdbViewError {
    #[error("View error: {0}")]
    ViewError(#[from] ViewError),

    #[error("tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Rocksdb error: {0}")]
    Rocksdb(#[from] rocksdb::Error),

    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    #[error("Entry does not exist in Rocksdb: {0}")]
    NotFound(String),
}

impl From<RocksdbViewError> for linera_base::error::Error {
    fn from(error: RocksdbViewError) -> Self {
        Self::StorageError {
            backend: "rocksdb".to_string(),
            error: error.to_string(),
        }
    }
}
