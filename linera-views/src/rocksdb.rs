// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    chain_guards::ChainGuard,
    hash::HashingContext,
    views::{
        CollectionOperations, Context, LogOperations, MapOperations, QueueOperations,
        RegisterOperations, ScopedOperations, ViewError,
    },
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{ops::Range, sync::Arc};
use thiserror::Error;

pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// An implementation of [`crate::views::Context`] based on Rocksdb
#[derive(Debug, Clone)]
pub struct RocksdbContext<E> {
    db: Arc<DB>,
    guard: Arc<ChainGuard>,
    base_key: Vec<u8>,
    extra: E,
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
    fn write_key<V: Serialize>(&mut self, key: Vec<u8>, value: &V) -> Result<(), RocksdbViewError>;

    fn delete_key(&mut self, key: Vec<u8>);
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
impl WriteOperations for MyBatch {
    fn write_key<V: Serialize>(&mut self, key: Vec<u8>, value: &V) -> Result<(), RocksdbViewError> {
        let bytes = bcs::to_bytes(value)?;
        self.0.push(WriteOp::Put { key, value: bytes });
        Ok(())
    }

    fn delete_key(&mut self, key: Vec<u8>) {
        self.0.push(WriteOp::Delete { key: key.to_vec() });
    }
}

impl<E> RocksdbContext<E> {
    pub fn new(db: Arc<DB>, guard: ChainGuard, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            db,
            guard: Arc::new(guard),
            base_key,
            extra,
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

pub enum WriteOp {
    Delete { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

pub struct MyBatch(Vec<WriteOp>);

impl std::ops::Deref for MyBatch {
    type Target = Vec<WriteOp>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MyBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[async_trait]
impl<E> Context for RocksdbContext<E>
where
    E: Clone + Send + Sync,
{
    type Batch = MyBatch;
    type Extra = E;
    type Error = RocksdbViewError;

    fn extra(&self) -> &E {
        &self.extra
    }

    async fn run_with_batch<F>(&self, builder: F) -> Result<(), Self::Error>
    where
        F: FnOnce(&mut Self::Batch) -> futures::future::BoxFuture<Result<(), Self::Error>>
            + Send
            + Sync,
    {
        let mut batch = MyBatch(Vec::new());
        builder(&mut batch).await?;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
            for e_ent in batch.0 {
                match e_ent {
                    WriteOp::Delete { key } => {
                        inner_batch.delete(&key);
                    }
                    WriteOp::Put { key, value } => {
                        inner_batch.put(&key, value);
                    }
                }
            }
            db.write(inner_batch)
        })
        .await??;
        Ok(())
    }
}

#[async_trait]
impl<E> ScopedOperations for RocksdbContext<E>
where
    E: Clone + Send + Sync,
{
    fn clone_with_scope(&self, index: u64) -> Self {
        Self {
            db: self.db.clone(),
            guard: self.guard.clone(),
            base_key: self.derive_key(&index),
            extra: self.extra.clone(),
        }
    }
}

#[async_trait]
impl<E, T> RegisterOperations<T> for RocksdbContext<E>
where
    T: Default + Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    async fn get(&mut self) -> Result<T, RocksdbViewError> {
        let value = self.db.read_key(&self.base_key).await?.unwrap_or_default();
        Ok(value)
    }

    async fn set(&mut self, batch: &mut Self::Batch, value: T) -> Result<(), RocksdbViewError> {
        batch.write_key(self.base_key.clone(), &value)?;
        Ok(())
    }

    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), Self::Error> {
        batch.delete_key(self.base_key.clone());
        Ok(())
    }
}

#[async_trait]
impl<E, T> LogOperations<T> for RocksdbContext<E>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
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
            batch.write_key(self.derive_key(&count), &value)?;
            count += 1;
        }
        batch.write_key(self.base_key.clone(), &count)?;
        Ok(())
    }

    async fn delete(
        &mut self,
        stored_count: usize,
        batch: &mut Self::Batch,
    ) -> Result<(), Self::Error> {
        batch.delete_key(self.base_key.clone());
        for index in 0..stored_count {
            batch.delete_key(self.derive_key(&index));
        }
        Ok(())
    }
}

#[async_trait]
impl<E, T> QueueOperations<T> for RocksdbContext<E>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
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
        batch.write_key(self.base_key.clone(), &range)?;
        for i in deletion_range {
            batch.delete_key(self.derive_key(&i));
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
            batch.write_key(self.derive_key(&range.end), &value)?;
            range.end += 1;
        }
        batch.write_key(self.base_key.clone(), &range)
    }

    async fn delete(
        &mut self,
        range: Range<usize>,
        batch: &mut Self::Batch,
    ) -> Result<(), RocksdbViewError> {
        batch.delete_key(self.base_key.clone());
        for i in range {
            batch.delete_key(self.derive_key(&i));
        }
        Ok(())
    }
}

#[async_trait]
impl<E, I, V> MapOperations<I, V> for RocksdbContext<E>
where
    I: Eq + Ord + Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
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
        batch.write_key(self.derive_key(&index), &value)?;
        Ok(())
    }

    async fn remove(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), RocksdbViewError> {
        batch.delete_key(self.derive_key(&index));
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

    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), RocksdbViewError>
    where
        F: FnMut(I) + Send,
    {
        let len = self.base_key.len();
        for key in self.db.find_keys_with_prefix(&self.base_key).await? {
            let key = bcs::from_bytes(&key[len..])?;
            f(key);
        }
        Ok(())
    }

    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), RocksdbViewError> {
        for key in self.db.find_keys_with_prefix(&self.base_key).await? {
            batch.delete_key(key);
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
impl<E, I> CollectionOperations<I> for RocksdbContext<E>
where
    I: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    fn clone_with_scope(&self, index: &I) -> Self {
        Self {
            db: self.db.clone(),
            guard: self.guard.clone(),
            base_key: self.derive_key(&CollectionKey::Subview(index)),
            extra: self.extra.clone(),
        }
    }

    async fn add_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        batch.write_key(self.derive_key(&CollectionKey::Index(index)), &())?;
        Ok(())
    }

    async fn remove_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        batch.delete_key(self.derive_key(&CollectionKey::Index(index)));
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

    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send,
    {
        // Hack: the BCS-serialization of `CollectionKey::Index(value)` for any `value` must
        // start with that of `CollectionKey::Index(())`, that is, the enum tag.
        let base = self.derive_key(&CollectionKey::Index(()));
        let len = base.len();
        for bytes in self.db.find_keys_with_prefix(&base).await? {
            match bcs::from_bytes(&bytes[len..]) {
                Ok(key) => {
                    f(key);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
}

impl<E> HashingContext for RocksdbContext<E>
where
    E: Clone + Send + Sync,
{
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
