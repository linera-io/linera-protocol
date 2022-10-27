// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    hash::HashingContext,
    views::{
        CollectionOperations, Context, MapOperations, QueueOperations,
        ViewError,
    },
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{ops::Range, sync::Arc};
use thiserror::Error;

pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// An implementation of [`crate::views::Context`] based on Rocksdb
#[derive(Debug, Clone)]
pub struct RocksdbContext<E> {
    db: Arc<DB>,
    base_key: Vec<u8>,
    extra: E,
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueOperations {
    async fn read_key<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<V>, RocksdbContextError>;

    async fn write_key<V: Serialize + Sync>(
        &self,
        key: &[u8],
        value: &V,
    ) -> Result<(), RocksdbContextError>;

    async fn delete_key(&self, key: &[u8]) -> Result<(), RocksdbContextError>;

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksdbContextError>;

    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &Vec<u8>,
    ) -> Result<Vec<Key>, RocksdbContextError>;

    async fn count_keys(&self) -> Result<usize, RocksdbContextError>;
}

#[async_trait]
impl KeyValueOperations for Arc<DB> {
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

    async fn write_key<V: Serialize + Sync>(
        &self,
        key: &[u8],
        value: &V,
    ) -> Result<(), RocksdbContextError> {
        let db = self.clone();
        let key = key.to_vec();
        let bytes = bcs::to_bytes(value)?;
        tokio::task::spawn_blocking(move || db.put(&key, bytes)).await??;
        Ok(())
    }

    async fn delete_key(&self, key: &[u8]) -> Result<(), RocksdbContextError> {
        let db = self.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || db.delete(&key)).await??;
        Ok(())
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
        key_prefix: &Vec<u8>,
    ) -> Result<Vec<Key>, RocksdbContextError> {
        let len = key_prefix.len();
        let mut keys = Vec::new();
        for key in self.find_keys_with_prefix(key_prefix).await? {
            keys.push(bcs::from_bytes(&key[len..])?);
        }
        Ok(keys)
    }

    async fn count_keys(&self) -> Result<usize, RocksdbContextError> {
        let db = self.clone();
        let size =
            tokio::task::spawn_blocking(move || db.iterator(rocksdb::IteratorMode::Start).count())
                .await?;
        Ok(size)
    }
}

impl<E> RocksdbContext<E> {
    pub fn new(db: Arc<DB>, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            db,
            base_key,
            extra,
        }
    }
}

pub enum WriteOperation {
    Delete { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

pub struct Batch(Vec<WriteOperation>);

#[async_trait]
impl<E> Context for RocksdbContext<E>
where
    E: Clone + Send + Sync,
{
    type Batch = Batch;
    type Extra = E;
    type Error = RocksdbContextError;

    fn extra(&self) -> &E {
        &self.extra
    }

    fn get_base_key(&self) -> Vec<u8> {
        self.base_key.clone()
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

    fn put_item_batch(
        &self,
        batch: &mut Batch,
        key: Vec<u8>,
        value: &impl Serialize,
    ) -> Result<(), RocksdbContextError> {
        let bytes = bcs::to_bytes(value)?;
        batch.0.push(WriteOperation::Put { key, value: bytes });
        Ok(())
    }

    fn remove_item_batch(&self, batch: &mut Batch, key: Vec<u8>) {
        batch.0.push(WriteOperation::Delete { key });
    }

    async fn read_key<V: DeserializeOwned>(
        &mut self,
        key: &Vec<u8>,
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
        key_prefix: &Vec<u8>,
    ) -> Result<Vec<Key>, RocksdbContextError> {
        self.db.get_sub_keys(key_prefix).await
    }

    async fn run_with_batch<F>(&self, builder: F) -> Result<(), ViewError>
    where
        F: FnOnce(&mut Self::Batch) -> futures::future::BoxFuture<Result<(), ViewError>>
            + Send
            + Sync,
    {
        let mut batch = Batch(Vec::new());
        builder(&mut batch).await?;
        self.write_batch(batch).await
    }

    fn create_batch(&self) -> Self::Batch {
        Batch(Vec::new())
    }

    async fn write_batch(&self, batch: Self::Batch) -> Result<(), ViewError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || -> Result<(), RocksdbContextError> {
            let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
            for e_ent in batch.0 {
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

    fn clone_self(&self, base_key: Vec<u8>) -> Self {
        Self {
            db: self.db.clone(),
            base_key,
            extra: self.extra.clone(),
        }
    }
}

#[async_trait]
impl<E, T> QueueOperations<T> for RocksdbContext<E>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error> {
        let range = self
            .read_key(&self.base_key.clone())
            .await?
            .unwrap_or_default();
        Ok(range)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        Ok(self.read_key(&self.derive_key(&index)).await?)
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::new();
        for index in range {
            match self.read_key(&self.derive_key(&index)).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    fn delete_front(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Self::Batch,
        count: usize,
    ) -> Result<(), Self::Error> {
        if count == 0 {
            return Ok(());
        }
        let deletion_range = stored_indices.clone().take(count);
        stored_indices.start += count;
        self.put_item_batch(batch, self.base_key.clone(), &stored_indices)?;
        for index in deletion_range {
            self.remove_item_batch(batch, self.derive_key(&index));
        }
        Ok(())
    }

    fn append_back(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        if values.is_empty() {
            return Ok(());
        }
        for value in values {
            self.put_item_batch(batch, self.derive_key(&stored_indices.end), &value)?;
            stored_indices.end += 1;
        }
        self.put_item_batch(batch, self.base_key.clone(), &stored_indices)?;
        Ok(())
    }

    fn delete(
        &mut self,
        stored_indices: Range<usize>,
        batch: &mut Self::Batch,
    ) -> Result<(), Self::Error> {
        self.remove_item_batch(batch, self.base_key.clone());
        for index in stored_indices {
            self.remove_item_batch(batch, self.derive_key(&index));
        }
        Ok(())
    }
}

#[async_trait]
impl<E, I, V> MapOperations<I, V> for RocksdbContext<E>
where
    I: Eq + Ord + Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync,
{
    async fn get(&mut self, index: &I) -> Result<Option<V>, Self::Error> {
        Ok(self.read_key(&self.derive_key(index)).await?)
    }

    fn insert(&mut self, batch: &mut Self::Batch, index: I, value: V) -> Result<(), Self::Error> {
        self.put_item_batch(batch, self.derive_key(&index), &value)?;
        Ok(())
    }

    fn remove(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        self.remove_item_batch(batch, self.derive_key(&index));
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        self.get_sub_keys(&self.base_key.clone()).await
    }

    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send,
    {
        for index in self.get_sub_keys(&self.base_key.clone()).await? {
            f(index);
        }
        Ok(())
    }

    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), Self::Error> {
        for key in self.find_keys_with_prefix(&self.base_key.clone()).await? {
            self.remove_item_batch(batch, key);
        }
        Ok(())
    }
}

#[derive(Serialize)]
enum CollectionKey<I> {
    Index(I),
    Subview(I),
}

#[async_trait]
impl<E, I> CollectionOperations<I> for RocksdbContext<E>
where
    I: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    fn clone_with_scope(&self, index: &I) -> Self {
        self.clone_self(self.derive_key(&CollectionKey::Subview(index)))
    }

    fn add_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        self.put_item_batch(batch, self.derive_key(&CollectionKey::Index(index)), &())?;
        Ok(())
    }

    fn remove_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        self.remove_item_batch(batch, self.derive_key(&CollectionKey::Index(index)));
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        let base = self.derive_key(&CollectionKey::Index(()));
        self.get_sub_keys(&base).await
    }

    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send,
    {
        let base = self.derive_key(&CollectionKey::Index(()));
        for index in self.get_sub_keys(&base).await? {
            f(index);
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
