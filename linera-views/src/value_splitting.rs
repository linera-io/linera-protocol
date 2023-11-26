// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{ContextFromStore, KeyIterable, KeyValueIterable, KeyValueStore},
    store::memory::{MemoryContextError, MemoryStore, MemoryStoreMap, TEST_MEMORY_MAX_STREAM_QUERIES},
};
use async_lock::{Mutex, MutexGuardArc};
use async_trait::async_trait;
use linera_base::ensure;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use thiserror::Error;

/// Data type indicating that the database is not consistent
#[derive(Error, Debug)]
pub enum DatabaseConsistencyError {
    /// The key is of length less than 4, so we cannot extract the first byte
    #[error("the key is of length less than 4, so we cannot extract the first byte")]
    TooShortKey,

    /// value segment is missing from the database
    #[error("value segment is missing from the database")]
    MissingSegment,

    /// no count of size u32 is available in the value
    #[error("no count of size u32 is available in the value")]
    NoCountAvailable,
}

/// A key-value store with no size limit for values.
///
/// It wraps a key-value store, potentially _with_ a size limit, and automatically
/// splits up large values into smaller ones. A single logical key-value pair is
/// stored as multiple smaller key-value pairs in the wrapped store.
/// See the README.md for additional details.
#[derive(Clone)]
pub struct ValueSplittingStore<K> {
    /// The underlying store of the transformed store.
    pub store: K,
}

#[async_trait]
impl<K> KeyValueStore for ValueSplittingStore<K>
where
    K: KeyValueStore + Send + Sync,
    K::Error: From<bcs::Error> + From<DatabaseConsistencyError>,
{
    const MAX_VALUE_SIZE: usize = usize::MAX;
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE - 4;
    type Error = K::Error;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let mut big_key = key.to_vec();
        big_key.extend(&[0, 0, 0, 0]);
        let value = self.store.read_value_bytes(&big_key).await?;
        let Some(value) = value else {
            return Ok(None);
        };
        let count = Self::read_count_from_value(&value)?;
        let mut big_value = value[4..].to_vec();
        if count == 1 {
            return Ok(Some(big_value));
        }
        let mut big_keys = Vec::new();
        for i in 1..count {
            let big_key_segment = Self::get_segment_key(key, i)?;
            big_keys.push(big_key_segment);
        }
        let segments = self.store.read_multi_values_bytes(big_keys).await?;
        for segment in segments {
            match segment {
                None => {
                    return Err(DatabaseConsistencyError::MissingSegment.into());
                }
                Some(segment) => {
                    big_value.extend(segment);
                }
            }
        }
        Ok(Some(big_value))
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut big_keys = Vec::new();
        for key in &keys {
            let mut big_key = key.clone();
            big_key.extend(&[0, 0, 0, 0]);
            big_keys.push(big_key);
        }
        let values = self.store.read_multi_values_bytes(big_keys).await?;
        let mut big_values = Vec::<Option<Vec<u8>>>::new();
        let mut keys_add = Vec::new();
        let mut n_blocks = Vec::new();
        for (key, value) in keys.iter().zip(values) {
            match value {
                None => {
                    n_blocks.push(0);
                    big_values.push(None);
                }
                Some(value) => {
                    let count = Self::read_count_from_value(&value)?;
                    let big_value = value[4..].to_vec();
                    for i in 1..count {
                        let big_key_segment = Self::get_segment_key(key, i)?;
                        keys_add.push(big_key_segment);
                    }
                    n_blocks.push(count);
                    big_values.push(Some(big_value));
                }
            }
        }
        if !keys_add.is_empty() {
            let mut segments = self
                .store
                .read_multi_values_bytes(keys_add)
                .await?
                .into_iter();
            for (idx, count) in n_blocks.iter().enumerate() {
                if count > &1 {
                    let value = big_values.get_mut(idx).unwrap();
                    if let Some(ref mut value) = value {
                        for _ in 1..*count {
                            let segment = segments.next().unwrap().unwrap();
                            value.extend(segment);
                        }
                    }
                }
            }
        }
        Ok(big_values)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        let mut keys = Vec::new();
        for big_key in self.store.find_keys_by_prefix(key_prefix).await?.iterator() {
            let big_key = big_key?;
            let len = big_key.len();
            if Self::read_index_from_key(big_key)? == 0 {
                let key = big_key[0..len - 4].to_vec();
                keys.push(key);
            }
        }
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let small_key_values = self.store.find_key_values_by_prefix(key_prefix).await?;
        let mut small_kv_iterator = small_key_values.into_iterator_owned();
        let mut key_values = Vec::new();
        while let Some(result) = small_kv_iterator.next() {
            let (mut big_key, value) = result?;
            if Self::read_index_from_key(&big_key)? != 0 {
                continue; // Leftover segment from an earlier value.
            }
            big_key.truncate(big_key.len() - 4);
            let key = big_key;
            let count = Self::read_count_from_value(&value)?;
            let mut big_value = value[4..].to_vec();
            for idx in 1..count {
                let (big_key, value) = small_kv_iterator
                    .next()
                    .ok_or(DatabaseConsistencyError::MissingSegment)??;
                ensure!(
                    Self::read_index_from_key(&big_key)? == idx
                        && big_key.starts_with(&key)
                        && big_key.len() == key.len() + 4,
                    DatabaseConsistencyError::MissingSegment
                );
                big_value.extend(value);
            }
            key_values.push((key, big_value));
        }
        Ok(key_values)
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Self::Error> {
        let mut batch_new = Batch::new();
        for operation in batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    let mut big_key = key.to_vec();
                    big_key.extend(&[0, 0, 0, 0]);
                    batch_new.delete_key(big_key);
                }
                WriteOperation::Put { key, mut value } => {
                    let big_key = Self::get_segment_key(&key, 0)?;
                    let mut count: u32 = 1;
                    let value_ext = if value.len() <= K::MAX_VALUE_SIZE - 4 {
                        Self::get_initial_count_first_chunk(count, &value)?
                    } else {
                        let remainder = value.split_off(K::MAX_VALUE_SIZE - 4);
                        for value_chunk in remainder.chunks(K::MAX_VALUE_SIZE) {
                            let big_key_segment = Self::get_segment_key(&key, count)?;
                            batch_new.put_key_value_bytes(big_key_segment, value_chunk.to_vec());
                            count += 1;
                        }
                        Self::get_initial_count_first_chunk(count, &value)?
                    };
                    batch_new.put_key_value_bytes(big_key, value_ext);
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    batch_new.delete_key_prefix(key_prefix);
                }
            }
        }
        self.store.write_batch(batch_new, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.store.clear_journal(base_key).await
    }
}

impl<K> ValueSplittingStore<K>
where
    K: KeyValueStore + Send + Sync,
    K::Error: From<bcs::Error> + From<DatabaseConsistencyError>,
{
    /// Creates a new store that deals with big values from one that does not.
    pub fn new(store: K) -> Self {
        ValueSplittingStore { store }
    }

    fn read_count_from_value(value: &[u8]) -> Result<u32, K::Error> {
        if value.len() < 4 {
            return Err(DatabaseConsistencyError::NoCountAvailable.into());
        }
        let mut bytes = value[0..4].to_vec();
        bytes.reverse();
        Ok(bcs::from_bytes::<u32>(&bytes)?)
    }

    fn get_segment_key(key: &[u8], index: u32) -> Result<Vec<u8>, K::Error> {
        let mut big_key_segment = key.to_vec();
        let mut bytes = bcs::to_bytes(&index)?;
        bytes.reverse();
        big_key_segment.extend(bytes);
        Ok(big_key_segment)
    }

    fn read_index_from_key(key: &[u8]) -> Result<u32, K::Error> {
        let len = key.len();
        if len < 4 {
            return Err(DatabaseConsistencyError::TooShortKey.into());
        }
        let mut bytes = key[len - 4..len].to_vec();
        bytes.reverse();
        Ok(bcs::from_bytes::<u32>(&bytes)?)
    }

    fn get_initial_count_first_chunk(count: u32, first_chunk: &[u8]) -> Result<Vec<u8>, K::Error> {
        let mut bytes = bcs::to_bytes(&count)?;
        bytes.reverse();
        let mut value_ext = Vec::new();
        value_ext.extend(bytes);
        value_ext.extend(first_chunk);
        Ok(value_ext)
    }
}

/// A virtual DB store where data are persisted in memory.
#[derive(Clone)]
pub struct TestMemoryStoreInternal {
    store: MemoryStore,
}

#[async_trait]
impl KeyValueStore for TestMemoryStoreInternal {
    // We set up the MAX_VALUE_SIZE to the artificially low value of 100
    // purely for testing purposes.
    const MAX_VALUE_SIZE: usize = 100;
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Error = MemoryContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        TEST_MEMORY_MAX_STREAM_QUERIES
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryContextError> {
        self.store.read_value_bytes(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, MemoryContextError> {
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, MemoryContextError> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, MemoryContextError> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), MemoryContextError> {
        ensure!(
            batch.check_value_size(Self::MAX_VALUE_SIZE),
            MemoryContextError::TooLargeValue
        );
        self.store.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.store.clear_journal(base_key).await
    }
}

impl TestMemoryStoreInternal {
    /// Creates a `TestMemoryStore` from the guard
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>) -> Self {
        let store = MemoryStore::new(guard, TEST_MEMORY_MAX_STREAM_QUERIES);
        TestMemoryStoreInternal { store }
    }
}

/// Supposed to be removed later
#[derive(Clone)]
pub struct TestMemoryStore {
    store: ValueSplittingStore<TestMemoryStoreInternal>,
}

#[async_trait]
impl KeyValueStore for TestMemoryStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Error = MemoryContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryContextError> {
        self.store.read_value_bytes(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, MemoryContextError> {
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, MemoryContextError> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, MemoryContextError> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), MemoryContextError> {
        self.store.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.store.clear_journal(base_key).await
    }
}

impl TestMemoryStore {
    /// Creates a `TestMemoryStore` from the guard
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>) -> Self {
        let store = TestMemoryStoreInternal::new(guard);
        let store = ValueSplittingStore::new(store);
        TestMemoryStore { store }
    }
}

/// An implementation of [`crate::common::Context`] that stores all values in memory.
pub type TestMemoryContext<E> = ContextFromStore<E, TestMemoryStore>;

impl<E> TestMemoryContext<E> {
    /// Creates a [`TestMemoryContext`].
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>, extra: E) -> Self {
        let store = TestMemoryStore::new(guard);
        let base_key = Vec::new();
        Self {
            store,
            base_key,
            extra,
        }
    }
}

/// Provides a `TestMemoryContext<()>` that can be used for tests.
/// It is not named create_memory_test_context because it is massively
/// used and so we want to have a short name.
pub fn create_test_memory_context() -> TestMemoryContext<()> {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    TestMemoryContext::new(guard, ())
}

/// Creates a `TestMemoryStore` for working.
pub fn create_test_memory_store() -> TestMemoryStore {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    TestMemoryStore::new(guard)
}

/// An implementation of [`crate::common::Context`] that stores all values in memory.
pub type TestMemoryContextInternal<E> = ContextFromStore<E, TestMemoryStoreInternal>;

impl<E> TestMemoryContextInternal<E> {
    /// Creates a [`TestMemoryContextInternal`].
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>, extra: E) -> Self {
        let store = TestMemoryStoreInternal::new(guard);
        let base_key = Vec::new();
        Self {
            store,
            base_key,
            extra,
        }
    }
}

/// Provides a `TestMemoryStoreInternal<()>` that can be used for tests.
pub fn create_test_memory_store_internal() -> TestMemoryStoreInternal {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    TestMemoryStoreInternal::new(guard)
}

/// Provides a `TestMemoryContextInternal<()>` that can be used for tests.
pub fn create_test_memory_context_internal() -> TestMemoryContextInternal<()> {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    TestMemoryContextInternal::new(guard, ())
}

#[cfg(test)]
mod tests {
    use linera_views::{
        batch::Batch,
        common::KeyValueStore,
        value_splitting::{
            create_test_memory_store_internal, TestMemoryStoreInternal, ValueSplittingStore,
        },
    };
    use rand::{Rng, SeedableRng};

    // The key splitting means that when a key is overwritten
    // some previous segments may still be present.
    #[tokio::test]
    #[allow(clippy::assertions_on_constants)]
    async fn test_value_splitting1_testing_leftovers() {
        let store = create_test_memory_store_internal();
        const MAX_LEN: usize = TestMemoryStoreInternal::MAX_VALUE_SIZE;
        assert!(MAX_LEN > 10);
        let big_store = ValueSplittingStore::new(store.clone());
        let key = vec![0, 0];
        // Write a key with a long value
        let mut batch = Batch::new();
        let value = Vec::from([0; MAX_LEN + 1]);
        batch.put_key_value_bytes(key.clone(), value.clone());
        big_store.write_batch(batch, &[]).await.unwrap();
        let value_read = big_store.read_value_bytes(&key).await.unwrap();
        assert_eq!(value_read, Some(value));
        // Write a key with a smaller value
        let mut batch = Batch::new();
        let value = Vec::from([0, 1]);
        batch.put_key_value_bytes(key.clone(), value.clone());
        big_store.write_batch(batch, &[]).await.unwrap();
        let value_read = big_store.read_value_bytes(&key).await.unwrap();
        assert_eq!(value_read, Some(value));
        // Two segments are present even though only one is used
        let keys = store.find_keys_by_prefix(&[0]).await.unwrap();
        assert_eq!(keys, vec![vec![0, 0, 0, 0, 0], vec![0, 0, 0, 0, 1]]);
    }

    #[tokio::test]
    async fn test_value_splitting2_testing_splitting() {
        let store = create_test_memory_store_internal();
        const MAX_LEN: usize = TestMemoryStoreInternal::MAX_VALUE_SIZE;
        let big_store = ValueSplittingStore::new(store.clone());
        let key = vec![0, 0];
        // Writing a big value
        let mut batch = Batch::new();
        let mut value = Vec::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(2);
        for _ in 0..2 * MAX_LEN - 4 {
            value.push(rng.gen::<u8>());
        }
        batch.put_key_value_bytes(key.clone(), value.clone());
        big_store.write_batch(batch, &[]).await.unwrap();
        let value_read = big_store.read_value_bytes(&key).await.unwrap();
        assert_eq!(value_read, Some(value.clone()));
        // Reading the segments and checking
        let mut value_concat = Vec::<u8>::new();
        for index in 0..2 {
            let mut segment_key = key.clone();
            let mut bytes = bcs::to_bytes(&index).unwrap();
            bytes.reverse();
            segment_key.extend(bytes);
            let value_read = store.read_value_bytes(&segment_key).await.unwrap();
            let Some(value_read) = value_read else {
                unreachable!()
            };
            if index == 0 {
                value_concat.extend(&value_read[4..]);
            } else {
                value_concat.extend(&value_read);
            }
        }
        assert_eq!(value, value_concat);
    }

    #[tokio::test]
    async fn test_value_splitting3_write_and_delete() {
        let store = create_test_memory_store_internal();
        const MAX_LEN: usize = TestMemoryStoreInternal::MAX_VALUE_SIZE;
        let big_store = ValueSplittingStore::new(store.clone());
        let key = vec![0, 0];
        // writing a big key
        let mut batch = Batch::new();
        let mut value = Vec::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(2);
        for _ in 0..3 * MAX_LEN - 4 {
            value.push(rng.gen::<u8>());
        }
        batch.put_key_value_bytes(key.clone(), value.clone());
        big_store.write_batch(batch, &[]).await.unwrap();
        // deleting it
        let mut batch = Batch::new();
        batch.delete_key(key.clone());
        big_store.write_batch(batch, &[]).await.unwrap();
        // reading everything (there are leftover keys)
        let key_values = big_store.find_key_values_by_prefix(&[0]).await.unwrap();
        assert_eq!(key_values.len(), 0);
        // Two segments remain
        let keys = store.find_keys_by_prefix(&[0]).await.unwrap();
        assert_eq!(keys, vec![vec![0, 0, 0, 0, 1], vec![0, 0, 0, 0, 2]]);
    }
}
