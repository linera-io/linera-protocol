// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Adds support for large values to a given store by splitting them between several keys.

use linera_base::ensure;
use thiserror::Error;

use crate::{
    batch::{Batch, WriteOperation},
    store::{
        KeyInterval, KeyIntervalStart, KeyValueDatabase, KeyValueStoreError, ReadableKeyValueStore,
        WithError, WritableKeyValueStore,
    },
};
#[cfg(with_testing)]
use crate::{
    memory::{MemoryStore, MemoryStoreError},
    store::TestKeyValueDatabase,
};

/// A key-value database with no size limit for values.
///
/// It wraps a key-value store, potentially _with_ a size limit, and automatically
/// splits up large values into smaller ones. A single logical key-value pair is
/// stored as multiple smaller key-value pairs in the wrapped store.
/// See the `README.md` for additional details.
#[derive(Clone)]
pub struct ValueSplittingDatabase<D> {
    /// The underlying database.
    database: D,
}

/// A key-value store with no size limit for values.
#[derive(Clone)]
pub struct ValueSplittingStore<S> {
    /// The underlying store.
    store: S,
}

/// The composed error type built from the inner error type.
#[derive(Error, Debug)]
pub enum ValueSplittingError<E> {
    /// inner store error
    #[error(transparent)]
    InnerStoreError(#[from] E),

    /// The key is of length less than 4, so we cannot extract the first byte
    #[error("the key is of length less than 4, so we cannot extract the first byte")]
    TooShortKey,

    /// Value segment is missing from the database
    #[error("value segment is missing from the database")]
    MissingSegment,

    /// No count of size `u32` is available in the value
    #[error("no count of size u32 is available in the value")]
    NoCountAvailable,
}

impl<E: KeyValueStoreError> From<bcs::Error> for ValueSplittingError<E> {
    fn from(error: bcs::Error) -> Self {
        let error = E::from(error);
        ValueSplittingError::InnerStoreError(error)
    }
}

impl<E: KeyValueStoreError + 'static> KeyValueStoreError for ValueSplittingError<E> {
    const BACKEND: &'static str = "value splitting";

    fn must_reload_view(&self) -> bool {
        match self {
            ValueSplittingError::InnerStoreError(e) => e.must_reload_view(),
            _ => false,
        }
    }
}

impl<S> WithError for ValueSplittingDatabase<S>
where
    S: WithError,
    S::Error: 'static,
{
    type Error = ValueSplittingError<S::Error>;
}

impl<D> WithError for ValueSplittingStore<D>
where
    D: WithError,
    D::Error: 'static,
{
    type Error = ValueSplittingError<D::Error>;
}

impl<S> ReadableKeyValueStore for ValueSplittingStore<S>
where
    S: ReadableKeyValueStore,
    S::Error: 'static,
{
    const MAX_KEY_SIZE: usize = S::MAX_KEY_SIZE - 4;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    fn root_key(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(self.store.root_key()?)
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
        let segments = self.store.read_multi_values_bytes(&big_keys).await?;
        for segment in segments {
            match segment {
                None => {
                    return Err(ValueSplittingError::MissingSegment);
                }
                Some(segment) => {
                    big_value.extend(segment);
                }
            }
        }
        Ok(Some(big_value))
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let mut big_key = key.to_vec();
        big_key.extend(&[0, 0, 0, 0]);
        Ok(self.store.contains_key(&big_key).await?)
    }

    async fn contains_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>, Self::Error> {
        let big_keys = keys
            .iter()
            .map(|key| {
                let mut big_key = key.clone();
                big_key.extend(&[0, 0, 0, 0]);
                big_key
            })
            .collect::<Vec<_>>();
        Ok(self.store.contains_keys(&big_keys).await?)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut big_keys = Vec::new();
        for key in keys {
            let mut big_key = key.clone();
            big_key.extend(&[0, 0, 0, 0]);
            big_keys.push(big_key);
        }
        let values = self.store.read_multi_values_bytes(&big_keys).await?;
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
                .read_multi_values_bytes(&keys_add)
                .await?
                .into_iter();
            for (big_value, count) in big_values.iter_mut().zip(&n_blocks) {
                if let Some(value) = big_value {
                    for _ in 1..*count {
                        let segment = segments.next().unwrap().unwrap();
                        value.extend(segment);
                    }
                }
            }
        }
        Ok(big_values)
    }

    async fn find_keys_in_interval(
        &self,
        key_interval: KeyInterval,
    ) -> Result<(Vec<Vec<u8>>, bool), Self::Error> {
        if key_interval.is_empty() {
            return Ok((Vec::new(), true));
        }
        let big_interval = Self::get_big_key_interval(&key_interval);
        let mut keys = Vec::new();
        let mut next_start = big_interval.start;
        loop {
            let remaining_limit = key_interval.limit.map(|limit| limit - keys.len());
            if remaining_limit == Some(0) {
                return Ok((keys, false));
            }
            let (big_keys, is_big_finished) = self
                .store
                .find_keys_in_interval(KeyInterval {
                    start: next_start.clone(),
                    end: big_interval.end.clone(),
                    limit: remaining_limit,
                })
                .await?;
            let mut last_big_key = None;
            for big_key in big_keys {
                let len = big_key.len();
                last_big_key = Some(big_key.clone());
                if Self::read_index_from_key(&big_key)? != 0 {
                    continue;
                }
                let key = big_key[0..len - 4].to_vec();
                // The big-key interval is a loose superset of the user
                // interval (variable-length user keys cause prefix-extending
                // user keys to land between `K || [0;4]` and `K || [255;4]`),
                // so we re-check each reconstructed user key.
                if !key_interval.contains(&key) {
                    continue;
                }
                keys.push(key);
                if key_interval.limit.is_some_and(|limit| keys.len() >= limit) {
                    return Ok((keys, false));
                }
            }
            if is_big_finished {
                return Ok((keys, true));
            }
            let Some(last_big_key) = last_big_key else {
                return Ok((keys, false));
            };
            next_start = KeyIntervalStart::Excluded(last_big_key);
        }
    }

    async fn find_key_values_in_interval(
        &self,
        key_interval: KeyInterval,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool), Self::Error> {
        if key_interval.is_empty() {
            return Ok((Vec::new(), true));
        }
        let big_interval = Self::get_big_key_interval(&key_interval);
        let mut key_values: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut next_start = big_interval.start;
        loop {
            let remaining_limit = key_interval.limit.map(|limit| limit - key_values.len());
            if remaining_limit == Some(0) {
                return Ok((key_values, false));
            }
            let (small_kvs, is_big_finished) = self
                .store
                .find_key_values_in_interval(KeyInterval {
                    start: next_start.clone(),
                    end: big_interval.end.clone(),
                    limit: remaining_limit,
                })
                .await?;
            if small_kvs.is_empty() {
                return Ok((key_values, is_big_finished));
            }
            let mut iter = small_kvs.into_iter();
            let mut last_big_key: Option<Vec<u8>> = None;
            let mut limit_reached = false;
            while let Some((big_key, value)) = iter.next() {
                last_big_key = Some(big_key.clone());
                if Self::read_index_from_key(&big_key)? != 0 {
                    // Tail segment of an earlier user key (only possible at
                    // the start of a batch when an `Excluded(start)` lands
                    // inside a segmented value).
                    continue;
                }
                let mut user_key = big_key;
                user_key.truncate(user_key.len() - 4);
                let count = Self::read_count_from_value(&value)?;
                let mut full_value = value[4..].to_vec();
                let mut consumed_idx = 0u32;
                while consumed_idx + 1 < count {
                    let Some((seg_key, seg_value)) = iter.next() else {
                        break;
                    };
                    ensure!(
                        Self::read_index_from_key(&seg_key)? == consumed_idx + 1
                            && seg_key.starts_with(&user_key)
                            && seg_key.len() == user_key.len() + 4,
                        ValueSplittingError::MissingSegment
                    );
                    last_big_key = Some(seg_key);
                    full_value.extend(seg_value);
                    consumed_idx += 1;
                }
                if consumed_idx + 1 < count {
                    // The current batch ran out before we could read all
                    // segments of this value. Fall back to point reads for
                    // the remaining ones rather than refetching the whole
                    // tail by scan (which would risk the same truncation).
                    let missing_keys: Vec<Vec<u8>> = (consumed_idx + 1..count)
                        .map(|i| Self::get_segment_key(&user_key, i))
                        .collect::<Result<_, _>>()?;
                    let missing_values =
                        self.store.read_multi_values_bytes(&missing_keys).await?;
                    for (i, value_opt) in missing_values.into_iter().enumerate() {
                        let value = value_opt.ok_or(ValueSplittingError::MissingSegment)?;
                        full_value.extend(value);
                        last_big_key = Some(missing_keys[i].clone());
                    }
                }
                if !key_interval.contains(&user_key) {
                    continue;
                }
                key_values.push((user_key, full_value));
                if key_interval
                    .limit
                    .is_some_and(|limit| key_values.len() >= limit)
                {
                    limit_reached = true;
                    break;
                }
            }
            if limit_reached {
                return Ok((key_values, false));
            }
            if is_big_finished {
                return Ok((key_values, true));
            }
            let Some(last) = last_big_key else {
                return Ok((key_values, false));
            };
            next_start = KeyIntervalStart::Excluded(last);
        }
    }
}

impl<K> WritableKeyValueStore for ValueSplittingStore<K>
where
    K: WritableKeyValueStore,
    K::Error: 'static,
{
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
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
        Ok(self.store.write_batch(batch_new).await?)
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        Ok(self.store.clear_journal().await?)
    }
}

impl<S> ValueSplittingStore<S>
where
    S: ReadableKeyValueStore,
{
    fn get_big_key_interval(key_interval: &KeyInterval) -> KeyInterval {
        let start = match &key_interval.start {
            KeyIntervalStart::Included(key) => {
                KeyIntervalStart::Included([key.as_slice(), &[0, 0, 0, 0]].concat())
            }
            KeyIntervalStart::Excluded(key) => {
                KeyIntervalStart::Excluded([key.as_slice(), &[0, 0, 0, 0]].concat())
            }
        };
        let end = match &key_interval.end {
            std::ops::Bound::Included(key) => {
                std::ops::Bound::Included([key.as_slice(), &[u8::MAX; 4]].concat())
            }
            std::ops::Bound::Excluded(key) => {
                std::ops::Bound::Excluded([key.as_slice(), &[0, 0, 0, 0]].concat())
            }
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };
        KeyInterval {
            start,
            end,
            limit: key_interval.limit,
        }
    }
}

impl<D> KeyValueDatabase for ValueSplittingDatabase<D>
where
    D: KeyValueDatabase,
    D::Error: 'static,
{
    type Config = D::Config;

    type Store = ValueSplittingStore<D::Store>;

    fn get_name() -> String {
        format!("value splitting {}", D::get_name())
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, Self::Error> {
        let database = D::connect(config, namespace).await?;
        Ok(Self { database })
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        let store = self.database.open_shared(root_key)?;
        Ok(ValueSplittingStore { store })
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        let store = self.database.open_exclusive(root_key)?;
        Ok(ValueSplittingStore { store })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        Ok(D::list_all(config).await?)
    }

    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        Ok(self.database.list_root_keys().await?)
    }

    async fn delete_all(config: &Self::Config) -> Result<(), Self::Error> {
        Ok(D::delete_all(config).await?)
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        Ok(D::exists(config, namespace).await?)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        Ok(D::create(config, namespace).await?)
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        Ok(D::delete(config, namespace).await?)
    }
}

#[cfg(with_testing)]
impl<D> TestKeyValueDatabase for ValueSplittingDatabase<D>
where
    D: TestKeyValueDatabase,
    D::Error: 'static,
{
    async fn new_test_config() -> Result<D::Config, Self::Error> {
        Ok(D::new_test_config().await?)
    }
}

impl<D> ValueSplittingStore<D>
where
    D: WithError,
{
    /// Creates a new store that deals with big values from one that does not.
    pub fn new(store: D) -> Self {
        ValueSplittingStore { store }
    }

    fn get_segment_key(key: &[u8], index: u32) -> Result<Vec<u8>, ValueSplittingError<D::Error>> {
        let mut big_key_segment = key.to_vec();
        let mut bytes = bcs::to_bytes(&index)?;
        bytes.reverse();
        big_key_segment.extend(bytes);
        Ok(big_key_segment)
    }

    fn get_initial_count_first_chunk(
        count: u32,
        first_chunk: &[u8],
    ) -> Result<Vec<u8>, ValueSplittingError<D::Error>> {
        let mut bytes = bcs::to_bytes(&count)?;
        bytes.reverse();
        let mut value_ext = Vec::new();
        value_ext.extend(bytes);
        value_ext.extend(first_chunk);
        Ok(value_ext)
    }

    fn read_count_from_value(value: &[u8]) -> Result<u32, ValueSplittingError<D::Error>> {
        if value.len() < 4 {
            return Err(ValueSplittingError::NoCountAvailable);
        }
        let mut bytes = value[0..4].to_vec();
        bytes.reverse();
        Ok(bcs::from_bytes::<u32>(&bytes)?)
    }

    fn read_index_from_key(key: &[u8]) -> Result<u32, ValueSplittingError<D::Error>> {
        let len = key.len();
        if len < 4 {
            return Err(ValueSplittingError::TooShortKey);
        }
        let mut bytes = key[len - 4..len].to_vec();
        bytes.reverse();
        Ok(bcs::from_bytes::<u32>(&bytes)?)
    }
}

/// A memory store for which the values are limited to 100 bytes and can be used for tests.
#[derive(Clone)]
#[cfg(with_testing)]
pub struct LimitedTestMemoryStore {
    inner: MemoryStore,
}

#[cfg(with_testing)]
impl Default for LimitedTestMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(with_testing)]
impl WithError for LimitedTestMemoryStore {
    type Error = MemoryStoreError;
}

#[cfg(with_testing)]
impl ReadableKeyValueStore for LimitedTestMemoryStore {
    const MAX_KEY_SIZE: usize = usize::MAX;

    fn max_stream_queries(&self) -> usize {
        self.inner.max_stream_queries()
    }

    fn root_key(&self) -> Result<Vec<u8>, MemoryStoreError> {
        self.inner.root_key()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryStoreError> {
        self.inner.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, MemoryStoreError> {
        self.inner.contains_key(key).await
    }

    async fn contains_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>, MemoryStoreError> {
        self.inner.contains_keys(keys).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, MemoryStoreError> {
        self.inner.read_multi_values_bytes(keys).await
    }

    async fn find_keys_in_interval(
        &self,
        key_interval: KeyInterval,
    ) -> Result<(Vec<Vec<u8>>, bool), MemoryStoreError> {
        self.inner.find_keys_in_interval(key_interval).await
    }

    async fn find_key_values_in_interval(
        &self,
        key_interval: KeyInterval,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool), MemoryStoreError> {
        self.inner.find_key_values_in_interval(key_interval).await
    }
}

#[cfg(with_testing)]
impl WritableKeyValueStore for LimitedTestMemoryStore {
    // We set up the MAX_VALUE_SIZE to the artificially low value of 100
    // purely for testing purposes.
    const MAX_VALUE_SIZE: usize = 100;

    async fn write_batch(&self, batch: Batch) -> Result<(), MemoryStoreError> {
        assert!(
            batch.check_value_size(Self::MAX_VALUE_SIZE),
            "The batch size is not adequate for this test"
        );
        self.inner.write_batch(batch).await
    }

    async fn clear_journal(&self) -> Result<(), MemoryStoreError> {
        self.inner.clear_journal().await
    }
}

#[cfg(with_testing)]
impl LimitedTestMemoryStore {
    /// Creates a `LimitedTestMemoryStore`
    pub fn new() -> Self {
        let inner = MemoryStore::new_for_testing();
        LimitedTestMemoryStore { inner }
    }
}

/// Provides a `LimitedTestMemoryStore<()>` that can be used for tests.
#[cfg(with_testing)]
pub fn create_value_splitting_memory_store() -> ValueSplittingStore<LimitedTestMemoryStore> {
    ValueSplittingStore::new(LimitedTestMemoryStore::new())
}

#[cfg(test)]
mod tests {
    use linera_views::{
        batch::Batch,
        store::{ReadableKeyValueStore, WritableKeyValueStore},
        value_splitting::{LimitedTestMemoryStore, ValueSplittingStore},
    };
    use rand::Rng;

    // The key splitting means that when a key is overwritten
    // some previous segments may still be present.
    #[tokio::test]
    async fn test_value_splitting1_testing_leftovers() {
        let store = LimitedTestMemoryStore::new();
        const MAX_LEN: usize = LimitedTestMemoryStore::MAX_VALUE_SIZE;
        const _: () = assert!(MAX_LEN > 10);
        let big_store = ValueSplittingStore::new(store.clone());
        let key = vec![0, 0];
        // Write a key with a long value
        let mut batch = Batch::new();
        let value = Vec::from([0; MAX_LEN + 1]);
        batch.put_key_value_bytes(key.clone(), value.clone());
        big_store.write_batch(batch).await.unwrap();
        let value_read = big_store.read_value_bytes(&key).await.unwrap();
        assert_eq!(value_read, Some(value));
        // Write a key with a smaller value
        let mut batch = Batch::new();
        let value = Vec::from([0, 1]);
        batch.put_key_value_bytes(key.clone(), value.clone());
        big_store.write_batch(batch).await.unwrap();
        let value_read = big_store.read_value_bytes(&key).await.unwrap();
        assert_eq!(value_read, Some(value));
        // Two segments are present even though only one is used
        let keys = store.find_keys_by_prefix(&[0]).await.unwrap();
        assert_eq!(keys, vec![vec![0, 0, 0, 0, 0], vec![0, 0, 0, 0, 1]]);
    }

    #[tokio::test]
    async fn test_value_splitting2_testing_splitting() {
        let store = LimitedTestMemoryStore::new();
        const MAX_LEN: usize = LimitedTestMemoryStore::MAX_VALUE_SIZE;
        let big_store = ValueSplittingStore::new(store.clone());
        let key = vec![0, 0];
        // Writing a big value
        let mut batch = Batch::new();
        let mut value = Vec::new();
        let mut rng = crate::random::make_deterministic_rng();
        for _ in 0..2 * MAX_LEN - 4 {
            value.push(rng.gen::<u8>());
        }
        batch.put_key_value_bytes(key.clone(), value.clone());
        big_store.write_batch(batch).await.unwrap();
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
                unreachable!(
                    "value_splitting test: segment key not found in underlying store right after a multi-segment write"
                )
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
        let store = LimitedTestMemoryStore::new();
        const MAX_LEN: usize = LimitedTestMemoryStore::MAX_VALUE_SIZE;
        let big_store = ValueSplittingStore::new(store.clone());
        let key = vec![0, 0];
        // writing a big key
        let mut batch = Batch::new();
        let mut value = Vec::new();
        let mut rng = crate::random::make_deterministic_rng();
        for _ in 0..3 * MAX_LEN - 4 {
            value.push(rng.gen::<u8>());
        }
        batch.put_key_value_bytes(key.clone(), value.clone());
        big_store.write_batch(batch).await.unwrap();
        // deleting it
        let mut batch = Batch::new();
        batch.delete_key(key.clone());
        big_store.write_batch(batch).await.unwrap();
        // reading everything (there are leftover keys)
        let key_values = big_store.find_key_values_by_prefix(&[0]).await.unwrap();
        assert_eq!(key_values.len(), 0);
        // Two segments remain
        let keys = store.find_keys_by_prefix(&[0]).await.unwrap();
        assert_eq!(keys, vec![vec![0, 0, 0, 0, 1], vec![0, 0, 0, 0, 2]]);
    }
}
