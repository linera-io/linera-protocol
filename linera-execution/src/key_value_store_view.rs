// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! We implement a `KeyValueStoreView` implements View and the function of `KeyValueStore`.

use std::{collections::BTreeMap, fmt::Debug};

use linera_base::ensure;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_views::{
    batch::{Batch, WriteOperation},
    common::{get_interval, HasherOutput},
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    register_view::RegisterView,
    sha3,
    store::ReadableKeyValueStore,
    views::{ClonableView, HashableView, Hasher, View},
    ViewError,
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The latency of hash computation
    pub static KEY_VALUE_STORE_VIEW_HASH_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "key_value_store_view_hash_latency",
            "KeyValueStoreView hash latency",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });

    /// The latency of get operation
    pub static KEY_VALUE_STORE_VIEW_GET_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "key_value_store_view_get_latency",
            "KeyValueStoreView get latency",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });

    /// The latency of multi get
    pub static KEY_VALUE_STORE_VIEW_MULTI_GET_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_multi_get_latency",
                "KeyValueStoreView multi get latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of contains key
    pub static KEY_VALUE_STORE_VIEW_CONTAINS_KEY_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_contains_key_latency",
                "KeyValueStoreView contains key latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of contains keys
    pub static KEY_VALUE_STORE_VIEW_CONTAINS_KEYS_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_contains_keys_latency",
                "KeyValueStoreView contains keys latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of find keys by prefix operation
    pub static KEY_VALUE_STORE_VIEW_FIND_KEYS_BY_PREFIX_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_find_keys_by_prefix_latency",
                "KeyValueStoreView find keys by prefix latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of find key values by prefix operation
    pub static KEY_VALUE_STORE_VIEW_FIND_KEY_VALUES_BY_PREFIX_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_find_key_values_by_prefix_latency",
                "KeyValueStoreView find key values by prefix latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of write batch operation
    pub static KEY_VALUE_STORE_VIEW_WRITE_BATCH_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_write_batch_latency",
                "KeyValueStoreView write batch latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });
}

/// A view that represents the functions of `KeyValueStore`.
///
/// Comment on the data set:
/// In order to work, the view needs to store the updates and deleted prefixes.
/// The updates and deleted prefixes have to be coherent. This means:
/// * If an index is deleted by one in deleted prefixes then it should not be present
///   in the updates at all.
/// * [`DeletePrefix::key_prefix`][entry1] should not dominate anyone. That is if we have `[0,2]`
///   then we should not have `[0,2,3]` since it would be dominated by the preceding.
///
/// With that we have:
/// * in order to test if an `index` is deleted by a prefix we compute the highest deleted prefix `dp`
///   such that `dp <= index`.
///   If `dp` is indeed a prefix then we conclude that `index` is deleted, otherwise not.
///   The no domination is essential here.
///
/// [entry1]: crate::batch::WriteOperation::DeletePrefix
#[derive(Debug, View, ClonableView)]
pub struct KeyValueStoreView<C> {
    map: RegisterView<C, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<C: Context> KeyValueStoreView<C> {
    fn max_key_size(&self) -> usize {
        let prefix_len = self.context().base_key().bytes.len();
        <C::Store as ReadableKeyValueStore>::MAX_KEY_SIZE - 1 - prefix_len
    }

    /// Applies the function f over all indices. If the function f returns
    /// false, then the loop ends prematurely.
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let map = self.map.get();
        for key in map.keys() {
            if !f(key)? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Applies the function f over all indices.
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_index_while(|key| {
            f(key)?;
            Ok(true)
        })
        .await
    }

    /// Applies the function f over all index/value pairs.
    /// If the function f returns false then the loop ends prematurely.
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        let map = self.map.get();
        for (key, value) in map {
            if !f(key, value)? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Applies the function f over all index/value pairs.
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_index_value_while(|key, value| {
            f(key, value)?;
            Ok(true)
        })
        .await
    }

    /// Returns the list of indices in lexicographic order.
    pub async fn indices(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index.to_vec());
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Returns the list of indices and values in lexicographic order.
    pub async fn index_values(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let mut index_values = Vec::new();
        self.for_each_index_value(|index, value| {
            index_values.push((index.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;
        Ok(index_values)
    }

    /// Returns the number of entries.
    pub async fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        self.for_each_index(|_index| {
            count += 1;
            Ok(())
        })
        .await?;
        Ok(count)
    }

    /// Obtains the value at the given index, if any.
    pub async fn get(&self, index: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_GET_LATENCY.measure_latency();
        ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
        let map = self.map.get();
        Ok(map.get(index).cloned())
    }

    /// Tests whether the store contains a specific index.
    pub async fn contains_key(&self, index: &[u8]) -> Result<bool, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_CONTAINS_KEY_LATENCY.measure_latency();
        ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
        let map = self.map.get();
        Ok(map.contains_key(index))
    }

    /// Tests whether the view contains a range of indices
    pub async fn contains_keys(&self, indices: Vec<Vec<u8>>) -> Result<Vec<bool>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_CONTAINS_KEYS_LATENCY.measure_latency();
        let map = self.map.get();
        let mut results = Vec::with_capacity(indices.len());
        for index in indices {
            ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
            results.push(map.contains_key(&index));
        }
        Ok(results)
    }

    /// Obtains the values of a range of indices
    pub async fn multi_get(
        &self,
        indices: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_MULTI_GET_LATENCY.measure_latency();
        let map = self.map.get();
        let mut results = Vec::with_capacity(indices.len());
        for index in indices {
            ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
            results.push(map.get(&index).cloned());
        }
        Ok(results)
    }

    /// Applies the given batch of `crate::common::WriteOperation`.
    pub async fn write_batch(&mut self, batch: Batch) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_WRITE_BATCH_LATENCY.measure_latency();
        let max_key_size = self.max_key_size();
        let map = self.map.get_mut();
        for operation in batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    map.remove(&key);
                }
                WriteOperation::Put { key, value } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    map.insert(key, value);
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    ensure!(key_prefix.len() <= max_key_size, ViewError::KeyTooLong);
                    let key_list = map
                        .range(get_interval(key_prefix.clone()))
                        .map(|x| x.0.to_vec())
                        .collect::<Vec<_>>();
                    for key in key_list {
                        map.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    /// Sets or inserts a value.
    pub async fn insert(&mut self, index: Vec<u8>, value: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.put_key_value_bytes(index, value);
        self.write_batch(batch).await
    }

    /// Removes a value. If absent then the action has no effect.
    pub async fn remove(&mut self, index: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.delete_key(index);
        self.write_batch(batch).await
    }

    /// Deletes a key prefix.
    pub async fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.delete_key_prefix(key_prefix);
        self.write_batch(batch).await
    }

    /// Iterates over all the keys matching the given prefix. The prefix is not included in the returned keys.
    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_FIND_KEYS_BY_PREFIX_LATENCY.measure_latency();
        ensure!(
            key_prefix.len() <= self.max_key_size(),
            ViewError::KeyTooLong
        );
        let len = key_prefix.len();
        let map = self.map.get();
        Ok(map
            .range(get_interval(key_prefix.to_vec()))
            .map(|x| x.0[len..].to_vec())
            .collect::<Vec<_>>())
    }

    /// Iterates over all the key-value pairs, for keys matching the given prefix. The
    /// prefix is not included in the returned keys.
    pub async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        #[cfg(with_metrics)]
        let _latency =
            metrics::KEY_VALUE_STORE_VIEW_FIND_KEY_VALUES_BY_PREFIX_LATENCY.measure_latency();
        ensure!(
            key_prefix.len() <= self.max_key_size(),
            ViewError::KeyTooLong
        );
        let len = key_prefix.len();
        let map = self.map.get();
        Ok(map
            .range(get_interval(key_prefix.to_vec()))
            .map(|x| (x.0[len..].to_vec(), x.1.to_vec()))
            .collect::<Vec<_>>())
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::KEY_VALUE_STORE_VIEW_HASH_LATENCY.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0u32;
        self.for_each_index_value(|index, value| -> Result<(), ViewError> {
            count += 1;
            hasher.update_with_bytes(index)?;
            hasher.update_with_bytes(value)?;
            Ok(())
        })
        .await?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

impl<C: Context> HashableView for KeyValueStoreView<C> {
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }
}

/// Type wrapping `KeyValueStoreView` while memoizing the hash.
pub(crate) type HashedKeyValueStoreView<C> =
    WrappedHashableContainerView<C, KeyValueStoreView<C>, HasherOutput>;
