// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{KeyIterable, KeyValueIterable, KeyValueStoreClient},
    memory::{create_memory_client, MemoryClient},
};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Array containing metric data for this client
/// This can be used for storage fees or for other
/// benchmarking purposes.
#[derive(Clone, Default, Debug)]
pub struct AtomicMetricStat {
    /// The total number of keys read in calls to `read_key` and `read_multi_key`
    pub n_reads: Arc<AtomicUsize>,
    /// The number of missed cases in read_key and read_multi_key
    pub n_miss_reads: Arc<AtomicUsize>,
    /// The number of Put in the batches
    pub n_puts: Arc<AtomicUsize>,
    /// The total number of Delete in the batches
    pub n_deletes: Arc<AtomicUsize>,
    /// The total number of DeletePrefix in the batches
    pub n_delete_prefix: Arc<AtomicUsize>,
    /// The total data that went into the Batches
    pub size_writes: Arc<AtomicUsize>,
    /// The total data being read from
    pub size_reads: Arc<AtomicUsize>,
}

/// Array containing metric data for this client
/// This can be used for storage fees or for other
/// benchmarking purposes.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
pub struct MetricStat {
    /// The total number of keys read in calls to `read_key` and `read_multi_key`
    pub n_reads: usize,
    /// The number of missed cases in read_key and read_multi_key
    pub n_miss_reads: usize,
    /// The number of Put in the batches
    pub n_puts: usize,
    /// The total number of Delete in the batches
    pub n_deletes: usize,
    /// The total number of DeletePrefix in the batches
    pub n_delete_prefix: usize,
    /// The total data that went into the Batches
    pub size_writes: usize,
    /// The total data being read from
    pub size_reads: usize,
}





/// The `MetricKeyValueClient` encapsulates a client and creates a new one that
/// measures the operations being done
#[derive(Clone)]
pub struct MetricKeyValueClient<K> {
    /// The inner client that is called by the metric one
    pub client: K,
    /// The data contained in the running of this container
    pub metric_stat: AtomicMetricStat,
}

#[async_trait]
impl<K> KeyValueStoreClient for MetricKeyValueClient<K>
where
    K: KeyValueStoreClient + Send + Sync,
{
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;
    type Error = K::Error;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.client.max_stream_queries()
    }

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let read = self.client.read_key_bytes(key).await?;
        self.metric_stat.n_reads.fetch_add(1, Ordering::Relaxed);
        match &read {
            None => {
                self.metric_stat.n_miss_reads.fetch_add(1, Ordering::Relaxed);
            }
            Some(value) => {
                self.metric_stat.size_reads.fetch_add(value.len(), Ordering::Relaxed);
            }
        }
        Ok(read)
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let n_keys = keys.len();
        let multi_read = self.client.read_multi_key_bytes(keys).await?;
        self.metric_stat.n_reads.fetch_add(n_keys, Ordering::Relaxed);
        for read in &multi_read {
            match read {
                None => {
                    self.metric_stat.n_miss_reads.fetch_add(1, Ordering::Relaxed);
                }
                Some(value) => {
                    self.metric_stat.size_reads.fetch_add(value.len(), Ordering::Relaxed);
                }
            }
        }
        Ok(multi_read)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        let keys = self.client.find_keys_by_prefix(key_prefix).await?;
        for key in keys.iterator() {
            self.metric_stat.size_reads.fetch_add(key?.len(), Ordering::Relaxed);
        }
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let key_values = self.client.find_key_values_by_prefix(key_prefix).await?;
        for key_value in key_values.iterator() {
            let key_value = key_value?;
            self.metric_stat.size_reads.fetch_add(key_value.0.len() + key_value.1.len(), Ordering::Relaxed);
        }
        Ok(key_values)
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Self::Error> {
        for operation in &batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    self.metric_stat.n_deletes.fetch_add(1, Ordering::Relaxed);
                    self.metric_stat.size_writes.fetch_add(key.len(), Ordering::Relaxed);
                }
                WriteOperation::Put { key, value } => {
                    self.metric_stat.n_puts.fetch_add(1, Ordering::Relaxed);
                    self.metric_stat.size_writes.fetch_add(key.len() + value.len(), Ordering::Relaxed);
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    self.metric_stat.n_deletes.fetch_add(1, Ordering::Relaxed);
                    self.metric_stat.size_writes.fetch_add(key_prefix.len(), Ordering::Relaxed);
                }
            }
        }
        self.client.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.client.clear_journal(base_key).await
    }
}

impl<K> MetricKeyValueClient<K>
where
    K: KeyValueStoreClient + Send + Sync,
{
    /// Returning the current metric information
    pub fn metric(&self) -> MetricStat {
        let n_reads = self.metric_stat.n_reads.load(Ordering::Relaxed);
        let n_miss_reads = self.metric_stat.n_miss_reads.load(Ordering::Relaxed);
        let n_puts = self.metric_stat.n_puts.load(Ordering::Relaxed);
        let n_deletes = self.metric_stat.n_deletes.load(Ordering::Relaxed);
        let n_delete_prefix = self.metric_stat.n_delete_prefix.load(Ordering::Relaxed);
        let size_writes = self.metric_stat.size_writes.load(Ordering::Relaxed);
        let size_reads = self.metric_stat.size_reads.load(Ordering::Relaxed);
        MetricStat { n_reads, n_miss_reads, n_puts, n_deletes, n_delete_prefix, size_writes, size_reads }
    }
}


/// Create a new client
pub fn get_metric_memory_client() -> MetricKeyValueClient<MemoryClient> {
    let client = create_memory_client();
    let metric_stat = AtomicMetricStat::default();
    MetricKeyValueClient {
        client,
        metric_stat,
    }
}

#[cfg(test)]
mod tests {
    use linera_views::{
        batch::Batch,
        common::KeyValueStoreClient,
        memory::MemoryClient,
        storage_metric::{get_metric_memory_client, MetricKeyValueClient, MetricStat},
    };

    async fn get_memory_test_state() -> MetricKeyValueClient<MemoryClient> {
        let client = get_metric_memory_client();
        assert_eq!(client.metric(), MetricStat::default());
        let mut batch = Batch::new();
        batch.put_key_value_bytes(vec![1, 2, 3], vec![1]);
        batch.put_key_value_bytes(vec![1, 2, 4], vec![2, 2]);
        batch.put_key_value_bytes(vec![1, 2, 5], vec![3, 3, 3]);
        batch.put_key_value_bytes(vec![1, 3, 3], vec![4, 4, 4, 4]);
        batch.delete_key(vec![1, 3, 7]);
        batch.delete_key_prefix(vec![2, 3]);
        client.write_batch(batch, &[]).await.unwrap();
        assert_eq!(
            client.metric(),
            MetricStat {
                n_reads: 0,
                n_miss_reads: 0,
                n_puts: 4,
                n_deletes: 1,
                n_delete_prefix: 1,
                size_writes: 27,
                size_reads: 0
            }
        );
        client
    }

    #[tokio::test]
    async fn test_metric_read_existing_key() {
        let client = get_memory_test_state().await;
        client.read_key_bytes(&[1, 3, 3]).await.unwrap();
        assert_eq!(
            client.metric(),
            MetricStat {
                n_reads: 1,
                n_miss_reads: 0,
                n_puts: 4,
                n_deletes: 1,
                n_delete_prefix: 1,
                size_writes: 27,
                size_reads: 4
            }
        );
    }

    #[tokio::test]
    async fn test_metric_read_missing_key() {
        let client = get_memory_test_state().await;
        client.read_key_bytes(&[1, 4, 4]).await.unwrap();
        assert_eq!(
            client.metric(),
            MetricStat {
                n_reads: 1,
                n_miss_reads: 1,
                n_puts: 4,
                n_deletes: 1,
                n_delete_prefix: 1,
                size_writes: 27,
                size_reads: 0
            }
        );
    }

    #[tokio::test]
    async fn test_metric_read_multi_key() {
        let client = get_memory_test_state().await;
        client
            .read_multi_key_bytes(vec![vec![1, 3, 3], vec![1, 2, 5]])
            .await
            .unwrap();
        assert_eq!(
            client.metric(),
            MetricStat {
                n_reads: 2,
                n_miss_reads: 0,
                n_puts: 4,
                n_deletes: 1,
                n_delete_prefix: 1,
                size_writes: 27,
                size_reads: 7
            }
        );
    }

    #[tokio::test]
    async fn test_metric_find_keys() {
        let client = get_memory_test_state().await;
        client.find_keys_by_prefix(&[1, 2]).await.unwrap();
        assert_eq!(
            client.metric(),
            MetricStat {
                n_reads: 0,
                n_miss_reads: 0,
                n_puts: 4,
                n_deletes: 1,
                n_delete_prefix: 1,
                size_writes: 27,
                size_reads: 3
            }
        );
    }

    #[tokio::test]
    async fn test_metric_find_key_values() {
        let client = get_memory_test_state().await;
        client.find_key_values_by_prefix(&[1, 2]).await.unwrap();
        assert_eq!(
            client.metric(),
            MetricStat {
                n_reads: 0,
                n_miss_reads: 0,
                n_puts: 4,
                n_deletes: 1,
                n_delete_prefix: 1,
                size_writes: 27,
                size_reads: 9
            }
        );
    }
}
