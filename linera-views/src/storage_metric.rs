// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use crate::{
    batch::{Batch, WriteOperation},
    common::{KeyIterable, KeyValueStoreClient},
};
use std::sync::Arc;
use async_lock::RwLock;

/// Array containing data for this client
#[derive(Clone, Default, Debug)]
pub struct MetricStat {
    /// The total number of read_key and read_multi_key
    n_reads: usize,
    /// The number of missed cases in read_key and read_multi_key
    n_miss_reads: usize,
    /// The number of Put in the batches
    n_puts: usize,
    /// The total number of Delete in the batches
    n_deletes: usize,
    /// The total number of DeletePrefix in the batches
    n_delete_prefix: usize,
    /// The total data that went into the Batches
    size_writes: usize,
    /// The total data being read from
    size_reads: usize,
}

/// The `MetricKeyValueClient` encapsulates a client and also measures the operations
/// being done
#[derive(Clone)]
pub struct MetricKeyValueClient<K> {
    /// The inner client that is called by the LRU cache one
    pub client: K,
    /// The data contained in the running of this container
    pub metric_stat: Arc<RwLock<MetricStat>>,
}

/// A container for a KeyIterable and a counter
struct KeyIterableCounter<K, I> {
    /// The counter
    pub metric_stat: MetricStat,
    /// The key iterable
    pub key_iterable: I,
    _store_client: std::marker::PhantomData<K>,
}

impl<K> KeyIterable<K::Error> for KeyIterableCounter<K, K::Keys>
where
    K: KeyValueStoreClient,
    <K as KeyValueStoreClient>::Keys: KeyIterable<<K as KeyValueStoreClient>::Error>
{
    type Iterator<'a> = KeyIteratorCounter<'a, K, <<K as KeyValueStoreClient>::Keys as KeyIterable<K::Error>>::Iterator<'a>>
    where
        Self: 'a;

    fn iterator(&self) -> Self::Iterator<'_> {
        let iter = self.key_iterable.iterator();
        let metric_stat = self.metric_stat.clone();
        KeyIteratorCounter { metric_stat, iter, _store_client: std::marker::PhantomData }
    }
}

/// The corresponding Iterator
struct KeyIteratorCounter<'a, K, I> {
    /// The counter
    pub metric_stat: MetricStat,
    /// The key iterator
    pub iter: I,
    _store_client: std::marker::PhantomData<&'a K>,
}

impl<'a, K> Iterator for KeyIteratorCounter<'a, K, <<K as KeyValueStoreClient>::Keys as KeyIterable<K::Error>>::Iterator<'a>>
where
    K: KeyValueStoreClient,
    <K as KeyValueStoreClient>::Keys: KeyIterable<<K as KeyValueStoreClient>::Error>
{
    type Item = Result<&'a [u8], <K as KeyValueStoreClient>::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}



/*
Two types:
---Standard case (Vec<Vec<u8>>):
   * T1 : Vec<Vec<u8>>
   * T2 : SimpleKeyIterator<'a, E>
---DynamoDb
   * T1 : DynamoDbKeys
   * T2 : DynamoDbKeyBlockIterator<'a>
---Our case here
   * T1: KeyIterableCounter<K> (basically (metric_stat, K::Keys)
   * T2: KeyIteratorCounter<K>

The link is established via
   T2 = T1::Iterator

 */



#[async_trait]
impl<K> KeyValueStoreClient for MetricKeyValueClient<K>
where
    K: KeyValueStoreClient + Send + Sync,
{
    // The LRU cache does not change the underlying client's size limit.
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;
    type Error = K::Error;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.client.max_stream_queries()
    }

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let read = self.client.read_key_bytes(key).await?;
        let mut metric_stat = self.metric_stat.write().await;
        metric_stat.n_reads += 1;
        match &read {
            None => {
                metric_stat.n_miss_reads += 1;
            },
            Some(value) => {
                metric_stat.size_reads += value.len();
            },
        }
        Ok(read)
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let multi_read = self.client.read_multi_key_bytes(keys).await?;
        let mut metric_stat = self.metric_stat.write().await;
        for read in &multi_read {
            match read {
                None => {
                    metric_stat.n_miss_reads += 1;
                },
                Some(value) => {
                    metric_stat.size_reads += value.len();
                },
            }
        }
        Ok(multi_read)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.client.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.client.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Self::Error> {
        let mut metric_stat = self.metric_stat.write().await;
        for operation in &batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    metric_stat.n_deletes += 1;
                    metric_stat.size_writes += key.len();
                },
                WriteOperation::Put { key, value } => {
                    metric_stat.n_puts += 1;
                    metric_stat.size_writes += key.len() + value.len();
                },
                WriteOperation::DeletePrefix { key_prefix } => {
                    metric_stat.n_delete_prefix += 1;
                    metric_stat.size_writes += key_prefix.len();
                },
            }
        }
        self.client.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.client.clear_journal(base_key).await
    }
}
