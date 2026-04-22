// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage::{StorageCacheConfig, DEFAULT_CLEANUP_INTERVAL_SECS};
use linera_views::lru_prefix_cache::StorageCacheConfig as ViewsStorageCacheConfig;

#[derive(Clone, Debug, clap::Parser)]
pub struct CommonStorageOptions {
    /// The maximal number of simultaneous queries to the database
    #[arg(long, global = true)]
    pub storage_max_concurrent_queries: Option<usize>,

    /// The maximal number of simultaneous stream queries to the database
    #[arg(long, default_value = "10", global = true)]
    pub storage_max_stream_queries: usize,

    /// The maximal memory used in the storage cache.
    #[arg(long, default_value = "10000000", global = true)]
    pub storage_max_cache_size: usize,

    /// The maximal size of a value entry in the storage cache.
    #[arg(long, default_value = "1000000", global = true)]
    pub storage_max_value_entry_size: usize,

    /// The maximal size of a find-keys entry in the storage cache.
    #[arg(long, default_value = "1000000", global = true)]
    pub storage_max_find_keys_entry_size: usize,

    /// The maximal size of a find-key-values entry in the storage cache.
    #[arg(long, default_value = "1000000", global = true)]
    pub storage_max_find_key_values_entry_size: usize,

    /// The maximal number of entries in the storage cache.
    #[arg(long, default_value = "1000", global = true)]
    pub storage_max_cache_entries: usize,

    /// The maximal memory used in the value cache.
    #[arg(long, default_value = "10000000", global = true)]
    pub storage_max_cache_value_size: usize,

    /// The maximal memory used in the find_keys_by_prefix cache.
    #[arg(long, default_value = "10000000", global = true)]
    pub storage_max_cache_find_keys_size: usize,

    /// The maximal memory used in the find_key_values_by_prefix cache.
    #[arg(long, default_value = "10000000", global = true)]
    pub storage_max_cache_find_key_values_size: usize,

    /// The maximal number of entries in the blob cache.
    #[arg(long, default_value = "1000", global = true)]
    pub blob_cache_size: usize,

    /// The maximal number of entries in the confirmed block cache.
    #[arg(long, default_value = "1000", global = true)]
    pub confirmed_block_cache_size: usize,

    /// The maximal number of entries in the assembled certificate cache.
    #[arg(long, default_value = "1000", global = true)]
    pub certificate_cache_size: usize,

    /// The maximal number of entries in the raw certificate cache.
    #[arg(long, default_value = "1000", global = true)]
    pub certificate_raw_cache_size: usize,

    /// The maximal number of entries in the event cache.
    #[arg(long, default_value = "1000", global = true)]
    pub event_cache_size: usize,

    /// Interval in seconds between weak reference cleanup sweeps in value caches.
    #[arg(long, default_value_t = DEFAULT_CLEANUP_INTERVAL_SECS, global = true)]
    pub cache_cleanup_interval_secs: u64,

    /// The replication factor for the keyspace
    #[arg(long, default_value = "1", global = true)]
    pub storage_replication_factor: u32,
}

impl CommonStorageOptions {
    pub fn storage_cache_config(&self) -> StorageCacheConfig {
        StorageCacheConfig {
            blob_cache_size: self.blob_cache_size,
            confirmed_block_cache_size: self.confirmed_block_cache_size,
            certificate_cache_size: self.certificate_cache_size,
            certificate_raw_cache_size: self.certificate_raw_cache_size,
            event_cache_size: self.event_cache_size,
            cache_cleanup_interval_secs: self.cache_cleanup_interval_secs,
        }
    }

    pub fn views_storage_cache_config(&self) -> ViewsStorageCacheConfig {
        ViewsStorageCacheConfig {
            max_cache_size: self.storage_max_cache_size,
            max_value_entry_size: self.storage_max_value_entry_size,
            max_find_keys_entry_size: self.storage_max_find_keys_entry_size,
            max_find_key_values_entry_size: self.storage_max_find_key_values_entry_size,
            max_cache_entries: self.storage_max_cache_entries,
            max_cache_value_size: self.storage_max_cache_value_size,
            max_cache_find_keys_size: self.storage_max_cache_find_keys_size,
            max_cache_find_key_values_size: self.storage_max_cache_find_key_values_size,
        }
    }
}
