// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage::{StorageCacheConfig, DEFAULT_CLEANUP_INTERVAL_SECS};
use linera_views::lru_prefix_cache::StorageCacheConfig as ViewsStorageCacheConfig;

/// Command-line options shared by all storage backends, controlling concurrency
/// limits and cache sizes.
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

    /// The maximal number of entries in the block-hash-by-height cache.
    #[arg(long, default_value = "1000", global = true)]
    pub block_hash_by_height_cache_size: usize,

    /// The maximal number of entries in the event-block-height cache.
    #[arg(long, default_value = "1000", global = true)]
    pub event_block_height_cache_size: usize,

    /// Interval in seconds between weak reference cleanup sweeps in value caches.
    #[arg(long, default_value_t = DEFAULT_CLEANUP_INTERVAL_SECS, global = true)]
    pub cache_cleanup_interval_secs: u64,

    /// The replication factor for the keyspace
    #[arg(long, default_value = "1", global = true)]
    pub storage_replication_factor: u32,

    /// Enable RocksDB's internal statistics collection and export them as Prometheus
    /// metrics. Off by default; enable it on nodes whose metrics are scraped.
    #[arg(long, global = true)]
    pub rocksdb_enable_statistics: bool,

    /// The level of detail collected when `--rocksdb-enable-statistics` is set. Higher
    /// levels collect more, and more expensive, data.
    #[arg(long, default_value = "except-histogram-or-timers", global = true)]
    pub rocksdb_statistics_level: RocksDbStatisticsLevelArg,
}

/// The RocksDB statistics collection level, selectable on the command line.
#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum RocksDbStatisticsLevelArg {
    /// Collect nothing.
    DisableAll,
    /// Collect tickers (counters) only; skip all histograms and timers.
    #[default]
    ExceptHistogramOrTimers,
    /// Collect tickers and histograms, but skip timer statistics.
    ExceptTimers,
    /// Collect everything except mutex-lock and compression timing.
    ExceptDetailedTimers,
    /// Collect everything except the counters requiring time inside the mutex lock.
    ExceptTimeForMutex,
    /// Collect everything, including mutex operation timing.
    All,
}

#[cfg(feature = "rocksdb")]
impl From<RocksDbStatisticsLevelArg> for linera_views::rocks_db::RocksDbStatisticsLevel {
    fn from(level: RocksDbStatisticsLevelArg) -> Self {
        match level {
            RocksDbStatisticsLevelArg::DisableAll => Self::DisableAll,
            RocksDbStatisticsLevelArg::ExceptHistogramOrTimers => Self::ExceptHistogramOrTimers,
            RocksDbStatisticsLevelArg::ExceptTimers => Self::ExceptTimers,
            RocksDbStatisticsLevelArg::ExceptDetailedTimers => Self::ExceptDetailedTimers,
            RocksDbStatisticsLevelArg::ExceptTimeForMutex => Self::ExceptTimeForMutex,
            RocksDbStatisticsLevelArg::All => Self::All,
        }
    }
}

impl CommonStorageOptions {
    /// Returns the options with their default values.
    pub fn with_defaults() -> Self {
        use clap::Parser as _;
        Self::parse_from(std::iter::empty::<String>())
    }

    /// Builds the storage cache configuration from these options.
    pub fn storage_cache_config(&self) -> StorageCacheConfig {
        StorageCacheConfig {
            blob_cache_size: self.blob_cache_size,
            confirmed_block_cache_size: self.confirmed_block_cache_size,
            certificate_cache_size: self.certificate_cache_size,
            certificate_raw_cache_size: self.certificate_raw_cache_size,
            event_cache_size: self.event_cache_size,
            block_hash_by_height_cache_size: self.block_hash_by_height_cache_size,
            event_block_height_cache_size: self.event_block_height_cache_size,
            cache_cleanup_interval_secs: self.cache_cleanup_interval_secs,
        }
    }

    /// Builds the views storage cache configuration from these options.
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

#[cfg(all(test, feature = "rocksdb"))]
mod tests {
    use clap::Parser as _;
    use linera_views::rocks_db::RocksDbStatisticsLevel;

    use super::{CommonStorageOptions, RocksDbStatisticsLevelArg};

    #[test]
    fn statistics_disabled_by_default() {
        let options = CommonStorageOptions::with_defaults();
        assert!(!options.rocksdb_enable_statistics);
        assert_eq!(
            RocksDbStatisticsLevel::from(options.rocksdb_statistics_level),
            RocksDbStatisticsLevel::ExceptHistogramOrTimers,
        );
    }

    #[test]
    fn parses_enable_flag_and_level() {
        let options = CommonStorageOptions::parse_from([
            "test",
            "--rocksdb-enable-statistics",
            "--rocksdb-statistics-level",
            "all",
        ]);
        assert!(options.rocksdb_enable_statistics);
        assert_eq!(
            RocksDbStatisticsLevel::from(options.rocksdb_statistics_level),
            RocksDbStatisticsLevel::All,
        );
    }

    #[test]
    fn level_arg_maps_one_to_one() {
        let cases = [
            (
                RocksDbStatisticsLevelArg::DisableAll,
                RocksDbStatisticsLevel::DisableAll,
            ),
            (
                RocksDbStatisticsLevelArg::ExceptHistogramOrTimers,
                RocksDbStatisticsLevel::ExceptHistogramOrTimers,
            ),
            (
                RocksDbStatisticsLevelArg::ExceptTimers,
                RocksDbStatisticsLevel::ExceptTimers,
            ),
            (
                RocksDbStatisticsLevelArg::ExceptDetailedTimers,
                RocksDbStatisticsLevel::ExceptDetailedTimers,
            ),
            (
                RocksDbStatisticsLevelArg::ExceptTimeForMutex,
                RocksDbStatisticsLevel::ExceptTimeForMutex,
            ),
            (RocksDbStatisticsLevelArg::All, RocksDbStatisticsLevel::All),
        ];
        for (arg, expected) in cases {
            assert_eq!(RocksDbStatisticsLevel::from(arg), expected);
        }
    }
}
