// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the RocksDB database.

// RocksDB's C API uses `i32` and signed sizes; casts at this boundary are
// by design.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use std::{
    ffi::OsString,
    fmt::Display,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use linera_base::ensure;
use rocksdb::{BlockBasedOptions, Cache, DBCompactionStyle, SliceTransform, WriteBufferManager};
use serde::{Deserialize, Serialize};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use tempfile::TempDir;
use thiserror::Error;

#[cfg(with_metrics)]
use crate::metering::MeteredDatabase;
#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::{Batch, WriteOperation},
    common::get_upper_bound_option,
    lru_caching::{LruCachingConfig, LruCachingDatabase},
    store::{
        KeyValueDatabase, KeyValueStoreError, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
    },
    value_splitting::{ValueSplittingDatabase, ValueSplittingError},
};

/// The prefixes being used in the system
static ROOT_KEY_DOMAIN: [u8; 1] = [0];
static STORED_ROOT_KEYS_PREFIX: u8 = 1;

/// The number of streams for the test
#[cfg(with_testing)]
const TEST_ROCKS_DB_MAX_STREAM_QUERIES: usize = 10;

// The maximum size of values in RocksDB is 3 GiB
// For offset reasons we decrease by 400
const MAX_VALUE_SIZE: usize = 3 * 1024 * 1024 * 1024 - 400;

// The maximum size of keys in RocksDB is 8 MiB
// For offset reasons we decrease by 400
const MAX_KEY_SIZE: usize = 8 * 1024 * 1024 - 400;

const WRITE_BUFFER_SIZE: usize = 256 * 1024 * 1024; // 256 MiB
const MAX_WRITE_BUFFER_NUMBER: i32 = 6;

fn get_available_memory(sys: &System) -> usize {
    sys.cgroup_limits()
        .map_or_else(|| sys.total_memory() as usize, |c| c.total_memory as usize)
}

fn get_available_cpus() -> i32 {
    std::thread::available_parallelism().map_or(1, |p| p.get() as i32)
}

const HYPER_CLOCK_CACHE_BLOCK_SIZE: usize = 8 * 1024; // 8 KiB

/// The RocksDB client that we use.
type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// The choice of the spawning mode.
/// `SpawnBlocking` always works and is the safest.
/// `BlockInPlace` can only be used in multi-threaded environment.
/// One way to select that is to select BlockInPlace when
/// `tokio::runtime::Handle::current().metrics().num_workers() > 1`
/// `BlockInPlace` is documented in <https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html>
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum RocksDbSpawnMode {
    /// This uses the `spawn_blocking` function of Tokio.
    SpawnBlocking,
    /// This uses the `block_in_place` function of Tokio.
    BlockInPlace,
}

impl RocksDbSpawnMode {
    /// Obtains the spawning mode from runtime.
    pub fn get_spawn_mode_from_runtime() -> Self {
        if tokio::runtime::Handle::current().metrics().num_workers() > 1 {
            RocksDbSpawnMode::BlockInPlace
        } else {
            RocksDbSpawnMode::SpawnBlocking
        }
    }

    /// Runs the computation for a function according to the selected policy.
    #[inline]
    async fn spawn<F, I, O>(&self, f: F, input: I) -> Result<O, RocksDbStoreInternalError>
    where
        F: FnOnce(I) -> Result<O, RocksDbStoreInternalError> + Send + 'static,
        I: Send + 'static,
        O: Send + 'static,
    {
        Ok(match self {
            RocksDbSpawnMode::BlockInPlace => tokio::task::block_in_place(move || f(input))?,
            RocksDbSpawnMode::SpawnBlocking => {
                tokio::task::spawn_blocking(move || f(input)).await??
            }
        })
    }
}

impl Display for RocksDbSpawnMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            RocksDbSpawnMode::SpawnBlocking => write!(f, "spawn_blocking"),
            RocksDbSpawnMode::BlockInPlace => write!(f, "block_in_place"),
        }
    }
}

fn check_key_size(key: &[u8]) -> Result<(), RocksDbStoreInternalError> {
    ensure!(
        key.len() <= MAX_KEY_SIZE,
        RocksDbStoreInternalError::KeyTooLong
    );
    Ok(())
}

#[derive(Clone)]
struct RocksDbStoreExecutor {
    db: Arc<DB>,
    start_key: Vec<u8>,
}

impl RocksDbStoreExecutor {
    fn contains_keys_internal(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, RocksDbStoreInternalError> {
        let size = keys.len();
        let mut results = vec![false; size];
        let mut indices = Vec::new();
        let mut keys_red = Vec::new();
        for (i, key) in keys.into_iter().enumerate() {
            check_key_size(&key)?;
            let mut full_key = self.start_key.to_vec();
            full_key.extend(key);
            if self.db.key_may_exist(&full_key) {
                indices.push(i);
                keys_red.push(full_key);
            }
        }
        let values_red = self.db.multi_get(keys_red);
        for (index, value) in indices.into_iter().zip(values_red) {
            results[index] = value?.is_some();
        }
        Ok(results)
    }

    fn read_multi_values_bytes_internal(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbStoreInternalError> {
        for key in &keys {
            check_key_size(key)?;
        }
        let full_keys = keys
            .into_iter()
            .map(|key| {
                let mut full_key = self.start_key.to_vec();
                full_key.extend(key);
                full_key
            })
            .collect::<Vec<_>>();
        let entries = self.db.multi_get(&full_keys);
        Ok(entries.into_iter().collect::<Result<_, _>>()?)
    }

    fn get_find_prefix_iterator(
        &self,
        prefix: &[u8],
    ) -> rocksdb::DBRawIteratorWithThreadMode<'_, DB> {
        // Configure ReadOptions optimized for SSDs and iterator performance
        let mut read_opts = rocksdb::ReadOptions::default();
        // Enable async I/O for better concurrency
        read_opts.set_async_io(true);

        // Set precise upper bound to minimize key traversal
        let upper_bound = get_upper_bound_option(prefix);
        if let Some(upper_bound) = upper_bound {
            read_opts.set_iterate_upper_bound(upper_bound);
        }

        let mut iter = self.db.raw_iterator_opt(read_opts);
        iter.seek(prefix);
        iter
    }

    fn find_keys_by_prefix_internal(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, RocksDbStoreInternalError> {
        check_key_size(&key_prefix)?;

        let mut prefix = self.start_key.clone();
        prefix.extend(key_prefix);
        let len = prefix.len();

        let mut iter = self.get_find_prefix_iterator(&prefix);
        let mut keys = Vec::new();
        while let Some(key) = iter.key() {
            keys.push(key[len..].to_vec());
            iter.next();
        }
        Ok(keys)
    }

    #[expect(clippy::type_complexity)]
    fn find_key_values_by_prefix_internal(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksDbStoreInternalError> {
        check_key_size(&key_prefix)?;
        let mut prefix = self.start_key.clone();
        prefix.extend(key_prefix);
        let len = prefix.len();

        let mut iter = self.get_find_prefix_iterator(&prefix);
        let mut key_values = Vec::new();
        while let Some((key, value)) = iter.item() {
            let key_value = (key[len..].to_vec(), value.to_vec());
            key_values.push(key_value);
            iter.next();
        }
        Ok(key_values)
    }

    fn write_batch_internal(
        &self,
        batch: Batch,
        write_root_key: bool,
    ) -> Result<(), RocksDbStoreInternalError> {
        let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
        for operation in batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    check_key_size(&key)?;
                    let mut full_key = self.start_key.to_vec();
                    full_key.extend(key);
                    inner_batch.delete(&full_key)
                }
                WriteOperation::Put { key, value } => {
                    check_key_size(&key)?;
                    let mut full_key = self.start_key.to_vec();
                    full_key.extend(key);
                    inner_batch.put(&full_key, value)
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    check_key_size(&key_prefix)?;
                    let mut full_key1 = self.start_key.to_vec();
                    full_key1.extend(&key_prefix);
                    let full_key2 =
                        get_upper_bound_option(&full_key1).expect("the first entry cannot be 255");
                    inner_batch.delete_range(&full_key1, &full_key2);
                }
            }
        }
        if write_root_key {
            let mut full_key = self.start_key.to_vec();
            full_key[0] = STORED_ROOT_KEYS_PREFIX;
            inner_batch.put(&full_key, vec![]);
        }
        self.db.write(inner_batch)?;
        Ok(())
    }
}

/// The inner client
#[derive(Clone)]
pub struct RocksDbStoreInternal {
    executor: RocksDbStoreExecutor,
    path_with_guard: PathWithGuard,
    max_stream_queries: usize,
    spawn_mode: RocksDbSpawnMode,
    root_key_written: Arc<AtomicBool>,
}

/// Database-level connection to RocksDB for managing namespaces and partitions.
#[derive(Clone)]
pub struct RocksDbDatabaseInternal {
    executor: RocksDbStoreExecutor,
    path_with_guard: PathWithGuard,
    max_stream_queries: usize,
    spawn_mode: RocksDbSpawnMode,
}

impl WithError for RocksDbDatabaseInternal {
    type Error = RocksDbStoreInternalError;
}

/// The initial configuration of the system
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RocksDbStoreInternalConfig {
    /// The path to the storage containing the namespaces
    pub path_with_guard: PathWithGuard,
    /// The chosen spawn mode
    pub spawn_mode: RocksDbSpawnMode,
    /// Preferred buffer size for async streams.
    pub max_stream_queries: usize,
    /// Runtime-tunable RocksDB options that override the built-in defaults.
    #[serde(default)]
    pub tuning_options: RocksDbTuningOptions,
}

/// Runtime-tunable RocksDB options.
///
/// These options change only the runtime behavior of RocksDB — memory budgets,
/// parallelism, compaction triggers, and how *new* data is written. They never
/// change the on-disk storage format in a way that would make existing data
/// unreadable, so they are safe to change between restarts of an existing
/// database (options that affect newly written SSTables, such as compression or
/// block size, simply take full effect as old files are rewritten by
/// compaction). Every field defaults to `None`, meaning the built-in default is
/// used.
///
/// Format-defining options (the prefix extractor, block format version,
/// compaction style, whole-key filtering, …) are deliberately *not* exposed
/// here, and are rejected by [`RocksDbTuningOptions::from_kv_pairs`].
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RocksDbTuningOptions {
    /// Size of a single memtable, in bytes (`write_buffer_size`).
    pub write_buffer_size: Option<usize>,
    /// Maximum number of memtables held in memory (`max_write_buffer_number`).
    pub max_write_buffer_number: Option<i32>,
    /// Number of L0 files that triggers a write slowdown.
    pub level_zero_slowdown_writes_trigger: Option<i32>,
    /// Number of L0 files that triggers a write stop.
    pub level_zero_stop_writes_trigger: Option<i32>,
    /// Number of L0 files that triggers compaction.
    pub level_zero_file_num_compaction_trigger: Option<i32>,
    /// Background thread parallelism (`increase_parallelism`).
    pub parallelism: Option<i32>,
    /// Maximum number of concurrent background jobs (flush + compaction).
    pub max_background_jobs: Option<i32>,
    /// Maximum number of threads used by a single compaction job.
    pub max_subcompactions: Option<u32>,
    /// Target SST file size at the base level, in bytes.
    pub target_file_size_base: Option<u64>,
    /// Block cache size, in bytes.
    pub block_cache_size: Option<usize>,
    /// Total memtable memory budget across column families, in bytes.
    pub write_buffer_manager_size: Option<usize>,
    /// Maximum number of open files (`-1` for unlimited).
    pub max_open_files: Option<i32>,
    /// Table block size, in bytes (affects newly written SSTables).
    pub block_size: Option<usize>,
    /// Bloom filter bits per key (affects newly written SSTables).
    pub bloom_filter_bits_per_key: Option<f64>,
    /// Compression algorithm for newly written blocks.
    pub compression_type: Option<RocksDbCompressionType>,
}

/// The block compression algorithm used by RocksDB for newly written blocks.
///
/// Changing this is safe: RocksDB records the algorithm per block, so existing
/// blocks remain readable regardless of the current setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum RocksDbCompressionType {
    /// No compression.
    None,
    /// Snappy.
    Snappy,
    /// LZ4 (the default).
    Lz4,
    /// LZ4 high-compression.
    Lz4hc,
    /// Zstandard.
    Zstd,
    /// Zlib.
    Zlib,
    /// Bzip2.
    Bz2,
}

impl RocksDbCompressionType {
    fn to_rocksdb(self) -> rocksdb::DBCompressionType {
        match self {
            RocksDbCompressionType::None => rocksdb::DBCompressionType::None,
            RocksDbCompressionType::Snappy => rocksdb::DBCompressionType::Snappy,
            RocksDbCompressionType::Lz4 => rocksdb::DBCompressionType::Lz4,
            RocksDbCompressionType::Lz4hc => rocksdb::DBCompressionType::Lz4hc,
            RocksDbCompressionType::Zstd => rocksdb::DBCompressionType::Zstd,
            RocksDbCompressionType::Zlib => rocksdb::DBCompressionType::Zlib,
            RocksDbCompressionType::Bz2 => rocksdb::DBCompressionType::Bz2,
        }
    }

    fn from_token(token: &str) -> Option<Self> {
        Some(match token.to_ascii_lowercase().as_str() {
            "none" => RocksDbCompressionType::None,
            "snappy" => RocksDbCompressionType::Snappy,
            "lz4" => RocksDbCompressionType::Lz4,
            "lz4hc" => RocksDbCompressionType::Lz4hc,
            "zstd" => RocksDbCompressionType::Zstd,
            "zlib" => RocksDbCompressionType::Zlib,
            "bz2" => RocksDbCompressionType::Bz2,
            _ => return None,
        })
    }
}

impl RocksDbTuningOptions {
    /// Option keys that affect the on-disk storage format. Passing any of these
    /// to [`Self::from_kv_pairs`] is an error: they cannot be changed safely on
    /// an existing database.
    const FORMAT_SENSITIVE_KEYS: &'static [&'static str] = &[
        "prefix_extractor",
        "format_version",
        "compaction_style",
        "whole_key_filtering",
        "memtable_prefix_bloom_ratio",
        "comparator",
        "merge_operator",
        "max_key_size",
        "max_value_size",
    ];

    /// The list of supported option keys, for use in error messages and docs.
    pub fn supported_keys() -> &'static [&'static str] {
        &[
            "write_buffer_size",
            "max_write_buffer_number",
            "level_zero_slowdown_writes_trigger",
            "level_zero_stop_writes_trigger",
            "level_zero_file_num_compaction_trigger",
            "parallelism",
            "max_background_jobs",
            "max_subcompactions",
            "target_file_size_base",
            "block_cache_size",
            "write_buffer_manager_size",
            "max_open_files",
            "block_size",
            "bloom_filter_bits_per_key",
            "compression_type",
        ]
    }

    /// Parses a list of `KEY=VALUE` strings into typed tuning options.
    ///
    /// Unknown keys, malformed entries, values that fail to parse, and
    /// format-sensitive keys all produce a descriptive error.
    pub fn from_kv_pairs<I, S>(pairs: I) -> Result<Self, RocksDbStoreInternalError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut options = Self::default();
        for pair in pairs {
            let pair = pair.as_ref();
            let (key, value) = pair.split_once('=').ok_or_else(|| {
                RocksDbStoreInternalError::InvalidTuningOption(format!(
                    "expected `KEY=VALUE`, got `{pair}`"
                ))
            })?;
            let key = key.trim();
            let value = value.trim();
            if Self::FORMAT_SENSITIVE_KEYS.contains(&key) {
                return Err(RocksDbStoreInternalError::InvalidTuningOption(format!(
                    "`{key}` affects the on-disk storage format and cannot be changed at runtime"
                )));
            }
            match key {
                "write_buffer_size" => {
                    options.write_buffer_size = Some(parse_tuning_value(key, value)?)
                }
                "max_write_buffer_number" => {
                    options.max_write_buffer_number = Some(parse_tuning_value(key, value)?)
                }
                "level_zero_slowdown_writes_trigger" => {
                    options.level_zero_slowdown_writes_trigger =
                        Some(parse_tuning_value(key, value)?)
                }
                "level_zero_stop_writes_trigger" => {
                    options.level_zero_stop_writes_trigger = Some(parse_tuning_value(key, value)?)
                }
                "level_zero_file_num_compaction_trigger" => {
                    options.level_zero_file_num_compaction_trigger =
                        Some(parse_tuning_value(key, value)?)
                }
                "parallelism" => options.parallelism = Some(parse_tuning_value(key, value)?),
                "max_background_jobs" => {
                    options.max_background_jobs = Some(parse_tuning_value(key, value)?)
                }
                "max_subcompactions" => {
                    options.max_subcompactions = Some(parse_tuning_value(key, value)?)
                }
                "target_file_size_base" => {
                    options.target_file_size_base = Some(parse_tuning_value(key, value)?)
                }
                "block_cache_size" => {
                    options.block_cache_size = Some(parse_tuning_value(key, value)?)
                }
                "write_buffer_manager_size" => {
                    options.write_buffer_manager_size = Some(parse_tuning_value(key, value)?)
                }
                "max_open_files" => options.max_open_files = Some(parse_tuning_value(key, value)?),
                "block_size" => options.block_size = Some(parse_tuning_value(key, value)?),
                "bloom_filter_bits_per_key" => {
                    options.bloom_filter_bits_per_key = Some(parse_tuning_value(key, value)?)
                }
                "compression_type" => {
                    options.compression_type =
                        Some(RocksDbCompressionType::from_token(value).ok_or_else(|| {
                            RocksDbStoreInternalError::InvalidTuningOption(format!(
                                "unknown compression type `{value}`; \
                                 expected one of none, snappy, lz4, lz4hc, zstd, zlib, bz2"
                            ))
                        })?)
                }
                _ => {
                    return Err(RocksDbStoreInternalError::InvalidTuningOption(format!(
                        "unknown option `{key}`; supported options are: {}",
                        Self::supported_keys().join(", ")
                    )))
                }
            }
        }
        Ok(options)
    }
}

fn parse_tuning_value<T>(key: &str, value: &str) -> Result<T, RocksDbStoreInternalError>
where
    T: std::str::FromStr,
    T::Err: Display,
{
    value.parse::<T>().map_err(|err| {
        RocksDbStoreInternalError::InvalidTuningOption(format!(
            "invalid value `{value}` for option `{key}`: {err}"
        ))
    })
}

impl RocksDbDatabaseInternal {
    fn check_namespace(namespace: &str) -> Result<(), RocksDbStoreInternalError> {
        if !namespace
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(RocksDbStoreInternalError::InvalidNamespace);
        }
        Ok(())
    }

    fn build(
        config: &RocksDbStoreInternalConfig,
        namespace: &str,
    ) -> Result<RocksDbDatabaseInternal, RocksDbStoreInternalError> {
        let start_key = ROOT_KEY_DOMAIN.to_vec();
        // Create a store to extract its executor and configuration
        let temp_store = RocksDbStoreInternal::build(config, namespace, start_key)?;
        Ok(RocksDbDatabaseInternal {
            executor: temp_store.executor,
            path_with_guard: temp_store.path_with_guard,
            max_stream_queries: temp_store.max_stream_queries,
            spawn_mode: temp_store.spawn_mode,
        })
    }
}

impl RocksDbStoreInternal {
    fn build(
        config: &RocksDbStoreInternalConfig,
        namespace: &str,
        start_key: Vec<u8>,
    ) -> Result<RocksDbStoreInternal, RocksDbStoreInternalError> {
        RocksDbDatabaseInternal::check_namespace(namespace)?;
        let mut path_buf = config.path_with_guard.path_buf.clone();
        let mut path_with_guard = config.path_with_guard.clone();
        path_buf.push(namespace);
        path_with_guard.path_buf = path_buf.clone();
        let max_stream_queries = config.max_stream_queries;
        let spawn_mode = config.spawn_mode;
        if !std::path::Path::exists(&path_buf) {
            std::fs::create_dir_all(path_buf.clone())?;
        }
        let sys = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()),
        );
        let num_cpus = get_available_cpus();
        let total_ram = get_available_memory(&sys);
        // Runtime overrides for the defaults below. `None` keeps the default.
        let tuning = &config.tuning_options;

        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Flush in-memory buffer to disk more often
        let write_buffer_size = tuning.write_buffer_size.unwrap_or(WRITE_BUFFER_SIZE);
        options.set_write_buffer_size(write_buffer_size);
        options.set_max_write_buffer_number(
            tuning
                .max_write_buffer_number
                .unwrap_or(MAX_WRITE_BUFFER_NUMBER),
        );
        options.set_compression_type(
            tuning
                .compression_type
                .map_or(rocksdb::DBCompressionType::Lz4, |c| c.to_rocksdb()),
        );
        options.set_level_zero_slowdown_writes_trigger(
            tuning.level_zero_slowdown_writes_trigger.unwrap_or(8),
        );
        options.set_level_zero_stop_writes_trigger(
            tuning.level_zero_stop_writes_trigger.unwrap_or(12),
        );
        options.set_level_zero_file_num_compaction_trigger(
            tuning.level_zero_file_num_compaction_trigger.unwrap_or(2),
        );
        // We deliberately give RocksDB one background thread *per* CPU so that
        // flush + (N-1) compactions can hammer the NVMe at full bandwidth while
        // still leaving enough CPU time for the foreground application threads.
        options.increase_parallelism(tuning.parallelism.unwrap_or(num_cpus));
        options.set_max_background_jobs(tuning.max_background_jobs.unwrap_or(num_cpus));
        options.set_max_subcompactions(tuning.max_subcompactions.unwrap_or(num_cpus as u32));
        options.set_level_compaction_dynamic_level_bytes(true);

        options.set_compaction_style(DBCompactionStyle::Level);
        options.set_target_file_size_base(
            tuning
                .target_file_size_base
                .unwrap_or(2 * write_buffer_size as u64),
        );
        // By default RocksDB keeps every file open (`-1`); only override if asked.
        if let Some(max_open_files) = tuning.max_open_files {
            options.set_max_open_files(max_open_files);
        }

        let mut block_options = BlockBasedOptions::default();
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_options.set_cache_index_and_filter_blocks(true);
        // Allocate 1/4 of total RAM for RocksDB block cache, which is a reasonable balance:
        // - Large enough to significantly improve read performance by caching frequently accessed blocks
        // - Small enough to leave memory for other system components
        // - Follows common practice for database caching in server environments
        // - Prevents excessive memory pressure that could lead to swapping or OOM conditions
        let block_cache_size = tuning.block_cache_size.unwrap_or(total_ram / 4);
        block_options.set_block_cache(&Cache::new_hyper_clock_cache(
            block_cache_size,
            HYPER_CLOCK_CACHE_BLOCK_SIZE,
        ));

        // Cap total memtable memory to prevent unbounded growth when multiple column
        // families are used or many memtables accumulate before flushing.
        let write_buffer_manager_size = tuning.write_buffer_manager_size.unwrap_or(total_ram / 4);
        let write_buffer_manager =
            WriteBufferManager::new_write_buffer_manager(write_buffer_manager_size, true);
        options.set_write_buffer_manager(&write_buffer_manager);

        // Configure bloom filters for prefix iteration optimization
        block_options.set_bloom_filter(tuning.bloom_filter_bits_per_key.unwrap_or(10.0), false);
        block_options.set_whole_key_filtering(false);

        // 32KB blocks instead of default 4KB - reduces iterator seeks
        block_options.set_block_size(tuning.block_size.unwrap_or(32 * 1024));
        // Use latest format for better compression and performance
        block_options.set_format_version(5);

        options.set_block_based_table_factory(&block_options);

        // Configure prefix extraction for bloom filter optimization
        // Use 8 bytes: ROOT_KEY_DOMAIN (1 byte) + BCS variant (1-2 bytes) + identifier start (4-5 bytes)
        let prefix_extractor = SliceTransform::create_fixed_prefix(8);
        options.set_prefix_extractor(prefix_extractor);

        // 12.5% of memtable size for bloom filter
        options.set_memtable_prefix_bloom_ratio(0.125);
        // Skip bloom filter for memtable when key exists
        options.set_optimize_filters_for_hits(true);
        // Use memory-mapped files for faster reads
        options.set_allow_mmap_reads(true);
        // Don't use random access pattern since we do prefix scans
        options.set_advise_random_on_open(false);

        let db = DB::open(&options, path_buf)?;
        let executor = RocksDbStoreExecutor {
            db: Arc::new(db),
            start_key,
        };
        Ok(RocksDbStoreInternal {
            executor,
            path_with_guard,
            max_stream_queries,
            spawn_mode,
            root_key_written: Arc::new(AtomicBool::new(false)),
        })
    }
}

impl WithError for RocksDbStoreInternal {
    type Error = RocksDbStoreInternalError;
}

impl ReadableKeyValueStore for RocksDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    fn root_key(&self) -> Result<Vec<u8>, RocksDbStoreInternalError> {
        assert!(self.executor.start_key.starts_with(&ROOT_KEY_DOMAIN));
        let root_key = bcs::from_bytes(&self.executor.start_key[ROOT_KEY_DOMAIN.len()..])?;
        Ok(root_key)
    }

    async fn read_value_bytes(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, RocksDbStoreInternalError> {
        check_key_size(key)?;
        let db = self.executor.db.clone();
        let mut full_key = self.executor.start_key.to_vec();
        full_key.extend(key);
        self.spawn_mode
            .spawn(move |x| Ok(db.get(&x)?), full_key)
            .await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, RocksDbStoreInternalError> {
        check_key_size(key)?;
        let db = self.executor.db.clone();
        let mut full_key = self.executor.start_key.to_vec();
        full_key.extend(key);
        self.spawn_mode
            .spawn(
                move |x| {
                    if !db.key_may_exist(&x) {
                        return Ok(false);
                    }
                    Ok(db.get(&x)?.is_some())
                },
                full_key,
            )
            .await
    }

    async fn contains_keys(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<bool>, RocksDbStoreInternalError> {
        let executor = self.executor.clone();
        self.spawn_mode
            .spawn(move |x| executor.contains_keys_internal(x), keys.to_vec())
            .await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbStoreInternalError> {
        let executor = self.executor.clone();
        self.spawn_mode
            .spawn(
                move |x| executor.read_multi_values_bytes_internal(x),
                keys.to_vec(),
            )
            .await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksDbStoreInternalError> {
        let executor = self.executor.clone();
        let key_prefix = key_prefix.to_vec();
        self.spawn_mode
            .spawn(
                move |x| executor.find_keys_by_prefix_internal(x),
                key_prefix,
            )
            .await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksDbStoreInternalError> {
        let executor = self.executor.clone();
        let key_prefix = key_prefix.to_vec();
        self.spawn_mode
            .spawn(
                move |x| executor.find_key_values_by_prefix_internal(x),
                key_prefix,
            )
            .await
    }
}

impl WritableKeyValueStore for RocksDbStoreInternal {
    const MAX_VALUE_SIZE: usize = MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), RocksDbStoreInternalError> {
        let write_root_key = !self.root_key_written.fetch_or(true, Ordering::SeqCst);
        let executor = self.executor.clone();
        self.spawn_mode
            .spawn(
                move |x| executor.write_batch_internal(x, write_root_key),
                batch,
            )
            .await
    }

    async fn clear_journal(&self) -> Result<(), RocksDbStoreInternalError> {
        Ok(())
    }
}

impl KeyValueDatabase for RocksDbDatabaseInternal {
    type Config = RocksDbStoreInternalConfig;
    type Store = RocksDbStoreInternal;

    fn get_name() -> String {
        "rocksdb internal".to_string()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Self, RocksDbStoreInternalError> {
        Self::build(config, namespace)
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, RocksDbStoreInternalError> {
        let mut start_key = ROOT_KEY_DOMAIN.to_vec();
        start_key.extend(bcs::to_bytes(root_key)?);
        let mut executor = self.executor.clone();
        executor.start_key = start_key;
        Ok(RocksDbStoreInternal {
            executor,
            path_with_guard: self.path_with_guard.clone(),
            max_stream_queries: self.max_stream_queries,
            spawn_mode: self.spawn_mode,
            root_key_written: Arc::new(AtomicBool::new(false)),
        })
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, RocksDbStoreInternalError> {
        self.open_shared(root_key)
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, RocksDbStoreInternalError> {
        let entries = std::fs::read_dir(config.path_with_guard.path_buf.clone())?;
        let mut namespaces = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                return Err(RocksDbStoreInternalError::NonDirectoryNamespace);
            }
            let namespace = match entry.file_name().into_string() {
                Err(error) => {
                    return Err(RocksDbStoreInternalError::IntoStringError(error));
                }
                Ok(namespace) => namespace,
            };
            namespaces.push(namespace);
        }
        Ok(namespaces)
    }

    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>, RocksDbStoreInternalError> {
        let mut store = self.open_shared(&[])?;
        store.executor.start_key = vec![STORED_ROOT_KEYS_PREFIX];
        let bcs_root_keys = store.find_keys_by_prefix(&[]).await?;
        let mut root_keys = Vec::new();
        for bcs_root_key in bcs_root_keys {
            let root_key = bcs::from_bytes::<Vec<u8>>(&bcs_root_key)?;
            root_keys.push(root_key);
        }
        Ok(root_keys)
    }

    async fn delete_all(config: &Self::Config) -> Result<(), RocksDbStoreInternalError> {
        let namespaces = Self::list_all(config).await?;
        for namespace in namespaces {
            let mut path_buf = config.path_with_guard.path_buf.clone();
            path_buf.push(&namespace);
            std::fs::remove_dir_all(path_buf.as_path())?;
        }
        Ok(())
    }

    async fn exists(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<bool, RocksDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_with_guard.path_buf.clone();
        path_buf.push(namespace);
        let test = std::path::Path::exists(&path_buf);
        Ok(test)
    }

    async fn create(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), RocksDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_with_guard.path_buf.clone();
        path_buf.push(namespace);
        if std::path::Path::exists(&path_buf) {
            return Err(RocksDbStoreInternalError::StoreAlreadyExists);
        }
        std::fs::create_dir_all(path_buf)?;
        Ok(())
    }

    async fn delete(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), RocksDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_with_guard.path_buf.clone();
        path_buf.push(namespace);
        let path = path_buf.as_path();
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}

#[cfg(with_testing)]
impl TestKeyValueDatabase for RocksDbDatabaseInternal {
    async fn new_test_config() -> Result<RocksDbStoreInternalConfig, RocksDbStoreInternalError> {
        let path_with_guard = PathWithGuard::new_testing();
        let spawn_mode = RocksDbSpawnMode::get_spawn_mode_from_runtime();
        let max_stream_queries = TEST_ROCKS_DB_MAX_STREAM_QUERIES;
        Ok(RocksDbStoreInternalConfig {
            path_with_guard,
            spawn_mode,
            max_stream_queries,
            tuning_options: RocksDbTuningOptions::default(),
        })
    }
}

/// The error type for [`RocksDbStoreInternal`]
#[derive(Error, Debug)]
pub enum RocksDbStoreInternalError {
    /// Store already exists
    #[error("Store already exists")]
    StoreAlreadyExists,

    /// Tokio join error in RocksDB.
    #[error("tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    /// RocksDB error.
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    /// The database contains a file which is not a directory
    #[error("Namespaces should be directories")]
    NonDirectoryNamespace,

    /// Error converting `OsString` to `String`
    #[error("error in the conversion from OsString: {0:?}")]
    IntoStringError(OsString),

    /// The key must have at most 8 MiB
    #[error("The key must have at most 8 MiB")]
    KeyTooLong,

    /// Namespace contains forbidden characters
    #[error("Namespace contains forbidden characters")]
    InvalidNamespace,

    /// A RocksDB tuning option could not be parsed.
    #[error("invalid RocksDB tuning option: {0}")]
    InvalidTuningOption(String),

    /// Filesystem error
    #[error("Filesystem error: {0}")]
    FsError(#[from] std::io::Error),

    /// BCS serialization error.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),
}

/// A path and the guard for the temporary directory if needed
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PathWithGuard {
    /// The path to the data
    pub path_buf: PathBuf,
    /// The guard for the directory if one is needed
    #[serde(skip)]
    _dir_guard: Option<Arc<TempDir>>,
}

impl PathWithGuard {
    /// Creates a `PathWithGuard` from an existing path.
    pub fn new(path_buf: PathBuf) -> Self {
        Self {
            path_buf,
            _dir_guard: None,
        }
    }

    /// Returns the test path for RocksDB without common config.
    #[cfg(with_testing)]
    fn new_testing() -> PathWithGuard {
        let dir = TempDir::new().unwrap();
        let path_buf = dir.path().to_path_buf();
        let dir_guard = Some(Arc::new(dir));
        PathWithGuard {
            path_buf,
            _dir_guard: dir_guard,
        }
    }
}

impl PartialEq for PathWithGuard {
    fn eq(&self, other: &Self) -> bool {
        self.path_buf == other.path_buf
    }
}
impl Eq for PathWithGuard {}

impl KeyValueStoreError for RocksDbStoreInternalError {
    const BACKEND: &'static str = "rocks_db";
}

/// The composed error type for the `RocksDbStore`
pub type RocksDbStoreError = ValueSplittingError<RocksDbStoreInternalError>;

/// The composed config type for the `RocksDbStore`
pub type RocksDbStoreConfig = LruCachingConfig<RocksDbStoreInternalConfig>;

/// The `RocksDbDatabase` composed type with metrics
#[cfg(with_metrics)]
pub type RocksDbDatabase = MeteredDatabase<
    LruCachingDatabase<
        MeteredDatabase<ValueSplittingDatabase<MeteredDatabase<RocksDbDatabaseInternal>>>,
    >,
>;
/// The `RocksDbDatabase` composed type
#[cfg(not(with_metrics))]
pub type RocksDbDatabase = LruCachingDatabase<ValueSplittingDatabase<RocksDbDatabaseInternal>>;

#[cfg(with_testing)]
impl crate::backends::DatabaseBackup for RocksDbDatabaseInternal {
    fn backup_to(&self, dir: &std::path::Path) -> anyhow::Result<()> {
        use rocksdb::{
            backup::{BackupEngine, BackupEngineOptions},
            Env,
        };
        let opts = BackupEngineOptions::new(dir)?;
        let env = Env::new()?;
        let mut engine = BackupEngine::open(&opts, &env)?;
        engine.create_new_backup_flush(&*self.executor.db, true)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{
        PathWithGuard, RocksDbCompressionType, RocksDbSpawnMode, RocksDbStoreInternal,
        RocksDbStoreInternalConfig, RocksDbTuningOptions, ROOT_KEY_DOMAIN,
    };

    #[test]
    fn parses_typed_values() {
        let options = RocksDbTuningOptions::from_kv_pairs([
            "write_buffer_size=268435456",
            "max_open_files=512",
            "bloom_filter_bits_per_key=12.5",
            "compression_type=zstd",
        ])
        .unwrap();
        assert_eq!(options.write_buffer_size, Some(268435456));
        assert_eq!(options.max_open_files, Some(512));
        assert_eq!(options.bloom_filter_bits_per_key, Some(12.5));
        assert_eq!(options.compression_type, Some(RocksDbCompressionType::Zstd));
        // Untouched options stay at their defaults.
        assert_eq!(options.max_background_jobs, None);
    }

    #[test]
    fn empty_input_is_all_defaults() {
        let options = RocksDbTuningOptions::from_kv_pairs(Vec::<String>::new()).unwrap();
        assert!(options.write_buffer_size.is_none());
        assert!(options.compression_type.is_none());
    }

    #[test]
    fn trims_whitespace_around_key_and_value() {
        let options = RocksDbTuningOptions::from_kv_pairs([" write_buffer_size = 1024 "]).unwrap();
        assert_eq!(options.write_buffer_size, Some(1024));
    }

    #[test]
    fn rejects_unknown_key() {
        let error = RocksDbTuningOptions::from_kv_pairs(["not_a_real_option=1"]).unwrap_err();
        assert!(error.to_string().contains("unknown option"));
    }

    #[test]
    fn rejects_format_sensitive_key() {
        for key in [
            "prefix_extractor=8",
            "format_version=6",
            "compaction_style=universal",
        ] {
            let error = RocksDbTuningOptions::from_kv_pairs([key]).unwrap_err();
            assert!(
                error.to_string().contains("storage format"),
                "unexpected error for {key}: {error}"
            );
        }
    }

    #[test]
    fn rejects_malformed_pair() {
        let error = RocksDbTuningOptions::from_kv_pairs(["write_buffer_size"]).unwrap_err();
        assert!(error.to_string().contains("KEY=VALUE"));
    }

    #[test]
    fn rejects_unparseable_value() {
        let error =
            RocksDbTuningOptions::from_kv_pairs(["write_buffer_size=not_a_number"]).unwrap_err();
        assert!(error.to_string().contains("invalid value"));
    }

    #[test]
    fn rejects_unknown_compression() {
        let error = RocksDbTuningOptions::from_kv_pairs(["compression_type=gzip"]).unwrap_err();
        assert!(error.to_string().contains("unknown compression type"));
    }

    /// Opens a real RocksDB store with overridden options to confirm they are
    /// accepted by RocksDB (in particular the conditionally-set `max_open_files`
    /// and the cache-size paths).
    #[test]
    fn build_applies_overrides() {
        let dir = TempDir::new().unwrap();
        let config = RocksDbStoreInternalConfig {
            path_with_guard: PathWithGuard::new(dir.path().to_path_buf()),
            spawn_mode: RocksDbSpawnMode::SpawnBlocking,
            max_stream_queries: 10,
            tuning_options: RocksDbTuningOptions::from_kv_pairs([
                "write_buffer_size=1048576",
                "max_open_files=128",
                "compression_type=none",
                "block_cache_size=8388608",
                "write_buffer_manager_size=8388608",
                "max_background_jobs=2",
            ])
            .unwrap(),
        };
        let store = RocksDbStoreInternal::build(&config, "test_ns", ROOT_KEY_DOMAIN.to_vec());
        assert!(store.is_ok(), "build failed: {:?}", store.err());
    }
}
