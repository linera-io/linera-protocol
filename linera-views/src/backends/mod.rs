// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod journaling;

#[cfg(with_metrics)]
pub mod metering;

pub mod value_splitting;

pub mod memory;

pub mod lru_caching;

pub mod dual;

#[cfg(with_scylladb)]
mod scylla_db;

#[cfg(with_rocksdb)]
mod rocks_db;

#[cfg(with_dynamodb)]
mod dynamo_db;

#[cfg(with_indexeddb)]
pub mod indexed_db;

/// The `RocksDbStore` composed type with metrics
#[cfg(all(with_rocksdb, with_metrics))]
pub type RocksDbStore = crate::metering::MeteredStore<
    crate::lru_caching::LruCachingStore<
        crate::metering::MeteredStore<
            crate::value_splitting::ValueSplittingStore<
                crate::metering::MeteredStore<crate::backends::rocks_db::RocksDbStoreInternal>,
            >,
        >,
    >,
>;

/// The `RocksDbStore` composed type
#[cfg(all(with_rocksdb, not(with_metrics)))]
pub type RocksDbStore = crate::lru_caching::LruCachingStore<
    crate::value_splitting::ValueSplittingStore<crate::backends::rocks_db::RocksDbStoreInternal>,
>;

/// The composed error type for the `RocksDbStore`
#[cfg(with_rocksdb)]
pub type RocksDbStoreError = crate::value_splitting::ValueSplittingError<
    crate::backends::rocks_db::RocksDbStoreInternalError,
>;

/// The composed config type for the `RocksDbStore`
#[cfg(with_rocksdb)]
pub type RocksDbStoreConfig =
    crate::lru_caching::LruSplittingConfig<crate::backends::rocks_db::RocksDbStoreInternalConfig>;

/// A shared DB client for DynamoDb implementing LruCaching and metrics
#[cfg(all(with_dynamodb, with_metrics))]
pub type DynamoDbStore = crate::metering::MeteredStore<
    crate::lru_caching::LruCachingStore<
        crate::metering::MeteredStore<
            crate::value_splitting::ValueSplittingStore<
                crate::metering::MeteredStore<
                    crate::journaling::JournalingKeyValueStore<
                        crate::backends::dynamo_db::DynamoDbStoreInternal,
                    >,
                >,
            >,
        >,
    >,
>;

/// A shared DB client for DynamoDb implementing LruCaching
#[cfg(all(with_dynamodb, not(with_metrics)))]
pub type DynamoDbStore = crate::lru_caching::LruCachingStore<
    crate::value_splitting::ValueSplittingStore<
        crate::journaling::JournalingKeyValueStore<
            crate::backends::dynamo_db::DynamoDbStoreInternal,
        >,
    >,
>;

/// The combined error type for the `DynamoDbStore`.
#[cfg(with_dynamodb)]
pub type DynamoDbStoreError = crate::value_splitting::ValueSplittingError<
    crate::backends::dynamo_db::DynamoDbStoreInternalError,
>;

/// The config type for DynamoDbStore
#[cfg(with_dynamodb)]
pub type DynamoDbStoreConfig =
    crate::lru_caching::LruSplittingConfig<crate::backends::dynamo_db::DynamoDbStoreInternalConfig>;

/// Getting a configuration for the system
#[cfg(with_dynamodb)]
pub async fn get_config(
    use_localstack: bool,
) -> Result<aws_sdk_dynamodb::Config, DynamoDbStoreError> {
    Ok(crate::backends::dynamo_db::get_config_internal(use_localstack).await?)
}

/// The `ScyllaDbStore` composed type with metrics
#[cfg(all(with_scylladb, with_metrics))]
pub type ScyllaDbStore = crate::metering::MeteredStore<
    crate::lru_caching::LruCachingStore<
        crate::metering::MeteredStore<
            crate::value_splitting::ValueSplittingStore<
                crate::metering::MeteredStore<
                    crate::journaling::JournalingKeyValueStore<
                        crate::backends::scylla_db::ScyllaDbStoreInternal,
                    >,
                >,
            >,
        >,
    >,
>;

/// The `ScyllaDbStore` composed type
#[cfg(all(with_scylladb, not(with_metrics)))]
pub type ScyllaDbStore = crate::lru_caching::LruCachingStore<
    crate::value_splitting::ValueSplittingStore<
        crate::journaling::JournalingKeyValueStore<
            crate::backends::scylla_db::ScyllaDbStoreInternal,
        >,
    >,
>;

/// The `ScyllaDbStoreConfig` input type
#[cfg(with_scylladb)]
pub type ScyllaDbStoreConfig =
    crate::lru_caching::LruSplittingConfig<crate::backends::scylla_db::ScyllaDbStoreInternalConfig>;

/// The combined error type for the `ScyllaDbStore`.
#[cfg(with_scylladb)]
pub type ScyllaDbStoreError = crate::value_splitting::ValueSplittingError<
    crate::backends::scylla_db::ScyllaDbStoreInternalError,
>;
