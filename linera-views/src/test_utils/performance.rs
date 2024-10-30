// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

use crate::{
    batch::Batch,
    store::LocalKeyValueStore,
    test_utils::{add_prefix, get_random_key_values2},
};

// We generate about 2000 keys of length 11 with a key of length 10000
// The keys are of the form 0,x_1, ..., x_n with 0 <= x_i < 4 and n=10.

/// A value to use for the keys
const PREFIX: &[u8] = &[0];

/// A value to use for the keys
const PREFIX_SEARCH: &[u8] = &[0, 0];

/// The number of keys
const NUM_ENTRIES: usize = 200;

/// The number of inserted keys
const NUM_INSERT: usize = 70;

/// The length of the keys
const LEN_KEY: usize = 10;

/// The length of the values
const LEN_VALUE: usize = 10000;

async fn clear_store<S: LocalKeyValueStore>(store: &S) {
    let mut batch = Batch::new();
    batch.delete_key_prefix(PREFIX.to_vec());
    store.write_batch(batch).await.unwrap();
}

/// Compute benchmark of the `contains_key` operation.
pub async fn performance_contains_key<S: LocalKeyValueStore, F>(
    store: S,
    iterations: u64,
    f: F,
) -> Duration
where
    S::Error: Debug,
    F: Fn(bool) -> bool,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(
            PREFIX,
            get_random_key_values2(NUM_ENTRIES, LEN_KEY, LEN_VALUE),
        );
        let mut batch = Batch::new();
        for key_value in &key_values[..NUM_INSERT] {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch).await.unwrap();

        let measurement = Instant::now();
        for key_value in &key_values {
            f(store.contains_key(&key_value.0).await.unwrap());
        }
        total_time += measurement.elapsed();

        clear_store(&store).await;
    }

    total_time
}

/// Compute benchmark of the `contains_keys` operation.
pub async fn performance_contains_keys<S: LocalKeyValueStore, F>(
    store: S,
    iterations: u64,
    f: F,
) -> Duration
where
    S::Error: Debug,
    F: Fn(Vec<bool>) -> Vec<bool>,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(
            PREFIX,
            get_random_key_values2(NUM_ENTRIES, LEN_KEY, LEN_VALUE),
        );
        let mut batch = Batch::new();
        for key_value in &key_values[..NUM_INSERT] {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch).await.unwrap();
        let keys = key_values
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        let measurement = Instant::now();
        f(store.contains_keys(keys).await.unwrap());
        total_time += measurement.elapsed();

        clear_store(&store).await;
    }

    total_time
}

/// Compute benchmark of the `performance_find_keys_by_prefix` operation.
pub async fn performance_find_keys_by_prefix<S: LocalKeyValueStore, F>(
    store: S,
    iterations: u64,
    f: F,
) -> Duration
where
    S::Error: Debug,
    F: Fn(S::Keys) -> S::Keys,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(
            PREFIX,
            get_random_key_values2(NUM_ENTRIES, LEN_KEY, LEN_VALUE),
        );
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch).await.unwrap();

        let measurement = Instant::now();
        f(store.find_keys_by_prefix(PREFIX_SEARCH).await.unwrap());
        total_time += measurement.elapsed();

        clear_store(&store).await;
    }

    total_time
}

/// Compute benchmark of the `performance_find_keys_by_prefix` operation.
pub async fn performance_find_key_values_by_prefix<S: LocalKeyValueStore, F>(
    store: S,
    iterations: u64,
    f: F,
) -> Duration
where
    S::Error: Debug,
    F: Fn(S::KeyValues) -> S::KeyValues,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(
            PREFIX,
            get_random_key_values2(NUM_ENTRIES, LEN_KEY, LEN_VALUE),
        );
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch).await.unwrap();

        let measurement = Instant::now();
        f(store
            .find_key_values_by_prefix(PREFIX_SEARCH)
            .await
            .unwrap());
        total_time += measurement.elapsed();

        clear_store(&store).await;
    }

    total_time
}

/// Compute benchmark of the `read_value_bytes` operation.
pub async fn performance_read_value_bytes<S: LocalKeyValueStore, F>(
    store: S,
    iterations: u64,
    f: F,
) -> Duration
where
    S::Error: Debug,
    F: Fn(Option<Vec<u8>>) -> Option<Vec<u8>>,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(
            PREFIX,
            get_random_key_values2(NUM_ENTRIES, LEN_KEY, LEN_VALUE),
        );
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch).await.unwrap();

        let measurement = Instant::now();
        for (key, _) in &key_values {
            f(store.read_value_bytes(key).await.unwrap());
        }
        total_time += measurement.elapsed();

        clear_store(&store).await;
    }

    total_time
}

/// Compute benchmark of the `read_multi_values_bytes` operation.
pub async fn performance_read_multi_values_bytes<S: LocalKeyValueStore, F>(
    store: S,
    iterations: u64,
    f: F,
) -> Duration
where
    S::Error: Debug,
    F: Fn(Vec<Option<Vec<u8>>>) -> Vec<Option<Vec<u8>>>,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(
            PREFIX,
            get_random_key_values2(NUM_ENTRIES, LEN_KEY, LEN_VALUE),
        );
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch).await.unwrap();
        let keys = key_values
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        let measurement = Instant::now();
        f(store.read_multi_values_bytes(keys).await.unwrap());
        total_time += measurement.elapsed();

        clear_store(&store).await;
    }

    total_time
}

/// Compute benchmark of the `write_batch` operation.
pub async fn performance_write_batch<S: LocalKeyValueStore, F>(
    store: S,
    iterations: u64,
    f: F,
) -> Duration
where
    S::Error: Debug,
    F: Fn(()) -> (),
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(
            PREFIX,
            get_random_key_values2(NUM_ENTRIES, LEN_KEY, LEN_VALUE),
        );
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }

        let measurement = Instant::now();
        f(store.write_batch(batch).await.unwrap());
        total_time += measurement.elapsed();

        clear_store(&store).await;
    }

    total_time
}
