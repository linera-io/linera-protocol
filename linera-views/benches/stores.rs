// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(with_dynamodb)]
use linera_views::dynamo_db::create_dynamo_db_test_store;
#[cfg(with_rocksdb)]
use linera_views::rocks_db::create_rocks_db_test_store;
#[cfg(with_scylladb)]
use linera_views::scylla_db::create_scylla_db_test_store;
use linera_views::{
    batch::Batch,
    common::LocalKeyValueStore,
    memory::create_memory_store,
    test_utils::{add_prefix, get_random_key_values2},
};
use tokio::runtime::Runtime;

// We generate about 200 keys of length 4 with a key of length 10000
// The keys are of the form 0,x,y,z,t with 0<= x,y,z,t < 4.

/// A value to use for the keys
const PREFIX: &[u8] = &[0];

/// A value to use for the keys
const PREFIX_SEARCH: &[u8] = &[0];

/// The number of keys length
const NUM_ENTRIES: usize = 200;

/// The length of the values
const LEN_VALUE: usize = 10000;

pub async fn performance_contains_key<S: LocalKeyValueStore>(store: S, iterations: u64) -> Duration
where
    S::Error: Debug,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(PREFIX, get_random_key_values2(NUM_ENTRIES, LEN_VALUE));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch, &[]).await.unwrap();

        let measurement = Instant::now();
        for key_value in &key_values {
            black_box(store.contains_key(&key_value.0).await.unwrap());
        }
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(PREFIX.to_vec());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_contains_key(criterion: &mut Criterion) {
    criterion.bench_function("memory_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_contains_key(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_contains_key(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_contains_key(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_contains_key(store, iterations).await
            })
    });
}

pub async fn performance_contains_keys<S: LocalKeyValueStore>(store: S, iterations: u64) -> Duration
where
    S::Error: Debug,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(PREFIX, get_random_key_values2(NUM_ENTRIES, LEN_VALUE));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch, &[]).await.unwrap();
        let keys = key_values
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        let measurement = Instant::now();
        black_box(store.contains_keys(keys).await.unwrap());
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(PREFIX.to_vec());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_contains_keys(criterion: &mut Criterion) {
    criterion.bench_function("memory_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_contains_keys(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_contains_keys(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_contains_keys(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_contains_keys(store, iterations).await
            })
    });
}

pub async fn performance_find_keys_by_prefix<S: LocalKeyValueStore>(
    store: S,
    iterations: u64,
) -> Duration
where
    S::Error: Debug,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(PREFIX, get_random_key_values2(NUM_ENTRIES, LEN_VALUE));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch, &[]).await.unwrap();

        let measurement = Instant::now();
        black_box(store.find_keys_by_prefix(PREFIX_SEARCH).await.unwrap());
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(PREFIX.to_vec());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_find_keys_by_prefix(criterion: &mut Criterion) {
    criterion.bench_function("memory_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_find_keys_by_prefix(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_find_keys_by_prefix(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_find_keys_by_prefix(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_find_keys_by_prefix(store, iterations).await
            })
    });
}

pub async fn performance_find_key_values_by_prefix<S: LocalKeyValueStore>(
    store: S,
    iterations: u64,
) -> Duration
where
    S::Error: Debug,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(PREFIX, get_random_key_values2(NUM_ENTRIES, LEN_VALUE));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch, &[]).await.unwrap();

        let measurement = Instant::now();
        black_box(store.find_key_values_by_prefix(PREFIX_SEARCH).await.unwrap());
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(PREFIX.to_vec());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_find_key_values_by_prefix(criterion: &mut Criterion) {
    criterion.bench_function("memory_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_find_key_values_by_prefix(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_find_key_values_by_prefix(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_find_key_values_by_prefix(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_find_key_values_by_prefix(store, iterations).await
            })
    });
}

pub async fn performance_read_value_bytes<S: LocalKeyValueStore>(
    store: S,
    iterations: u64,
) -> Duration
where
    S::Error: Debug,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(PREFIX, get_random_key_values2(NUM_ENTRIES, LEN_VALUE));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch, &[]).await.unwrap();

        let measurement = Instant::now();
        for (key, _) in &key_values {
            black_box(store.read_value_bytes(key).await.unwrap());
        }
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(PREFIX.to_vec());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_read_value_bytes(criterion: &mut Criterion) {
    criterion.bench_function("memory_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_read_value_bytes(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_read_value_bytes(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_read_value_bytes(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_read_value_bytes(store, iterations).await
            })
    });
}

pub async fn performance_read_multi_values_bytes<S: LocalKeyValueStore>(
    store: S,
    iterations: u64,
) -> Duration
where
    S::Error: Debug,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(PREFIX, get_random_key_values2(NUM_ENTRIES, LEN_VALUE));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch, &[]).await.unwrap();
        let keys = key_values
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();

        let measurement = Instant::now();
        black_box(store.read_multi_values_bytes(keys).await.unwrap());
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(PREFIX.to_vec());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_read_multi_values_bytes(criterion: &mut Criterion) {
    criterion.bench_function("memory_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_read_multi_values_bytes(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_read_multi_values_bytes(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_read_multi_values_bytes(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_read_multi_values_bytes(store, iterations).await
            })
    });
}

pub async fn performance_write_batch<S: LocalKeyValueStore>(store: S, iterations: u64) -> Duration
where
    S::Error: Debug,
{
    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = add_prefix(PREFIX, get_random_key_values2(NUM_ENTRIES, LEN_VALUE));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }

        let measurement = Instant::now();
        store.write_batch(batch, &[]).await.unwrap();
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(PREFIX.to_vec());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_write_batch(criterion: &mut Criterion) {
    criterion.bench_function("memory_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_write_batch(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_write_batch(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_write_batch(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_write_batch(store, iterations).await
            })
    });
}

criterion_group!(
    benches,
    bench_contains_key,
    bench_contains_keys,
    bench_find_keys_by_prefix,
    bench_find_key_values_by_prefix,
    bench_read_value_bytes,
    bench_read_multi_values_bytes,
    bench_write_batch
);
criterion_main!(benches);
