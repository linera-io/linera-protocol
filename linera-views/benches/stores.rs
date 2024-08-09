// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(with_dynamodb)]
use linera_views::dynamo_db::{create_dynamo_db_test_store, DynamoDbStore};
#[cfg(with_rocksdb)]
use linera_views::rocks_db::{create_rocks_db_test_store, RocksDbStore};
#[cfg(with_scylladb)]
use linera_views::scylla_db::{create_scylla_db_test_store, ScyllaDbStore};
use linera_views::{
    batch::Batch,
    common::LocalKeyValueStore,
    memory::{create_memory_store, MemoryStore},
    test_utils::{get_random_key_values2, prefix_expansion},
};
use tokio::runtime::Runtime;

pub async fn performance_contains_key<S: LocalKeyValueStore>(store: S, iterations: u64) -> Duration
where
    S::Error: Debug,
{
    let prefix = vec![0];
    let num_entries = 100;
    let len_value = 10000;

    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = prefix_expansion(&prefix, get_random_key_values2(num_entries, len_value));
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
        batch.delete_key_prefix(prefix.clone());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_contain_key(criterion: &mut Criterion) {
    criterion.bench_function("memory_contain_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_contains_key::<MemoryStore>(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_contain_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_contains_key::<RocksDbStore>(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_contain_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_contains_key::<DynamoDbStore>(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_contain_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_contains_key::<ScyllaDbStore>(store, iterations).await
            })
    });
}

pub async fn performance_contains_keys<S: LocalKeyValueStore>(store: S, iterations: u64) -> Duration
where
    S::Error: Debug,
{
    let prefix = vec![0];
    let num_entries = 100;
    let len_value = 10000;

    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = prefix_expansion(&prefix, get_random_key_values2(num_entries, len_value));
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
        batch.delete_key_prefix(prefix.clone());
        store.write_batch(batch, &[]).await.unwrap();
    }

    total_time
}

fn bench_contain_keys(criterion: &mut Criterion) {
    criterion.bench_function("memory_contain_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_memory_store();
                performance_contains_keys::<MemoryStore>(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_contain_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_contains_keys::<RocksDbStore>(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_contain_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_contains_keys::<DynamoDbStore>(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_contain_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_contains_keys::<ScyllaDbStore>(store, iterations).await
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
    let prefix = vec![0];
    let num_entries = 100;
    let len_value = 10000;

    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = prefix_expansion(&prefix, get_random_key_values2(num_entries, len_value));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch, &[]).await.unwrap();

        let measurement = Instant::now();
        black_box(store.find_keys_by_prefix(&prefix).await.unwrap());
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(prefix.clone());
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
                performance_find_keys_by_prefix::<MemoryStore>(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_find_keys_by_prefix::<RocksDbStore>(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_find_keys_by_prefix::<DynamoDbStore>(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_find_keys_by_prefix::<ScyllaDbStore>(store, iterations).await
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
    let prefix = vec![0];
    let num_entries = 100;
    let len_value = 10000;

    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = prefix_expansion(&prefix, get_random_key_values2(num_entries, len_value));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }
        store.write_batch(batch, &[]).await.unwrap();

        let measurement = Instant::now();
        black_box(store.find_key_values_by_prefix(&prefix).await.unwrap());
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(prefix.clone());
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
                performance_find_key_values_by_prefix::<MemoryStore>(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_find_key_values_by_prefix::<RocksDbStore>(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_find_key_values_by_prefix::<DynamoDbStore>(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_find_key_values_by_prefix::<ScyllaDbStore>(store, iterations).await
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
    let prefix = vec![0];
    let num_entries = 100;
    let len_value = 10000;

    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = prefix_expansion(&prefix, get_random_key_values2(num_entries, len_value));
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
        batch.delete_key_prefix(prefix.clone());
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
                performance_read_value_bytes::<MemoryStore>(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_read_value_bytes::<RocksDbStore>(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_read_value_bytes::<DynamoDbStore>(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_read_value_bytes::<ScyllaDbStore>(store, iterations).await
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
    let prefix = vec![0];
    let num_entries = 100;
    let len_value = 10000;

    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = prefix_expansion(&prefix, get_random_key_values2(num_entries, len_value));
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
        batch.delete_key_prefix(prefix.clone());
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
                performance_read_multi_values_bytes::<MemoryStore>(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_read_multi_values_bytes::<RocksDbStore>(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_read_multi_values_bytes::<DynamoDbStore>(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_read_multi_values_bytes::<ScyllaDbStore>(store, iterations).await
            })
    });
}

pub async fn performance_write_batch<S: LocalKeyValueStore>(store: S, iterations: u64) -> Duration
where
    S::Error: Debug,
{
    let prefix = vec![0];
    let num_entries = 100;
    let len_value = 10000;

    let mut total_time = Duration::ZERO;
    for _ in 0..iterations {
        let key_values = prefix_expansion(&prefix, get_random_key_values2(num_entries, len_value));
        let mut batch = Batch::new();
        for key_value in &key_values {
            batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
        }

        let measurement = Instant::now();
        store.write_batch(batch, &[]).await.unwrap();
        total_time += measurement.elapsed();

        let mut batch = Batch::new();
        batch.delete_key_prefix(prefix.clone());
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
                performance_write_batch::<MemoryStore>(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let (store, _dir) = create_rocks_db_test_store().await;
                performance_write_batch::<RocksDbStore>(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_write_batch::<DynamoDbStore>(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_write_batch::<ScyllaDbStore>(store, iterations).await
            })
    });
}

criterion_group!(
    benches,
    bench_contain_key,
    bench_contain_keys,
    bench_find_keys_by_prefix,
    bench_find_key_values_by_prefix,
    bench_read_value_bytes,
    bench_read_multi_values_bytes,
    bench_write_batch
);
criterion_main!(benches);
