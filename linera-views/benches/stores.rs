// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(with_dynamodb)]
use linera_views::dynamo_db::DynamoDbStore;
#[cfg(with_rocksdb)]
use linera_views::rocks_db::RocksDbStore;
#[cfg(with_scylladb)]
use linera_views::scylla_db::ScyllaDbStore;
use linera_views::{
    memory::MemoryStore,
    store::TestKeyValueStore as _,
    test_utils::performance::{
        performance_contains_key, performance_contains_keys, performance_find_key_values_by_prefix,
        performance_find_keys_by_prefix, performance_read_multi_values_bytes,
        performance_read_value_bytes, performance_write_batch,
    },
};
use tokio::runtime::Runtime;

fn bench_contains_key(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = MemoryStore::new_test_store().await.unwrap();
                performance_contains_key(store, iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = RocksDbStore::new_test_store().await.unwrap();
                performance_contains_key(store, iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = DynamoDbStore::new_test_store().await.unwrap();
                performance_contains_key(store, iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ScyllaDbStore::new_test_store().await.unwrap();
                performance_contains_key(store, iterations, black_box).await
            })
    });
}

fn bench_contains_keys(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = MemoryStore::new_test_store().await.unwrap();
                performance_contains_keys(store, iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = RocksDbStore::new_test_store().await.unwrap();
                performance_contains_keys(store, iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = DynamoDbStore::new_test_store().await.unwrap();
                performance_contains_keys(store, iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ScyllaDbStore::new_test_store().await.unwrap();
                performance_contains_keys(store, iterations, black_box).await
            })
    });
}

fn bench_find_keys_by_prefix(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = MemoryStore::new_test_store().await.unwrap();
                performance_find_keys_by_prefix(store, iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = RocksDbStore::new_test_store().await.unwrap();
                performance_find_keys_by_prefix(store, iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = DynamoDbStore::new_test_store().await.unwrap();
                performance_find_keys_by_prefix(store, iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ScyllaDbStore::new_test_store().await.unwrap();
                performance_find_keys_by_prefix(store, iterations, black_box).await
            })
    });
}

fn bench_find_key_values_by_prefix(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = MemoryStore::new_test_store().await.unwrap();
                performance_find_key_values_by_prefix(store, iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = RocksDbStore::new_test_store().await.unwrap();
                performance_find_key_values_by_prefix(store, iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = DynamoDbStore::new_test_store().await.unwrap();
                performance_find_key_values_by_prefix(store, iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ScyllaDbStore::new_test_store().await.unwrap();
                performance_find_key_values_by_prefix(store, iterations, black_box).await
            })
    });
}

fn bench_read_value_bytes(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = MemoryStore::new_test_store().await.unwrap();
                performance_read_value_bytes(store, iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = RocksDbStore::new_test_store().await.unwrap();
                performance_read_value_bytes(store, iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = DynamoDbStore::new_test_store().await.unwrap();
                performance_read_value_bytes(store, iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ScyllaDbStore::new_test_store().await.unwrap();
                performance_read_value_bytes(store, iterations, black_box).await
            })
    });
}

fn bench_read_multi_values_bytes(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = MemoryStore::new_test_store().await.unwrap();
                performance_read_multi_values_bytes(store, iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = RocksDbStore::new_test_store().await.unwrap();
                performance_read_multi_values_bytes(store, iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = DynamoDbStore::new_test_store().await.unwrap();
                performance_read_multi_values_bytes(store, iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ScyllaDbStore::new_test_store().await.unwrap();
                performance_read_multi_values_bytes(store, iterations, black_box).await
            })
    });
}

fn bench_write_batch(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = MemoryStore::new_test_store().await.unwrap();
                performance_write_batch(store, iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = RocksDbStore::new_test_store().await.unwrap();
                performance_write_batch(store, iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = DynamoDbStore::new_test_store().await.unwrap();
                performance_write_batch(store, iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ScyllaDbStore::new_test_store().await.unwrap();
                performance_write_batch(store, iterations, black_box).await
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
