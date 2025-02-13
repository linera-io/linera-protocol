// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(with_dynamodb)]
use linera_views::dynamo_db::DynamoDbStore;
#[cfg(with_rocksdb)]
use linera_views::rocks_db::RocksDbStore;
#[cfg(with_scylladb)]
use linera_views::scylla_db::ScyllaDbStore;
use linera_views::{memory::MemoryStore, test_utils::performance};
use tokio::runtime::Runtime;

fn bench_contains_key(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_key::<MemoryStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_key::<RocksDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_key::<DynamoDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_key::<ScyllaDbStore, _>(iterations, black_box).await
            })
    });
}

fn bench_contains_keys(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_keys::<MemoryStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_keys::<RocksDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_keys::<DynamoDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_keys::<ScyllaDbStore, _>(iterations, black_box).await
            })
    });
}

fn bench_find_keys_by_prefix(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_keys_by_prefix::<MemoryStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_keys_by_prefix::<RocksDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_keys_by_prefix::<DynamoDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_keys_by_prefix::<ScyllaDbStore, _>(iterations, black_box).await
            })
    });
}

fn bench_find_key_values_by_prefix(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_key_values_by_prefix::<MemoryStore, _>(iterations, black_box)
                    .await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_key_values_by_prefix::<RocksDbStore, _>(iterations, black_box)
                    .await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_key_values_by_prefix::<DynamoDbStore, _>(iterations, black_box)
                    .await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_find_key_values_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_key_values_by_prefix::<ScyllaDbStore, _>(iterations, black_box)
                    .await
            })
    });
}

fn bench_read_value_bytes(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_value_bytes::<MemoryStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_value_bytes::<RocksDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_value_bytes::<DynamoDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_value_bytes::<ScyllaDbStore, _>(iterations, black_box).await
            })
    });
}

fn bench_read_multi_values_bytes(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_multi_values_bytes::<MemoryStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_multi_values_bytes::<RocksDbStore, _>(iterations, black_box).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_multi_values_bytes::<DynamoDbStore, _>(iterations, black_box)
                    .await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_multi_values_bytes::<ScyllaDbStore, _>(iterations, black_box)
                    .await
            })
    });
}

fn bench_write_batch(criterion: &mut Criterion) {
    criterion.bench_function("store_memory_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::write_batch::<MemoryStore>(iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("store_rocksdb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::write_batch::<RocksDbStore>(iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("store_dynamodb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::write_batch::<DynamoDbStore>(iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("store_scylladb_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::write_batch::<ScyllaDbStore>(iterations).await
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
