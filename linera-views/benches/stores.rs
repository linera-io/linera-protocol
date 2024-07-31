// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};
use std::fmt::Debug;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use linera_views::{
    batch::Batch,
    test_utils::{prefix_expansion, get_random_key_values2},
    memory::{create_memory_store, MemoryStore},
    common::{LocalKeyValueStore},
};
use tokio::runtime::Runtime;

#[cfg(with_rocksdb)]
use linera_views::rocks_db::{create_rocks_db_test_store, RocksDbStore};

#[cfg(with_dynamodb)]
use linera_views::dynamo_db::{create_dynamo_db_test_store, DynamoDbStore};

#[cfg(with_scylladb)]
use linera_views::scylla_db::{create_scylla_db_test_store, ScyllaDbStore};

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
    criterion.bench_function(
        "memory_contain_key",
        |bencher| {
            bencher
                .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
		.iter_custom(|iterations| async move {
                    let store = create_memory_store();
                    performance_contains_key::<MemoryStore>(store, iterations).await
                })
        }
    );

    #[cfg(with_rocksdb)]
    criterion.bench_function(
        "rocksdb_contain_key",
        |bencher| {
            bencher
                .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
		.iter_custom(|iterations| async move {
                    let (store, _dir) = create_rocks_db_test_store().await;
                    performance_contains_key::<RocksDbStore>(store, iterations).await
                })
        }
    );

    #[cfg(with_dynamodb)]
    criterion.bench_function(
        "dynamodb_contain_key",
        |bencher| {
            bencher
                .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
		.iter_custom(|iterations| async move {
                    let store = create_dynamo_db_test_store().await;
                    performance_contains_key::<DynamoDbStore>(store, iterations).await
                })
        }
    );

    #[cfg(with_scylladb)]
    criterion.bench_function(
        "scylladb_contain_key",
        |bencher| {
            bencher
                .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
		.iter_custom(|iterations| async move {
                    let store = create_scylla_db_test_store().await;
                    performance_contains_key::<ScyllaDbStore>(store, iterations).await
                })
        }
    );
}

criterion_group!(
    benches,
    bench_contain_key
);
criterion_main!(benches);
