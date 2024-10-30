// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use linera_storage_service::client::ServiceStoreClient;
use linera_views::{
    store::TestKeyValueStore as _,
    test_utils::performance::{
        performance_contains_key, performance_contains_keys, performance_find_key_values_by_prefix,
        performance_find_keys_by_prefix, performance_read_multi_values_bytes,
        performance_read_value_bytes, performance_write_batch,
    },
};
use tokio::runtime::Runtime;

fn bench_storage_service(criterion: &mut Criterion) {
    criterion.bench_function("store_storage_service_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ServiceStoreClient::new_test_store().await.unwrap();
                performance_contains_key(store, iterations, black_box).await
            })
    });

    criterion.bench_function("store_storage_service_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ServiceStoreClient::new_test_store().await.unwrap();
                performance_contains_keys(store, iterations, black_box).await
            })
    });

    criterion.bench_function("store_storage_service_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ServiceStoreClient::new_test_store().await.unwrap();
                performance_find_keys_by_prefix(store, iterations, black_box).await
            })
    });

    criterion.bench_function(
        "store_storage_service_find_key_values_by_prefix",
        |bencher| {
            bencher
                .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
                .iter_custom(|iterations| async move {
                    let store = ServiceStoreClient::new_test_store().await.unwrap();
                    performance_find_key_values_by_prefix(store, iterations, black_box).await
                })
        },
    );

    criterion.bench_function("store_storage_service_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ServiceStoreClient::new_test_store().await.unwrap();
                performance_read_value_bytes(store, iterations, black_box).await
            })
    });

    criterion.bench_function("store_storage_service_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ServiceStoreClient::new_test_store().await.unwrap();
                performance_read_multi_values_bytes(store, iterations, black_box).await
            })
    });

    criterion.bench_function("store_storage_service_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = ServiceStoreClient::new_test_store().await.unwrap();
                performance_write_batch(store, iterations, black_box).await
            })
    });
}

criterion_group!(benches, bench_storage_service,);
criterion_main!(benches);
