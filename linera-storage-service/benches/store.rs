// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use linera_storage_service::client::ServiceStoreClient;
use linera_views::test_utils::performance;
use tokio::runtime::Runtime;

fn bench_storage_service(criterion: &mut Criterion) {
    criterion.bench_function("store_storage_service_contains_key", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_key::<ServiceStoreClient, _>(iterations, black_box).await
            })
    });

    criterion.bench_function("store_storage_service_contains_keys", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::contains_keys::<ServiceStoreClient, _>(iterations, black_box).await
            })
    });

    criterion.bench_function("store_storage_service_find_keys_by_prefix", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::find_keys_by_prefix::<ServiceStoreClient, _>(iterations, black_box)
                    .await
            })
    });

    criterion.bench_function(
        "store_storage_service_find_key_values_by_prefix",
        |bencher| {
            bencher
                .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
                .iter_custom(|iterations| async move {
                    performance::find_key_values_by_prefix::<ServiceStoreClient, _>(
                        iterations, black_box,
                    )
                    .await
                })
        },
    );

    criterion.bench_function("store_storage_service_read_value_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_value_bytes::<ServiceStoreClient, _>(iterations, black_box).await
            })
    });

    criterion.bench_function("store_storage_service_read_multi_values_bytes", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::read_multi_values_bytes::<ServiceStoreClient, _>(iterations, black_box)
                    .await
            })
    });

    criterion.bench_function("store_storage_service_write_batch", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                performance::write_batch::<ServiceStoreClient>(iterations).await
            })
    });
}

criterion_group!(benches, bench_storage_service,);
criterion_main!(benches);
