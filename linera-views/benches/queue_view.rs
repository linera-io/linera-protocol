// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};
use std::fmt::Debug;

use linera_views::test_utils::{DeterministicRng, make_deterministic_rng};
use linera_views::views::{CryptoHashRootView, RootView};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use linera_views::{
    common::KeyValueStore,
    backends::memory::create_test_memory_store,
    context::ViewContext,
    bucket_queue_view::BucketQueueView,
    queue_view::QueueView,
    views::View,
};
use rand::Rng;
#[cfg(with_dynamodb)]
use linera_views::dynamo_db::create_dynamo_db_test_store;
#[cfg(with_rocksdb)]
use linera_views::rocks_db::create_rocks_db_test_store;
#[cfg(with_scylladb)]
use linera_views::scylla_db::create_scylla_db_test_store;
use tokio::runtime::Runtime;

/// The number of operations
const N_OPERATIONS: usize = 1000;

enum Operations {
    Save,
    DeleteFront,
    PushBack(u8),
}

fn generate_test_case(n_operation: usize, rng: &mut DeterministicRng) -> Vec<Operations> {
    let mut operations = Vec:: new();
    let mut total_length = 0;
    for _ in 0..n_operation {
        let choice = rng.gen_range(0..10);
        if choice == 0 {
            operations.push(Operations::Save);
        } else if choice < 3 && total_length > 0 {
            operations.push(Operations::DeleteFront);
            total_length -= 1;
        } else {
            let val = rng.gen::<u8>();
            operations.push(Operations::PushBack(val));
            total_length += 1;
        }
    }
    operations
}

#[derive(CryptoHashRootView)]
pub struct QueueStateView<C> {
    pub queue: QueueView<C, u8>,
}

pub async fn performance_queue_view<S: KeyValueStore + Clone + Sync + 'static>(store: S, iterations: u64) -> Duration
where
    S::Error: Debug + Send + Sync + 'static,
{
    let context = ViewContext { store, base_key: Vec::new(), extra: () };
    let mut total_time = Duration::ZERO;
    let mut rng = make_deterministic_rng();
    for _ in 0..iterations {
        let operations = generate_test_case(N_OPERATIONS, &mut rng);
        let mut view = QueueStateView::load(context.clone()).await.unwrap();
        //
        let measurement = Instant::now();
        for operation in operations {
            match operation {
                Operations::Save => {
                    view.save().await.unwrap();
                },
                Operations::DeleteFront => {
                    view.queue.delete_front();
                },
                Operations::PushBack(val) => {
                    view.queue.push_back(val);
                },
            }
            black_box(view.queue.front().await.unwrap());
        }
        view.clear();
        view.save().await.unwrap();
        total_time += measurement.elapsed();
    }

    total_time
}

fn bench_queue_view(criterion: &mut Criterion) {
    criterion.bench_function("memory_queue_view", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_test_memory_store();
                performance_queue_view(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_queue_view", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_rocks_db_test_store().await;
                performance_queue_view(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_queue_view", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_queue_view(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_queue_view", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_queue_view(store, iterations).await
            })
    });
}

#[derive(CryptoHashRootView)]
pub struct BucketQueueStateView<C> {
    pub queue: BucketQueueView<C, u8, 100>,
}

pub async fn performance_bucket_queue_view<S: KeyValueStore + Clone + Sync + 'static>(store: S, iterations: u64) -> Duration
where
    S::Error: Debug + Send + Sync + 'static,
{
    let context = ViewContext { store, base_key: Vec::new(), extra: () };
    let mut total_time = Duration::ZERO;
    let mut rng = make_deterministic_rng();
    for _ in 0..iterations {
        let operations = generate_test_case(N_OPERATIONS, &mut rng);
        let mut view = BucketQueueStateView::load(context.clone()).await.unwrap();
        //
        let measurement = Instant::now();
        for operation in operations {
            match operation {
                Operations::Save => {
                    view.save().await.unwrap();
                },
                Operations::DeleteFront => {
                    view.queue.delete_front().await.unwrap();
                },
                Operations::PushBack(val) => {
                    view.queue.push_back(val);
                },
            }
            black_box(view.queue.front());
        }
        view.clear();
        view.save().await.unwrap();
        total_time += measurement.elapsed();
    }

    total_time
}

fn bench_bucket_queue_view(criterion: &mut Criterion) {
    criterion.bench_function("memory_bucket_queue_view", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_test_memory_store();
                performance_bucket_queue_view(store, iterations).await
            })
    });

    #[cfg(with_rocksdb)]
    criterion.bench_function("rocksdb_bucket_queue_view", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_rocks_db_test_store().await;
                performance_bucket_queue_view(store, iterations).await
            })
    });

    #[cfg(with_dynamodb)]
    criterion.bench_function("dynamodb_bucket_queue_view", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_dynamo_db_test_store().await;
                performance_bucket_queue_view(store, iterations).await
            })
    });

    #[cfg(with_scylladb)]
    criterion.bench_function("scylladb_bucket_queue_view", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let store = create_scylla_db_test_store().await;
                performance_bucket_queue_view(store, iterations).await
            })
    });
}





criterion_group!(
    benches,
    bench_queue_view,
    bench_bucket_queue_view
);
criterion_main!(benches);
