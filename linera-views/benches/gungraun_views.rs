// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Instruction-count benchmarks for linera-views using Gungraun.
//!
//! These benchmarks use Valgrind's Callgrind to count CPU instructions,
//! providing deterministic, noise-free regression detection in CI.
//! Both 100-element and 1000-element variants are included to separate
//! constant overhead from per-element scaling costs.

use std::hint::black_box;

use gungraun::{
    library_benchmark, library_benchmark_group, main, Callgrind, EventKind, LibraryBenchmarkConfig,
};
use linera_views::{
    batch::Batch,
    bucket_queue_view::BucketQueueView,
    collection_view::CollectionView,
    context::{Context, MemoryContext},
    map_view::MapView,
    queue_view::QueueView,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    store::WritableKeyValueStore as _,
    views::View,
};
use tokio::runtime::Runtime;

mod common;

use common::ComplexIndex;

// ---------------------------------------------------------------------------
// MapView setup helpers
// ---------------------------------------------------------------------------

fn setup_empty_map() -> (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        MapView::load(context).await.unwrap()
    });
    (rt, view)
}

fn setup_map_100() -> (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: MapView<_, String, Vec<u8>> = MapView::load(context).await.unwrap();
        for i in 0..100 {
            view.insert(&format!("key_{i:04}"), vec![i as u8; 100])
                .unwrap();
        }
        view
    });
    (rt, view)
}

fn setup_map_1000() -> (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: MapView<_, String, Vec<u8>> = MapView::load(context).await.unwrap();
        for i in 0..1000 {
            view.insert(&format!("key_{i:04}"), vec![i as u8; 100])
                .unwrap();
        }
        view
    });
    (rt, view)
}

fn setup_map_1000_pre_saved() -> (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: MapView<_, String, Vec<u8>> = MapView::load(context).await.unwrap();
        for i in 0..1000 {
            view.insert(&format!("key_{i:04}"), vec![i as u8; 100])
                .unwrap();
        }
        let ctx = view.context().clone();
        let mut batch = Batch::new();
        view.pre_save(&mut batch).unwrap();
        ctx.store().write_batch(batch).await.unwrap();
        view
    });
    (rt, view)
}

// ---------------------------------------------------------------------------
// MapView benchmarks
// ---------------------------------------------------------------------------

#[library_benchmark]
#[bench::insert_100(setup_empty_map())]
fn bench_map_view_insert_100(
    (_rt, mut view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>),
) {
    for i in 0..100 {
        view.insert(&format!("key_{i:04}"), vec![i as u8; 100])
            .unwrap();
    }
    black_box(&view);
}

#[library_benchmark]
#[bench::insert_1000(setup_empty_map())]
fn bench_map_view_insert_1000(
    (_rt, mut view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>),
) {
    for i in 0..1000 {
        view.insert(&format!("key_{i:04}"), vec![i as u8; 100])
            .unwrap();
    }
    black_box(&view);
}

#[library_benchmark]
#[bench::from_100(setup_map_100())]
#[bench::from_1000(setup_map_1000())]
fn bench_map_view_get_existing((rt, view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>)) {
    rt.block_on(async {
        for i in (0..100).step_by(10) {
            let val = view.get(&format!("key_{i:04}")).await.unwrap();
            black_box(val);
        }
    });
}

#[library_benchmark]
#[bench::from_1000(setup_map_1000())]
fn bench_map_view_get_missing((rt, view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>)) {
    rt.block_on(async {
        for i in 0..100 {
            let val = view.get(&format!("missing_{i:04}")).await.unwrap();
            black_box(val);
        }
    });
}

#[library_benchmark]
#[bench::from_100(setup_map_100())]
#[bench::from_1000(setup_map_1000())]
fn bench_map_view_indices((rt, view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>)) {
    rt.block_on(async {
        let keys = view.indices().await.unwrap();
        black_box(keys);
    });
}

#[library_benchmark]
#[bench::from_100(setup_map_100())]
#[bench::from_1000(setup_map_1000())]
fn bench_map_view_contains_key((rt, view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>)) {
    rt.block_on(async {
        for i in (0..100).step_by(10) {
            let result = view.contains_key(&format!("key_{i:04}")).await.unwrap();
            black_box(result);
        }
    });
}

#[library_benchmark]
#[bench::remove_500_from_1000(setup_map_1000())]
fn bench_map_view_remove((_rt, mut view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>)) {
    for i in 0..500 {
        view.remove(&format!("key_{i:04}")).unwrap();
    }
    black_box(&view);
}

#[library_benchmark]
#[bench::from_100(setup_map_100())]
#[bench::from_1000(setup_map_1000())]
fn bench_map_view_pre_save((_rt, view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>)) {
    let mut batch = Batch::new();
    let _ = view.pre_save(&mut batch);
    black_box(batch);
}

#[library_benchmark]
#[bench::from_1000(setup_map_1000_pre_saved())]
fn bench_map_view_post_save(
    (_rt, mut view): (Runtime, MapView<MemoryContext<()>, String, Vec<u8>>),
) {
    view.post_save();
    black_box(&view);
}

// ---------------------------------------------------------------------------
// QueueView setup helpers
// ---------------------------------------------------------------------------

fn setup_empty_queue() -> (Runtime, QueueView<MemoryContext<()>, u8>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        QueueView::load(context).await.unwrap()
    });
    (rt, view)
}

fn setup_queue_1000() -> (Runtime, QueueView<MemoryContext<()>, u8>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: QueueView<_, u8> = QueueView::load(context).await.unwrap();
        for i in 0..1000 {
            view.push_back((i % 256) as u8);
        }
        view
    });
    (rt, view)
}

// ---------------------------------------------------------------------------
// QueueView benchmarks
// ---------------------------------------------------------------------------

#[library_benchmark]
#[bench::push_1000(setup_empty_queue())]
fn bench_queue_push_back((_rt, mut view): (Runtime, QueueView<MemoryContext<()>, u8>)) {
    for i in 0..1000u16 {
        view.push_back((i % 256) as u8);
    }
    black_box(&view);
}

#[library_benchmark]
#[bench::front_100_from_1000(setup_queue_1000())]
fn bench_queue_front((rt, view): (Runtime, QueueView<MemoryContext<()>, u8>)) {
    rt.block_on(async {
        for _ in 0..100 {
            let val = view.front().await.unwrap();
            black_box(val);
        }
    });
}

#[library_benchmark]
#[bench::delete_500_from_1000(setup_queue_1000())]
fn bench_queue_delete_front((_rt, mut view): (Runtime, QueueView<MemoryContext<()>, u8>)) {
    for _ in 0..500 {
        view.delete_front();
    }
    black_box(&view);
}

#[library_benchmark]
#[bench::pre_save_1000(setup_queue_1000())]
fn bench_queue_pre_save((_rt, view): (Runtime, QueueView<MemoryContext<()>, u8>)) {
    let mut batch = Batch::new();
    let _ = view.pre_save(&mut batch);
    black_box(batch);
}

// ---------------------------------------------------------------------------
// BucketQueueView setup helpers
// ---------------------------------------------------------------------------

fn setup_empty_bucket_queue() -> (Runtime, BucketQueueView<MemoryContext<()>, u8, 100>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        BucketQueueView::load(context).await.unwrap()
    });
    (rt, view)
}

fn setup_bucket_queue_1000() -> (Runtime, BucketQueueView<MemoryContext<()>, u8, 100>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: BucketQueueView<_, u8, 100> = BucketQueueView::load(context).await.unwrap();
        for i in 0..1000 {
            view.push_back((i % 256) as u8);
        }
        view
    });
    (rt, view)
}

// ---------------------------------------------------------------------------
// BucketQueueView benchmarks
// ---------------------------------------------------------------------------

#[library_benchmark]
#[bench::push_1000(setup_empty_bucket_queue())]
fn bench_bucket_queue_push_back(
    (_rt, mut view): (Runtime, BucketQueueView<MemoryContext<()>, u8, 100>),
) {
    for i in 0..1000u16 {
        view.push_back((i % 256) as u8);
    }
    black_box(&view);
}

#[library_benchmark]
#[bench::front_100_from_1000(setup_bucket_queue_1000())]
fn bench_bucket_queue_front((_rt, view): (Runtime, BucketQueueView<MemoryContext<()>, u8, 100>)) {
    for _ in 0..100 {
        let val = view.front();
        black_box(val);
    }
}

#[library_benchmark]
#[bench::delete_500_from_1000(setup_bucket_queue_1000())]
fn bench_bucket_queue_delete_front(
    (rt, mut view): (Runtime, BucketQueueView<MemoryContext<()>, u8, 100>),
) {
    rt.block_on(async {
        for _ in 0..500 {
            view.delete_front().await.unwrap();
        }
        black_box(&view);
    });
}

#[library_benchmark]
#[bench::pre_save_1000(setup_bucket_queue_1000())]
fn bench_bucket_queue_pre_save(
    (_rt, view): (Runtime, BucketQueueView<MemoryContext<()>, u8, 100>),
) {
    let mut batch = Batch::new();
    let _ = view.pre_save(&mut batch);
    black_box(batch);
}

// ---------------------------------------------------------------------------
// RegisterView setup helpers
// ---------------------------------------------------------------------------

fn setup_register_view() -> (Runtime, RegisterView<MemoryContext<()>, String>) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: RegisterView<_, String> = RegisterView::load(context).await.unwrap();
        view.set("initial_value".to_string());
        view
    });
    (rt, view)
}

// ---------------------------------------------------------------------------
// RegisterView benchmarks
// ---------------------------------------------------------------------------

#[library_benchmark]
#[bench::get_set_100(setup_register_view())]
fn bench_register_view_get_set(
    (_rt, mut view): (Runtime, RegisterView<MemoryContext<()>, String>),
) {
    for i in 0..100 {
        black_box(view.get());
        view.set(format!("value_{i:04}"));
    }
}

#[library_benchmark]
#[bench::pre_save_100(setup_register_view())]
fn bench_register_view_pre_save((_rt, view): (Runtime, RegisterView<MemoryContext<()>, String>)) {
    for _ in 0..100 {
        let mut batch = Batch::new();
        let _ = view.pre_save(&mut batch);
        black_box(batch);
    }
}

// ---------------------------------------------------------------------------
// CollectionView setup helpers
// ---------------------------------------------------------------------------

type TestCollView =
    CollectionView<MemoryContext<()>, ComplexIndex, RegisterView<MemoryContext<()>, String>>;

fn make_collection_indices(n: usize) -> Vec<ComplexIndex> {
    (0..n)
        .map(|i| ComplexIndex::NestedVariant(Box::new(ComplexIndex::Leaf(format!("entry_{i:04}")))))
        .collect()
}

fn setup_populated_coll_view() -> (Runtime, TestCollView) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: CollectionView<_, ComplexIndex, RegisterView<_, String>> =
            CollectionView::load(context).await.unwrap();
        for idx in make_collection_indices(100) {
            view.load_entry_mut(&idx)
                .await
                .unwrap()
                .set(format!("{idx:?}"));
        }
        view
    });
    (rt, view)
}

fn setup_coll_view_from_storage() -> (Runtime, TestCollView) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: CollectionView<_, ComplexIndex, RegisterView<_, String>> =
            CollectionView::load(context).await.unwrap();
        for idx in make_collection_indices(100) {
            view.load_entry_mut(&idx)
                .await
                .unwrap()
                .set(format!("{idx:?}"));
        }
        let ctx = view.context().clone();
        let mut batch = Batch::new();
        view.pre_save(&mut batch).unwrap();
        ctx.store().write_batch(batch).await.unwrap();
        view.post_save();
        CollectionView::load(ctx).await.unwrap()
    });
    (rt, view)
}

// ---------------------------------------------------------------------------
// CollectionView benchmarks
// ---------------------------------------------------------------------------

#[library_benchmark]
#[bench::load_all_100_in_memory(setup_populated_coll_view())]
fn bench_coll_view_load_all_in_memory((rt, view): (Runtime, TestCollView)) {
    rt.block_on(async {
        let entries = view.try_load_all_entries().await.unwrap();
        black_box(entries);
    });
}

#[library_benchmark]
#[bench::load_all_100_from_storage(setup_coll_view_from_storage())]
fn bench_coll_view_load_all_from_storage((rt, view): (Runtime, TestCollView)) {
    rt.block_on(async {
        let entries = view.try_load_all_entries().await.unwrap();
        black_box(entries);
    });
}

#[library_benchmark]
#[bench::try_load_10_from_100(setup_populated_coll_view())]
fn bench_coll_view_try_load_entry((rt, view): (Runtime, TestCollView)) {
    rt.block_on(async {
        for idx in make_collection_indices(10) {
            let entry = view.try_load_entry(&idx).await.unwrap();
            black_box(entry);
        }
    });
}

#[library_benchmark]
#[bench::indices_100(setup_populated_coll_view())]
fn bench_coll_view_indices((rt, view): (Runtime, TestCollView)) {
    rt.block_on(async {
        let indices = view.indices().await.unwrap();
        black_box(indices);
    });
}

#[library_benchmark]
#[bench::pre_save_100(setup_populated_coll_view())]
fn bench_coll_view_pre_save((_rt, view): (Runtime, TestCollView)) {
    let mut batch = Batch::new();
    let _ = view.pre_save(&mut batch);
    black_box(batch);
}

// ---------------------------------------------------------------------------
// ReentrantCollectionView setup helpers
// ---------------------------------------------------------------------------

type TestReentrantCollView = ReentrantCollectionView<
    MemoryContext<()>,
    ComplexIndex,
    RegisterView<MemoryContext<()>, String>,
>;

fn setup_populated_reentrant_coll() -> (Runtime, TestReentrantCollView) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: ReentrantCollectionView<_, ComplexIndex, RegisterView<_, String>> =
            ReentrantCollectionView::load(context).await.unwrap();
        for idx in make_collection_indices(100) {
            view.try_load_entry_mut(&idx)
                .await
                .unwrap()
                .set(format!("{idx:?}"));
        }
        view
    });
    (rt, view)
}

fn setup_reentrant_coll_from_storage() -> (Runtime, TestReentrantCollView) {
    let rt = Runtime::new().unwrap();
    let view = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: ReentrantCollectionView<_, ComplexIndex, RegisterView<_, String>> =
            ReentrantCollectionView::load(context).await.unwrap();
        for idx in make_collection_indices(100) {
            view.try_load_entry_mut(&idx)
                .await
                .unwrap()
                .set(format!("{idx:?}"));
        }
        let ctx = view.context().clone();
        let mut batch = Batch::new();
        view.pre_save(&mut batch).unwrap();
        ctx.store().write_batch(batch).await.unwrap();
        view.post_save();
        ReentrantCollectionView::load(ctx).await.unwrap()
    });
    (rt, view)
}

// ---------------------------------------------------------------------------
// ReentrantCollectionView benchmarks
// ---------------------------------------------------------------------------

#[library_benchmark]
#[bench::load_all_100_in_memory(setup_populated_reentrant_coll())]
fn bench_reentrant_coll_load_all_in_memory((rt, view): (Runtime, TestReentrantCollView)) {
    rt.block_on(async {
        let entries = view.try_load_all_entries().await.unwrap();
        black_box(entries);
    });
}

#[library_benchmark]
#[bench::load_all_100_from_storage(setup_reentrant_coll_from_storage())]
fn bench_reentrant_coll_load_all_from_storage((rt, view): (Runtime, TestReentrantCollView)) {
    rt.block_on(async {
        let entries = view.try_load_all_entries().await.unwrap();
        black_box(entries);
    });
}

#[library_benchmark]
#[bench::contains_key_10_from_100(setup_populated_reentrant_coll())]
fn bench_reentrant_coll_contains_key((rt, view): (Runtime, TestReentrantCollView)) {
    rt.block_on(async {
        for idx in make_collection_indices(10) {
            let result = view.contains_key(&idx).await.unwrap();
            black_box(result);
        }
    });
}

#[library_benchmark]
#[bench::indices_100(setup_populated_reentrant_coll())]
fn bench_reentrant_coll_indices((rt, view): (Runtime, TestReentrantCollView)) {
    rt.block_on(async {
        let indices = view.indices().await.unwrap();
        black_box(indices);
    });
}

#[library_benchmark]
#[bench::pre_save_100(setup_populated_reentrant_coll())]
fn bench_reentrant_coll_pre_save((_rt, view): (Runtime, TestReentrantCollView)) {
    let mut batch = Batch::new();
    let _ = view.pre_save(&mut batch);
    black_box(batch);
}

// ---------------------------------------------------------------------------
// Cold-load benchmarks (load View from storage)
// ---------------------------------------------------------------------------

fn setup_map_1000_in_storage() -> (Runtime, MemoryContext<()>) {
    let rt = Runtime::new().unwrap();
    let context = rt.block_on(async {
        let context = MemoryContext::new_for_testing(());
        let mut view: MapView<_, String, Vec<u8>> = MapView::load(context.clone()).await.unwrap();
        for i in 0..1000 {
            view.insert(&format!("key_{i:04}"), vec![i as u8; 100])
                .unwrap();
        }
        let mut batch = Batch::new();
        let _ = view.pre_save(&mut batch);
        context.store().write_batch(batch).await.unwrap();
        view.post_save();
        context
    });
    (rt, context)
}

#[library_benchmark]
#[bench::load_1000(setup_map_1000_in_storage())]
fn bench_map_view_cold_load((rt, context): (Runtime, MemoryContext<()>)) {
    rt.block_on(async {
        let view: MapView<_, String, Vec<u8>> = MapView::load(context).await.unwrap();
        black_box(view);
    });
}

// ---------------------------------------------------------------------------
// Benchmark groups and main
// ---------------------------------------------------------------------------

library_benchmark_group!(
    name = map_view_group;
    benchmarks =
        bench_map_view_insert_100,
        bench_map_view_insert_1000,
        bench_map_view_get_existing,
        bench_map_view_get_missing,
        bench_map_view_indices,
        bench_map_view_contains_key,
        bench_map_view_remove,
        bench_map_view_pre_save,
        bench_map_view_post_save
);

library_benchmark_group!(
    name = queue_view_group;
    benchmarks =
        bench_queue_push_back,
        bench_queue_front,
        bench_queue_delete_front,
        bench_queue_pre_save,
        bench_bucket_queue_push_back,
        bench_bucket_queue_front,
        bench_bucket_queue_delete_front,
        bench_bucket_queue_pre_save
);

library_benchmark_group!(
    name = register_view_group;
    benchmarks =
        bench_register_view_get_set,
        bench_register_view_pre_save
);

library_benchmark_group!(
    name = collection_view_group;
    benchmarks =
        bench_coll_view_load_all_in_memory,
        bench_coll_view_load_all_from_storage,
        bench_coll_view_try_load_entry,
        bench_coll_view_indices,
        bench_coll_view_pre_save
);

library_benchmark_group!(
    name = reentrant_collection_view_group;
    benchmarks =
        bench_reentrant_coll_load_all_in_memory,
        bench_reentrant_coll_load_all_from_storage,
        bench_reentrant_coll_contains_key,
        bench_reentrant_coll_indices,
        bench_reentrant_coll_pre_save
);

library_benchmark_group!(
    name = cold_load_group;
    benchmarks =
        bench_map_view_cold_load
);

main!(
    config = LibraryBenchmarkConfig::default()
        .tool(
            Callgrind::default()
                .soft_limits([(EventKind::Ir, 1.0)])
        );
    library_benchmark_groups =
        map_view_group,
        queue_view_group,
        register_view_group,
        collection_view_group,
        reentrant_collection_view_group,
        cold_load_group
);
