// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use linera_base::time::{Duration, Instant};
use linera_views::{
    batch::Batch,
    context::{create_test_memory_context, Context, MemoryContext},
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    views::View,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

/// Benchmarks the [`ReentrantCollectionView::try_load_all_entries`] against the manual
/// pattern, when the collection has all of its entries staged in memory.
fn bench_load_all_entries_already_in_memory(criterion: &mut Criterion) {
    criterion.bench_function(
        "load_all_entries_already_in_memory_with_method",
        |bencher| {
            bencher
                .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
                .iter_custom(|iterations| async move {
                    let mut total_time = Duration::ZERO;

                    for _ in 0..iterations {
                        let view = create_populated_reentrant_collection_view().await;

                        let measurement = Instant::now();
                        let entries = view
                            .try_load_all_entries()
                            .await
                            .expect("Failed to load entries from `ReentrantCollectionView`");
                        for (index, entry) in entries {
                            black_box(index);
                            black_box(entry);
                        }
                        total_time += measurement.elapsed();
                    }

                    total_time
                })
        },
    );

    criterion.bench_function("load_all_entries_already_in_memory_manually", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let mut total_time = Duration::ZERO;

                for _ in 0..iterations {
                    let view = create_populated_reentrant_collection_view().await;

                    let measurement = Instant::now();
                    let indices = view
                        .indices()
                        .await
                        .expect("Failed to load all indices from `ReentrantCollectionView`");
                    let entries = view
                        .try_load_entries(&indices)
                        .await
                        .expect("Failed to load entries from `ReentrantCollectionView`");
                    for (index, entry) in indices.into_iter().zip(entries) {
                        if let Some(entry) = entry {
                            black_box(index);
                            black_box(entry);
                        }
                    }
                    total_time += measurement.elapsed();
                }

                total_time
            })
    });
}

/// Benchmarks the [`ReentrantCollectionView::try_load_all_entries`] against the manual
/// pattern, when the collection has none of its entries staged in memory.
fn bench_load_all_entries_from_storage(criterion: &mut Criterion) {
    criterion.bench_function("load_all_entries_from_storage_with_method", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let mut total_time = Duration::ZERO;

                for _ in 0..iterations {
                    let view = create_and_store_populated_reentrant_collection_view().await;

                    let measurement = Instant::now();
                    let entries = view
                        .try_load_all_entries()
                        .await
                        .expect("Failed to load entries from `ReentrantCollectionView`");
                    for (index, entry) in entries {
                        black_box(index);
                        black_box(entry);
                    }
                    total_time += measurement.elapsed();
                }

                total_time
            })
    });

    criterion.bench_function("load_all_entries_from_storage_manually", |bencher| {
        bencher
            .to_async(Runtime::new().expect("Failed to create Tokio runtime"))
            .iter_custom(|iterations| async move {
                let mut total_time = Duration::ZERO;

                for _ in 0..iterations {
                    let view = create_and_store_populated_reentrant_collection_view().await;

                    let measurement = Instant::now();
                    let indices = view
                        .indices()
                        .await
                        .expect("Failed to load all indices from `ReentrantCollectionView`");
                    let entries = view
                        .try_load_entries(&indices)
                        .await
                        .expect("Failed to load entries from `ReentrantCollectionView`");
                    for (index, entry) in indices.into_iter().zip(entries) {
                        if let Some(entry) = entry {
                            black_box(index);
                            black_box(entry);
                        }
                    }
                    total_time += measurement.elapsed();
                }

                total_time
            })
    });
}

/// A helper type that simulates an index type that has a non-trivial cost to
/// serialize/deserialize.
#[derive(Clone, Debug, Deserialize, Serialize)]
enum ComplexIndex {
    UselessVariant,
    NestedVariant(Box<ComplexIndex>),
    Leaf(String),
}

/// Creates a populated [`ReentrantCollectionView`] with its contents still staged in memory.
async fn create_populated_reentrant_collection_view(
) -> ReentrantCollectionView<MemoryContext<()>, ComplexIndex, RegisterView<MemoryContext<()>, String>>
{
    let context = create_test_memory_context();
    let mut view: ReentrantCollectionView<_, ComplexIndex, RegisterView<_, String>> =
        ReentrantCollectionView::load(context)
            .await
            .expect("Failed to create `ReentrantCollectionView`");

    let greek_alphabet = [
        ("alpha", "α"),
        ("beta", "β"),
        ("gamma", "γ"),
        ("delta", "δ"),
        ("epsilon", "ε"),
        ("zeta", "ζ"),
        ("eta", "η"),
        ("theta", "θ"),
        ("iota", "ι"),
        ("kappa", "κ"),
        ("lambda", "λ"),
        ("mu", "μ"),
        ("nu", "ν"),
        ("xi", "ξ"),
        ("omicron", "ο"),
        ("pi", "π"),
        ("rho", "ρ"),
        ("sigma", "σ"),
        ("tau", "τ"),
        ("upsilon", "υ"),
        ("phi", "φ"),
        ("chi", "χ"),
        ("psi", "ψ"),
        ("omega", "ω"),
    ];

    for (name, letter) in greek_alphabet {
        let index = ComplexIndex::NestedVariant(Box::new(ComplexIndex::Leaf(name.to_owned())));

        view.try_load_entry_mut(&index)
            .await
            .expect("Failed to create entry in `ReentrantCollectionView`")
            .set(letter.to_owned());
    }

    view
}

/// Creates a populated [`ReentrantCollectionView`] with its contents completely flushed to
/// the storage.
async fn create_and_store_populated_reentrant_collection_view(
) -> ReentrantCollectionView<MemoryContext<()>, ComplexIndex, RegisterView<MemoryContext<()>, String>>
{
    let mut view = create_populated_reentrant_collection_view().await;
    let context = view.context().clone();
    let mut batch = Batch::new();
    view.flush(&mut batch)
        .expect("Failed to flush populated `ReentrantCollectionView`'s contents");
    context
        .write_batch(batch)
        .await
        .expect("Failed to store populated `ReentrantCollectionView`'s contents");

    ReentrantCollectionView::load(context)
        .await
        .expect("Failed to create second `ReentrantCollectionView`")
}

criterion_group!(
    benches,
    bench_load_all_entries_already_in_memory,
    bench_load_all_entries_from_storage
);
criterion_main!(benches);
