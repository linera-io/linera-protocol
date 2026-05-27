// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Regression guards for unbounded `papaya::HashMap` allocations on clone
//! (suspected contributor to `JAVASCRIPT-REACT-69`).
//!
//! `papaya::HashMap` was stored *by value* in `GrpcConnectionPool`,
//! `GrpcNodeProvider`, and `GrpcClient`. Each `Clone` on one of those structs
//! allocated a fresh map together with its `seize::Collector` and per-thread
//! slot table. Under load (every chain subscription clones `GrpcClient`, every
//! committee refresh re-clones cooldowns) this produces O(validators × chains)
//! allocations of `seize::Collector`, which on wasm would surface at
//! `seize::raw::tls::ThreadLocal::with_capacity`. Whether this was the sole
//! root cause of `JAVASCRIPT-REACT-69` was not confirmed by reproduction.
//!
//! The fix wraps each affected map in `Arc`, so `Clone` becomes a refcount
//! bump and the map is genuinely shared (which the doc comments already
//! claimed).
//!
//! These tests measure the property directly using a counting global
//! allocator. Both assertions run on the same allocator: the negative test
//! demonstrates that the bug pattern still allocates (so the test stays
//! meaningful if dependencies change), and the positive test asserts the
//! fixed `Arc<papaya::HashMap>` shape is alloc-free under clone.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use linera_base::time::Instant;

struct Counting;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Cargo runs `#[test]` functions in parallel, but our counter is process-wide.
/// Serialize the measurement sections so concurrent allocations from other
/// tests cannot pollute the delta.
static MEASURE_LOCK: Mutex<()> = Mutex::new(());

fn measure<R>(body: impl FnOnce() -> R) -> (usize, R) {
    let _guard = MEASURE_LOCK.lock().expect("measurement mutex poisoned");
    let baseline = ALLOC_COUNT.load(Ordering::Relaxed);
    let result = body();
    let delta = ALLOC_COUNT.load(Ordering::Relaxed).saturating_sub(baseline);
    (delta, result)
}

/// Positive guard: the post-fix field shape is alloc-free under `Clone`.
///
/// This is the property that prevents `seize::Collector` from being allocated
/// on every `GrpcClient::clone` / `make_node` call.
#[test]
fn cloning_arc_papaya_hashmap_does_not_allocate() {
    // Construct the map up-front so its allocations are not in the measurement
    // window. We pre-allocate the destination `Vec` for the same reason.
    let map: Arc<papaya::HashMap<String, Instant>> = Arc::new(papaya::HashMap::new());
    let mut clones: Vec<Arc<papaya::HashMap<String, Instant>>> = Vec::with_capacity(1_000);

    let (delta, ()) = measure(|| {
        for _ in 0..1_000 {
            clones.push(map.clone());
        }
    });

    eprintln!("Arc<papaya::HashMap>: 1000 clones -> {delta} allocations");

    // `Arc::clone` is a pure refcount bump — no global allocator traffic.
    // The destination `Vec` has reserved capacity, so it should not grow.
    // Tolerate a tiny noise margin (e.g. from the runtime's bookkeeping).
    assert!(
        delta < 10,
        "Arc<papaya::HashMap>::clone allocated {delta} times across 1000 clones (expected <10)"
    );
    drop(clones);
}

/// Negative guard: cloning a `papaya::HashMap` by value really does allocate.
///
/// This is the bug pattern that previously lived in `GrpcClient`,
/// `GrpcNodeProvider`, and `GrpcConnectionPool`. Keeping it in tests makes
/// the regression risk explicit: if a future change makes `papaya::HashMap`
/// clone-by-value alloc-free on its own (e.g. interior `Arc`), the
/// `Arc<…>` wrapping could be reconsidered — and this test will guide that
/// decision instead of being silent.
#[test]
fn cloning_papaya_hashmap_by_value_allocates() {
    let map: papaya::HashMap<String, Instant> = papaya::HashMap::new();
    let mut clones: Vec<papaya::HashMap<String, Instant>> = Vec::with_capacity(100);

    let (delta, ()) = measure(|| {
        for _ in 0..100 {
            clones.push(map.clone());
        }
    });

    eprintln!("papaya::HashMap (bug pattern): 100 clones -> {delta} allocations");

    // Each `papaya::HashMap::clone` allocates a fresh `seize::Collector` plus
    // its `ThreadLocal` slot table — at least one allocation per clone, in
    // practice several. We assert >= one alloc per clone with margin.
    assert!(
        delta >= 100,
        "papaya::HashMap::clone allocated only {delta} times across 100 clones \
         (expected >= 100; if this is now alloc-free, revisit the Arc wrapping \
         in linera-rpc/src/grpc/*.rs)"
    );
    drop(clones);
}
