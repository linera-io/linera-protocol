// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use alloy_primitives::Keccak256;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sha3::{Digest, Sha3_256};

fn keccak(i: u8) {
    let mut hasher = Keccak256::new();
    hasher.update([i; 32]);
    hasher.finalize();
}

fn sha256(i: u8) {
    let mut hasher = Sha3_256::new();
    hasher.update([i; 32]);
    hasher.finalize();
}

fn keccak_benchmark(c: &mut Criterion) {
    c.bench_function("keccak", |b| b.iter(|| keccak(black_box(20))));
}

fn sha256_benchmark(c: &mut Criterion) {
    c.bench_function("sha256", |b| b.iter(|| sha256(black_box(20))));
}

criterion_group!(benches, keccak_benchmark, sha256_benchmark);
criterion_main!(benches);
