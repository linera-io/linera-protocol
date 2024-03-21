// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        with_testing: { any(test, feature = "test") },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
        with_dynamodb: { all(not(target_arch = "wasm32"), feature = "dynamodb") },
        with_rocksdb: { all(not(target_arch = "wasm32"), feature = "rocksdb") },
        with_scylladb: { all(not(target_arch = "wasm32"), feature = "scylladb") },
        with_wasmer: { all(not(target_arch = "wasm32"), feature = "wasmer") },
        with_wasmtime: { all(not(target_arch = "wasm32"), feature = "wasmtime") },
        with_wasm_runtime: { any(with_wasmer, with_wasmtime) },
    };
}
