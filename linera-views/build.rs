// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        web: { all(target_arch = "wasm32", feature = "web") },
        with_testing: { any(test, feature = "test") },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
        with_dynamodb: { all(not(target_arch = "wasm32"), feature = "dynamodb") },
        with_indexeddb: { all(web, feature = "indexeddb") },
        with_rocksdb: { all(not(target_arch = "wasm32"), feature = "rocksdb") },
        with_scylladb: { all(not(target_arch = "wasm32"), feature = "scylladb") },
    };
}
