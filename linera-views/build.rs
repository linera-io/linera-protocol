// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        web: { all(target_arch = "wasm32", feature = "web") },
        with_testing: { any(test, feature = "test") },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },

        with_dynamodb: { all(not(target_arch = "wasm32"), feature = "dynamodb") },
        with_test_dynamodb: { all(not(target_arch = "wasm32"), feature = "test-dynamodb") },
        with_indexeddb: { all(web, feature = "indexeddb") },
        with_test_indexeddb: { all(web, feature = "test-indexeddb") },
        with_rocksdb: { all(not(target_arch = "wasm32"), feature = "rocksdb") },
        with_test_rocksdb: { all(not(target_arch = "wasm32"), feature = "test-rocksdb") },
        with_scylladb: { all(not(target_arch = "wasm32"), feature = "scylladb") },
        with_test_scylladb: { all(not(target_arch = "wasm32"), feature = "test-scylladb") },
    };
}
