// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        web: { all(target_arch = "wasm32", feature = "web") },
        with_storage: { any(
            feature = "scylladb",
            feature = "rocksdb",
            feature = "dynamodb",
            feature = "storage-service"
        ) },
        with_persist: { any(feature = "fs", with_indexed_db) },
        with_indexed_db: { all(web, feature = "indexed-db") },
        with_testing: { any(test, feature = "test") },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
        with_dynamodb: { all(not(target_arch = "wasm32"), feature = "dynamodb") },
        with_test_dynamodb: { all(not(target_arch = "wasm32"), feature = "test-dynamodb") },
        with_rocksdb: { all(not(target_arch = "wasm32"), feature = "rocksdb") },
        with_test_rocksdb: { all(not(target_arch = "wasm32"), feature = "test-rocksdb") },
        with_scylladb: { all(not(target_arch = "wasm32"), feature = "scylladb") },
        with_test_scylladb: { all(not(target_arch = "wasm32"), feature = "test-scylladb") },
        with_storage_service: { all(not(target_arch = "wasm32"), feature = "storage-service") },
        with_test_storage_service: { all(not(target_arch = "wasm32"), feature = "test-storage-service") },
    };
}
