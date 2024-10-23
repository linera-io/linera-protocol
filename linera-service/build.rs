// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
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
        with_kubernetes: { all(not(target_arch = "wasm32"), feature = "kubernetes") },
        with_test_kubernetes: { all(not(target_arch = "wasm32"), feature = "test-kubernetes") },
        with_remote_net: { all(not(target_arch = "wasm32"), feature = "remote-net") },
        with_test_remote_net: { all(not(target_arch = "wasm32"), feature = "test-remote-net") },
    };
}
