// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        with_rocksdb: { all(not(target_arch = "wasm32"), feature = "rocksdb") },
        with_scylladb: { all(not(target_arch = "wasm32"), feature = "scylladb") },
        with_dynamodb: { all(not(target_arch = "wasm32"), feature = "dynamodb") },
        with_storage_service: { feature = "storage-service" },
    };
}
