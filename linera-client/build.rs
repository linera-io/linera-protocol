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
        with_persist: { any(feature = "fs", with_local_storage) },
        with_local_storage: { all(web, feature = "local_storage") },
        with_testing: { any(test, feature = "test") },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
    };
}
