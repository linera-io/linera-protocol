// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        web: { all(target_arch = "wasm32", target_os = "unknown") },
        with_testing: { any(test, feature = "test") },
        with_metrics: { all(not(web), feature = "metrics") },
        with_wasmtime: { all(not(web), feature = "wasmtime") },
        with_wasmer: { all(not(web), feature = "wasmer") },
    }
}
