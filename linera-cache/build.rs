// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        web: { target_arch = "wasm32" },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
        with_testing: { any(test, feature = "test") },
    };
}
