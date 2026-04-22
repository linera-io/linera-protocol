// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        with_revm: { feature = "revm" },
        with_testing: { any(test, feature = "test") },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
    };
}
