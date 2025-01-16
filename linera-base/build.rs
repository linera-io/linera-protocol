// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        web: { all(target_arch = "wasm32", feature = "web") },
        chain: { all(target_arch = "wasm32", not(web)) },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
        with_reqwest: { feature = "reqwest" },
        with_testing: { any(test, feature = "test") },

        // the old version of `getrandom` we pin here is available on all targets, but
        // using it will panic if no suitable source of entropy is found
        with_getrandom: { any(web, not(target_arch = "wasm32")) },
    };
}
