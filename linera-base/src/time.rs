// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Abstractions over time that can be used natively or on the Web.
 */

cfg_if::cfg_if! {
    if #[cfg(web)] {
        // This must remain conditional as otherwise it pulls in JavaScript symbols
        // on-chain (on any Wasm target).
        pub use web_time::*;
        pub use linera_kywasmtime as timer;
    } else {
        pub use std::time::*;
        pub use tokio::time as timer;
    }
}
