// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Abstractions over time that can be used natively or on the Web.
 */

cfg_if::cfg_if! {
    if #[cfg(web)] {
        pub use web_time::*;
        pub use wasmtimer::tokio as timer;
    } else {
        pub use std::time::*;
        pub use tokio::time as timer;
    }
}
