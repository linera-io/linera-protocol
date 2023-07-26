// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Linera Witty
//!
//! This crate allows generating [WIT] files and host side code to interface with WebAssembly guests
//! that adhere to the [WIT] interface format. The source of truth for the generated code and WIT
//! files is the Rust source code.
//!
//! [WIT]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/WIT.md

#![deny(missing_docs)]

#[macro_use]
mod macro_utils;

mod memory_layout;
mod primitive_types;
mod runtime;
mod type_traits;
mod util;

#[cfg(any(test, feature = "test"))]
pub use self::runtime::{FakeInstance, FakeRuntime};
pub use self::{
    memory_layout::Layout,
    runtime::{GuestPointer, InstanceWithMemory, Memory, Runtime, RuntimeError, RuntimeMemory},
    type_traits::{WitLoad, WitStore, WitType},
    util::{Merge, Split},
};
pub use frunk::{hlist, hlist::HList, hlist_pat, HList, HNil};
#[cfg(feature = "macros")]
pub use linera_witty_macros::{WitLoad, WitStore, WitType};
