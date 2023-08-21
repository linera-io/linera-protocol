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

mod exported_function_interface;
mod imported_function_interface;
mod memory_layout;
mod primitive_types;
mod runtime;
mod type_traits;
mod util;

#[cfg(feature = "wasmer")]
pub use self::runtime::wasmer;
#[cfg(feature = "wasmtime")]
pub use self::runtime::wasmtime;
#[cfg(any(test, feature = "test"))]
pub use self::runtime::{MockExportedFunction, MockInstance, MockRuntime};
pub use self::{
    exported_function_interface::{ExportFunction, ExportTo, ExportedFunctionInterface},
    imported_function_interface::ImportedFunctionInterface,
    memory_layout::{JoinFlatLayouts, Layout},
    runtime::{
        GuestPointer, Instance, InstanceWithFunction, InstanceWithMemory, Memory, Runtime,
        RuntimeError, RuntimeMemory,
    },
    type_traits::{WitLoad, WitStore, WitType},
    util::{Merge, Split},
};
pub use frunk::{hlist, hlist::HList, hlist_pat, HCons, HList, HNil};
#[cfg(all(feature = "macros", any(feature = "wasmer", feature = "wasmtime")))]
pub use linera_witty_macros::wit_export;
#[cfg(feature = "macros")]
pub use linera_witty_macros::{wit_import, WitLoad, WitStore, WitType};
