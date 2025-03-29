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

/// Implementation of Witty traits for EVM types.
mod ethereum;
mod exported_function_interface;
mod imported_function_interface;
mod memory_layout;
mod primitive_types;
mod runtime;
#[cfg(with_testing)]
pub mod test;
mod type_traits;
mod util;
pub mod wit_generation;

pub use frunk::{hlist, hlist::HList, hlist_pat, HCons, HList, HNil};
#[cfg(with_wit_export)]
pub use linera_witty_macros::wit_export;
#[cfg(with_macros)]
pub use linera_witty_macros::{wit_import, WitLoad, WitStore, WitType};

#[cfg(with_wasmer)]
pub use self::runtime::wasmer;
#[cfg(with_wasmtime)]
pub use self::runtime::wasmtime;
#[cfg(with_testing)]
pub use self::runtime::{MockExportedFunction, MockInstance, MockResults, MockRuntime};
pub use self::{
    exported_function_interface::{ExportFunction, ExportTo, ExportedFunctionInterface},
    imported_function_interface::ImportedFunctionInterface,
    memory_layout::{JoinFlatLayouts, Layout},
    runtime::{
        GuestPointer, Instance, InstanceWithFunction, InstanceWithMemory, Memory, Runtime,
        RuntimeError, RuntimeMemory,
    },
    type_traits::{RegisterWitTypes, WitLoad, WitStore, WitType},
    util::{Merge, Split},
};
