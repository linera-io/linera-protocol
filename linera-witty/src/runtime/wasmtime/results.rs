// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of Wasmtime function result types.

use frunk::{hlist, hlist_pat, HList};
use wasmtime::{WasmResults, WasmTy};

use crate::{memory_layout::FlatLayout, primitive_types::FlatType};

/// Conversions between flat layouts and Wasmtime function result types.
pub trait WasmtimeResults: FlatLayout {
    /// The type Wasmtime uses to represent the results.
    type Results: WasmResults;

    /// Converts from Wasmtime's representation into a flat layout.
    fn from_wasmtime(results: Self::Results) -> Self;

    /// Converts from this flat layout into Wasmtime's representation.
    fn into_wasmtime(self) -> Self::Results;
}

impl WasmtimeResults for HList![] {
    type Results = ();

    fn from_wasmtime((): Self::Results) -> Self::Flat {
        hlist![]
    }

    fn into_wasmtime(self) -> Self::Results {}
}

impl<T> WasmtimeResults for HList![T]
where
    T: FlatType + WasmTy,
{
    type Results = T;

    fn from_wasmtime(value: Self::Results) -> Self {
        hlist![value]
    }

    fn into_wasmtime(self) -> Self::Results {
        let hlist_pat![value] = self;
        value
    }
}
