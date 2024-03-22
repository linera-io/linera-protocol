// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of Wasmer function result types.

use frunk::{hlist, hlist_pat, HList};
use wasmer::{FromToNativeWasmType, WasmTypeList};

use crate::{memory_layout::FlatLayout, primitive_types::FlatType};

/// Conversions between flat layouts and Wasmer function result types.
pub trait WasmerResults: FlatLayout {
    /// The type Wasmer uses to represent the results.
    type Results: WasmTypeList;

    /// Converts from Wasmer's representation into a flat layout.
    fn from_wasmer(results: Self::Results) -> Self;

    /// Converts from this flat layout into Wasmer's representation.
    fn into_wasmer(self) -> Self::Results;
}

impl WasmerResults for HList![] {
    type Results = ();

    fn from_wasmer((): Self::Results) -> Self::Flat {
        hlist![]
    }

    fn into_wasmer(self) -> Self::Results {}
}

impl<T> WasmerResults for HList![T]
where
    T: FlatType + FromToNativeWasmType,
{
    type Results = T;

    fn from_wasmer(value: Self::Results) -> Self {
        hlist![value]
    }

    fn into_wasmer(self) -> Self::Results {
        let hlist_pat![value] = self;
        value
    }
}
