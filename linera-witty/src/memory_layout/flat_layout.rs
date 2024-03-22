// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of the layout of complex types as a sequence of native WebAssembly types.

use frunk::{HCons, HNil};

use super::Layout;
use crate::primitive_types::FlatType;

/// Representation of the layout of complex types as a sequence of native WebAssembly types.
///
/// This allows laying out complex types as a sequence of WebAssembly types that can represent the
/// parameters or the return list of a function. WIT uses this as an optimization to pass complex
/// types as multiple native WebAssembly parameters.
pub trait FlatLayout: Default + Layout<Flat = Self> {}

impl FlatLayout for HNil {}

impl<Head, Tail> FlatLayout for HCons<Head, Tail>
where
    Head: FlatType,
    Tail: FlatLayout,
{
}
