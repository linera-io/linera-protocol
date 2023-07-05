// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Primitive types [supported by
//! WebAssembly](https://webassembly.github.io/spec/core/syntax/types.html).
//!
//! These are types that the WebAssembly virtual machine can operate with. More importantly, these
//! are the types that can be used in function interfaces as parameter types and return types.

use super::SimpleType;

/// Primitive types supported by WebAssembly.
pub trait FlatType: SimpleType<Flat = Self> {}

impl FlatType for i32 {}
impl FlatType for i64 {}
impl FlatType for f32 {}
impl FlatType for f64 {}
