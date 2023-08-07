// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Primitive types [supported by
//! WebAssembly](https://webassembly.github.io/spec/core/syntax/types.html).
//!
//! These are types that the WebAssembly virtual machine can operate with. More importantly, these
//! are the types that can be used in function interfaces as parameter types and return types.
//!
//! The [Component Model canonical ABI used by WIT][flattening] specifies that when flattening
//! `variant` types, a "joined" flat type must be found for each element in its flat layout (see
//! [`JoinFlatTypes`][`super::JoinFlatTypes`] for more information). The [`FlatType`] trait
//! includes support for the reverse "split" operation.
//!
//! [flattening]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening

use super::SimpleType;

/// Primitive types supported by WebAssembly.
pub trait FlatType: SimpleType<Flat = Self> {
    /// Splits itself from a joined `i32` flat value.
    ///
    /// # Panics
    ///
    /// If this flat type can't be joined into an `i32`.
    fn split_from_i32(joined_i32: i32) -> Self;

    /// Splits itself from a joined `i64` flat value.
    ///
    /// # Panics
    ///
    /// If this flat type can't be joined into an `i64`.
    fn split_from_i64(joined_i64: i64) -> Self;

    /// Splits itself from a joined `f32` flat value.
    ///
    /// # Panics
    ///
    /// If this flat type can't be joined into an `f32`.
    fn split_from_f32(joined_f32: f32) -> Self;

    /// Splits itself from a joined `f64` flat value.
    ///
    /// # Panics
    ///
    /// If this flat type can't be joined into an `f64`.
    fn split_from_f64(joined_f64: f64) -> Self;

    /// Splits off the `Target` flat type from this flat type.
    ///
    /// # Panics
    ///
    /// If the `Target` type can't be joined into this flat type.
    fn split_into<Target: FlatType>(self) -> Target;
}

impl FlatType for i32 {
    fn split_from_i32(joined_i32: i32) -> Self {
        joined_i32
    }

    fn split_from_i64(joined_i64: i64) -> Self {
        joined_i64
            .try_into()
            .expect("Invalid `i32` stored in `i64`")
    }

    fn split_from_f32(_joined_f32: f32) -> Self {
        unreachable!("`i32` is never joined into `f32`");
    }

    fn split_from_f64(_joined_f64: f64) -> Self {
        unreachable!("`i32` is never joined into `f64`");
    }

    fn split_into<Target: FlatType>(self) -> Target {
        Target::split_from_i32(self)
    }
}

impl FlatType for i64 {
    fn split_from_i32(_joined_i32: i32) -> Self {
        unreachable!("`i64` is never joined into `i32`");
    }

    fn split_from_i64(joined_i64: i64) -> Self {
        joined_i64
    }

    fn split_from_f32(_joined_f32: f32) -> Self {
        unreachable!("`i64` is never joined into `f32`");
    }

    fn split_from_f64(_joined_f64: f64) -> Self {
        unreachable!("`i64` is never joined into `f64`");
    }

    fn split_into<Target: FlatType>(self) -> Target {
        Target::split_from_i64(self)
    }
}

impl FlatType for f32 {
    fn split_from_i32(joined_i32: i32) -> Self {
        joined_i32 as f32
    }

    fn split_from_i64(joined_i64: i64) -> Self {
        (joined_i64 as i32) as f32
    }

    fn split_from_f32(joined_f32: f32) -> Self {
        joined_f32
    }

    fn split_from_f64(_joined_f64: f64) -> Self {
        unreachable!("`f32` is never joined into `f64`");
    }

    fn split_into<Target: FlatType>(self) -> Target {
        Target::split_from_f32(self)
    }
}

impl FlatType for f64 {
    fn split_from_i32(_joined_i32: i32) -> Self {
        unreachable!("`f64` is never joined into `i32`");
    }

    fn split_from_i64(joined_i64: i64) -> Self {
        joined_i64 as f64
    }

    fn split_from_f32(_joined_f32: f32) -> Self {
        unreachable!("`f64` is never joined into `f32`");
    }

    fn split_from_f64(joined_f64: f64) -> Self {
        joined_f64
    }

    fn split_into<Target: FlatType>(self) -> Target {
        Target::split_from_f64(self)
    }
}
