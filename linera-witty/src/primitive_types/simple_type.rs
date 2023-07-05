// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fundamental types used by WIT defined by the [Component Model] that map directly to Rust
//! primitive types.
//!
//! These are primitive types that WIT defines how to store in memory.
//!
//! [Component Model]:
//! https://github.com/WebAssembly/component-model/blob/main/design/mvp/Explainer.md#type-definitions

/// Marker trait to prevent [`SimpleType`] to be implemented for other types.
pub trait Sealed {}

/// Primitive fundamental WIT types.
pub trait SimpleType: Default + Sealed + Sized {
    /// Alignment when storing in memory, according to the [canonical ABI].
    ///
    /// [canonical ABI]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#alignment
    const ALIGNMENT: u32;
}

macro_rules! simple_type {
    ($impl_type:ident, $alignment:expr) => {
        impl Sealed for $impl_type {}

        impl SimpleType for $impl_type {
            const ALIGNMENT: u32 = $alignment;
        }
    };
}

simple_type!(bool, 1);
simple_type!(i8, 1);
simple_type!(i16, 2);
simple_type!(i32, 4);
simple_type!(i64, 8);
simple_type!(u8, 1);
simple_type!(u16, 2);
simple_type!(u32, 4);
simple_type!(u64, 8);
simple_type!(f32, 4);
simple_type!(f64, 8);
simple_type!(char, 4);
