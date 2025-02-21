// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fundamental types used by WIT defined by the [Component Model] that map directly to Rust
//! primitive types.
//!
//! These are primitive types that WIT defines how to store in memory.
//!
//! [Component Model]:
//! https://github.com/WebAssembly/component-model/blob/main/design/mvp/Explainer.md#type-definitions

use super::FlatType;

/// Marker trait to prevent [`SimpleType`] from being implemented for other types.
pub trait Sealed {}

/// Primitive fundamental WIT types.
pub trait SimpleType: Default + Sealed + Sized {
    /// Alignment when storing in memory, according to the [canonical ABI].
    ///
    /// [canonical ABI]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#alignment
    const ALIGNMENT: u32;

    /// The underlying WebAssembly type used when flattening this type.
    type Flat: FlatType;

    /// Flattens this type into a [`FlatType`] that's natively supported by WebAssembly.
    fn flatten(self) -> Self::Flat;

    /// Unflattens this type from its [`FlatType`] representation.
    fn unflatten_from(flat: Self::Flat) -> Self;
}

macro_rules! simple_type {
    ($impl_type:ident -> $flat:ty, $alignment:expr) => {
        simple_type!($impl_type -> $flat, $alignment, { |flat| flat as $impl_type });
    };

    ($impl_type:ident -> $flat:ty, $alignment:expr, $unflatten:tt) => {
        impl Sealed for $impl_type {}

        impl SimpleType for $impl_type {
            const ALIGNMENT: u32 = $alignment;

            type Flat = $flat;

            fn flatten(self) -> Self::Flat {
                self as $flat
            }

            fn unflatten_from(flat: Self::Flat) -> Self {
                let unflatten = $unflatten;
                unflatten(flat)
            }
        }
    };
}

simple_type!(bool -> i32, 1, { |flat| flat != 0 });
simple_type!(i8 -> i32, 1);
simple_type!(i16 -> i32, 2);
simple_type!(i32 -> i32, 4);
simple_type!(i64 -> i64, 8);
simple_type!(u8 -> i32, 1);
simple_type!(u16 -> i32, 2);
simple_type!(u32 -> i32, 4);
simple_type!(u64 -> i64, 8);
simple_type!(f32 -> f32, 4);
simple_type!(f64 -> f64, 8);
simple_type!(char -> i32, 4,
    { |flat| char::from_u32(flat as u32).expect("Attempt to unflatten an invalid `char`") }
);
