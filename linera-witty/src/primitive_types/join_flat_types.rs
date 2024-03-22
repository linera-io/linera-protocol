// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the `join` operator for flattening WIT `variant` types.
//!
//! The [Component Model canonical ABI][flattening] used by WIT specifies that in order to flatten
//! a `variant` type, it is necessary to find common flat types to represent individual elements of
//! the flattened variants. This is called the `join` operator, and is basically finding the
//! widest bit-representation for the two types.
//!
//! [flattening]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening

use either::Either;

use super::FlatType;
use crate::util::ZeroExtend;

/// Trait that allows joining one of two possible flat type values into a common flat type.
pub trait JoinFlatTypes {
    /// The resulting flat type.
    type Flat: FlatType;

    /// Joins a value that's one of two flat types into a single flat type.
    fn join(self) -> Self::Flat;
}

/// Implement [`JoinFlatTypes`] for an [`Either`] type of two flat types.
macro_rules! join_flat_types {
    (
        $( ($left:ty, $right:ty) -> $joined:ty => ($join_left:tt, $join_right:tt $(,)?) ),* $(,)?
    ) => {
        $(
            impl JoinFlatTypes for Either<$left, $right> {
                type Flat = $joined;

                fn join(self) -> Self::Flat {
                    match self {
                        Either::Left(left) => {
                            let join_left = $join_left;
                            join_left(left)
                        }
                        Either::Right(right) => {
                            let join_right = $join_right;
                            join_right(right)
                        }
                    }
                }
            }
        )*
    }
}

join_flat_types!(
    (i32, i32) -> i32 => (
        { |value| value },
        { |value| value },
    ),
    (i32, i64) -> i64 => (
        { |value: i32| value.zero_extend() },
        { |value| value },
    ),
    (i32, f32) -> i32 => (
        { |value| value },
        { |value| value as i32 },
    ),
    (i32, f64) -> i64 => (
        { |value: i32| value.zero_extend() },
        { |value| value as i64 },
    ),

    (i64, i32) -> i64 => (
        { |value| value },
        { |value: i32| value.zero_extend() },
    ),
    (i64, i64) -> i64 => (
        { |value| value },
        { |value| value },
    ),
    (i64, f32) -> i64 => (
        { |value| value },
        { |value| (value as i32).zero_extend() },
    ),
    (i64, f64) -> i64 => (
        { |value| value },
        { |value| value as i64 },
    ),

    (f32, i32) -> i32 => (
        { |value| value as i32 },
        { |value| value },
    ),
    (f32, i64) -> i64 => (
        { |value| (value as i32).zero_extend() },
        { |value| value },
    ),
    (f32, f32) -> f32 => (
        { |value| value },
        { |value| value },
    ),
    (f32, f64) -> i64 => (
        { |value| (value as i32).zero_extend() },
        { |value| value as i64 },
    ),

    (f64, i32) -> i64 => (
        { |value| value as i64 },
        { |value: i32| value.zero_extend() },
    ),
    (f64, i64) -> i64 => (
        { |value| value as i64 },
        { |value| value },
    ),
    (f64, f32) -> i64 => (
        { |value| value as i64 },
        { |value| (value as i32).zero_extend() },
    ),
    (f64, f64) -> f64 => (
        { |value| value },
        { |value| value },
    ),
);

impl<AnyFlatType> JoinFlatTypes for Either<(), AnyFlatType>
where
    AnyFlatType: FlatType,
{
    type Flat = AnyFlatType;

    fn join(self) -> Self::Flat {
        match self {
            Either::Left(()) => AnyFlatType::default(),
            Either::Right(value) => value,
        }
    }
}

impl<AnyFlatType> JoinFlatTypes for Either<AnyFlatType, ()>
where
    AnyFlatType: FlatType,
{
    type Flat = AnyFlatType;

    fn join(self) -> Self::Flat {
        match self {
            Either::Left(value) => value,
            Either::Right(()) => AnyFlatType::default(),
        }
    }
}
