// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions with zero-extension.

/// Converts from a type into a wider `Target` type by zero-extending the most significant bits.
pub trait ZeroExtend<Target> {
    /// Converts into the `Target` type by zero-extending the most significant bits.
    fn zero_extend(self) -> Target;
}

impl ZeroExtend<i64> for i32 {
    fn zero_extend(self) -> i64 {
        self as u32 as u64 as i64
    }
}
