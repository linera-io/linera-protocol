// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Object-safe conversion traits.
*/

/// An object-safe version of `std::convert::Into`.
pub trait DynInto<To> {
    /// Converts a boxed object into the target type.
    fn into_box(self: Box<Self>) -> To;
}

impl<To, From: Into<To>> DynInto<To> for From {
    fn into_box(self: Box<From>) -> To {
        (*self).into()
    }
}
