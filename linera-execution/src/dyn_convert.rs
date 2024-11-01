// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
TODO document
 */

pub trait DynInto<To> {
    fn into_box(self: Box<Self>) -> To;
}

impl<To, From: Into<To>> DynInto<To> for From {
    fn into_box(self: Box<From>) -> To {
        (*self).into()
    }
}
