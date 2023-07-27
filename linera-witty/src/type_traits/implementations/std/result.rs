// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`Result`] type.

use crate::{Layout, Merge, WitType};
use frunk::HCons;

impl<T, E> WitType for Result<T, E>
where
    T: WitType,
    E: WitType,
    T::Layout: Merge<E::Layout>,
    <T::Layout as Merge<E::Layout>>::Output: Layout,
{
    const SIZE: u32 = {
        let ok_alignment = <T::Layout as Layout>::ALIGNMENT;
        let err_alignment = <E::Layout as Layout>::ALIGNMENT;

        let padding = if ok_alignment > err_alignment {
            ok_alignment - 1
        } else {
            err_alignment - 1
        };

        if T::SIZE > E::SIZE {
            1 + padding + T::SIZE
        } else {
            1 + padding + E::SIZE
        }
    };

    type Layout = HCons<i8, <T::Layout as Merge<E::Layout>>::Output>;
}
