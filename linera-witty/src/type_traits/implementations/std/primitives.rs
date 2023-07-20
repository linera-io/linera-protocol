// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for other standard primitive types.

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitType,
};
use frunk::{hlist_pat, HList};

impl WitType for bool {
    const SIZE: u32 = 1;

    type Layout = HList![i8];
}

impl WitLoad for bool {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(u8::load(memory, location)? != 0)
    }

    fn lift_from<Instance>(
        hlist_pat![value]: <Self::Layout as Layout>::Flat,
        _memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(value != 0)
    }
}

impl<'t, T> WitType for &'t T
where
    T: WitType,
{
    const SIZE: u32 = T::SIZE;

    type Layout = T::Layout;
}
