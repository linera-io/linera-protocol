// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`Vec`] type.

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitType,
};
use frunk::{hlist_pat, HList};

impl<T> WitType for Vec<T>
where
    T: WitType,
{
    const SIZE: u32 = 8;

    type Layout = HList![i32, i32];
}

impl<T> WitLoad for Vec<T>
where
    T: WitLoad,
{
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let address = GuestPointer::load(memory, location)?;
        let length = u32::load(memory, location.after::<GuestPointer>())?;

        (0..length)
            .map(|index| T::load(memory, address.index::<T>(index)))
            .collect()
    }

    fn lift_from<Instance>(
        hlist_pat![address, length]: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let address = GuestPointer(address.try_into()?);
        let length = length as u32;

        (0..length)
            .map(|index| T::load(memory, address.index::<T>(index)))
            .collect()
    }
}
