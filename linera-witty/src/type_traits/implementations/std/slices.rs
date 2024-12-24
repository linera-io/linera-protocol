// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for slice types.

use std::borrow::Cow;

use frunk::{hlist, HList};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitStore, WitType,
};

impl<T> WitType for [T]
where
    T: WitType,
{
    const SIZE: u32 = 8;

    type Layout = HList![i32, i32];
    type Dependencies = HList![T];

    fn wit_type_name() -> Cow<'static, str> {
        format!("list<{}>", T::wit_type_name()).into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        // The native `list` type doesn't need to be declared
        "".into()
    }
}

impl<T> WitStore for [T]
where
    T: WitStore,
{
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let length = u32::try_from(self.len())?;
        let size = length * T::SIZE;

        let destination = memory.allocate(size, <T::Layout as Layout>::ALIGNMENT)?;

        destination.store(memory, location)?;
        length.store(memory, location.after::<GuestPointer>())?;

        self.iter()
            .zip(0..)
            .try_for_each(|(element, index)| element.store(memory, destination.index::<T>(index)))
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::Layout, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let length = u32::try_from(self.len())?;
        let size = length * T::SIZE;

        let destination = memory.allocate(size, <T::Layout as Layout>::ALIGNMENT)?;

        self.iter().zip(0..).try_for_each(|(element, index)| {
            element.store(memory, destination.index::<T>(index))
        })?;

        Ok(destination.lower(memory)? + hlist![length as i32])
    }
}

impl<T> WitType for Box<[T]>
where
    T: WitType,
{
    const SIZE: u32 = <[T] as WitType>::SIZE;

    type Layout = <[T] as WitType>::Layout;
    type Dependencies = <[T] as WitType>::Dependencies;

    fn wit_type_name() -> Cow<'static, str> {
        <[T] as WitType>::wit_type_name()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        <[T] as WitType>::wit_type_declaration()
    }
}
