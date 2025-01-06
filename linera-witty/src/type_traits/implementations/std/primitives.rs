// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for other standard primitive types.

use std::borrow::Cow;

use frunk::{hlist, hlist_pat, HList};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

impl WitType for bool {
    const SIZE: u32 = 1;

    type Layout = HList![i8];
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "bool".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        // Primitive types don't need to be declared
        "".into()
    }
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

impl WitStore for bool {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let value: u8 = if *self { 1 } else { 0 };
        value.store(memory, location)
    }

    fn lower<Instance>(
        &self,
        _memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(hlist![i32::from(*self)])
    }
}

impl<'t, T> WitType for &'t T
where
    T: WitType + ?Sized,
{
    const SIZE: u32 = T::SIZE;

    type Layout = T::Layout;
    type Dependencies = T::Dependencies;

    fn wit_type_name() -> Cow<'static, str> {
        T::wit_type_name()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        T::wit_type_declaration()
    }
}

impl<'t, T> WitStore for &'t T
where
    T: WitStore + ?Sized,
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
        T::store(self, memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        T::lower(self, memory)
    }
}
