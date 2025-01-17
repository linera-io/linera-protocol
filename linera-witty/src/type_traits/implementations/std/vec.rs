// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`Vec`] type.

use std::{borrow::Cow, ops::Deref};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

impl<T> WitType for Vec<T>
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
        Box::load(memory, location).map(Vec::from)
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Box::lift_from(flat_layout, memory).map(Vec::from)
    }
}

impl<T> WitStore for Vec<T>
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
        self.deref().store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::Layout, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        self.deref().lower(memory)
    }
}
