// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`PhantomData`] type.

use std::{borrow::Cow, marker::PhantomData};

use frunk::{hlist, HList};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

impl<T> WitType for PhantomData<T> {
    const SIZE: u32 = 0;

    type Layout = HList![];
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "unit".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        // The `unit` type used doesn't need to be declared
        "".into()
    }
}

impl<T> WitLoad for PhantomData<T> {
    fn load<Instance>(
        _memory: &Memory<'_, Instance>,
        _location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(PhantomData)
    }

    fn lift_from<Instance>(
        _flat_layout: <Self::Layout as Layout>::Flat,
        _memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(PhantomData)
    }
}

impl<T> WitStore for PhantomData<T> {
    fn store<Instance>(
        &self,
        _memory: &mut Memory<'_, Instance>,
        _location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(())
    }

    fn lower<Instance>(
        &self,
        _memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::Layout, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(hlist![])
    }
}
