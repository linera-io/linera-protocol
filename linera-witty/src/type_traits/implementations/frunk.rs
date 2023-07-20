// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for types from the standard library.

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory, Split,
    WitLoad, WitType,
};
use frunk::{HCons, HNil};
use std::ops::Add;

impl WitType for HNil {
    const SIZE: u32 = 0;

    type Layout = HNil;
}

impl WitLoad for HNil {
    fn load<Instance>(
        _memory: &Memory<'_, Instance>,
        _location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
    {
        Ok(HNil)
    }

    fn lift_from<Instance>(
        HNil: <Self::Layout as Layout>::Flat,
        _memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
    {
        Ok(HNil)
    }
}

impl<Head, Tail> WitType for HCons<Head, Tail>
where
    Head: WitType,
    Tail: WitType,
    Head::Layout: Add<Tail::Layout>,
    <Head::Layout as Add<Tail::Layout>>::Output: Layout,
{
    const SIZE: u32 = Head::SIZE + Tail::SIZE;

    type Layout = <Head::Layout as Add<Tail::Layout>>::Output;
}

impl<Head, Tail> WitLoad for HCons<Head, Tail>
where
    Head: WitLoad,
    Tail: WitLoad,
    Head::Layout: Add<Tail::Layout>,
    <Head::Layout as Add<Tail::Layout>>::Output: Layout,
    <Self::Layout as Layout>::Flat:
        Split<<Head::Layout as Layout>::Flat, Remainder = <Tail::Layout as Layout>::Flat>,
{
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(HCons {
            head: Head::load(memory, location)?,
            tail: Tail::load(memory, location.after::<Head>())?,
        })
    }

    fn lift_from<Instance>(
        layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (head_layout, tail_layout) = layout.split();

        Ok(HCons {
            head: Head::lift_from(head_layout, memory)?,
            tail: Tail::lift_from(tail_layout, memory)?,
        })
    }
}
