// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the Rust time types.

use std::{borrow::Cow, time::Duration};

use frunk::HList;

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

impl WitType for Duration {
    const SIZE: u32 = <HList![u64, u32] as WitType>::SIZE;

    type Layout = <HList![u64, u32] as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "duration".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        "    type duration = tuple<u64, u32>;".into()
    }
}

impl WitLoad for Duration {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (secs, nanos) = <(u64, u32) as WitLoad>::load(memory, location)?;
        Ok(Duration::new(secs, nanos))
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (secs, nanos) = <(u64, u32) as WitLoad>::lift_from(flat_layout, memory)?;
        Ok(Duration::new(secs, nanos))
    }
}

impl WitStore for Duration {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let secs = self.as_secs();
        let nanos = self.subsec_nanos();

        (secs, nanos).store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let secs = self.as_secs();
        let nanos = self.subsec_nanos();

        (secs, nanos).lower(memory)
    }
}
