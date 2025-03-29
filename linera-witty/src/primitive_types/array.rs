// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use crate::{
    GuestPointer, HList, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

impl WitType for [u8; 20] {
    const SIZE: u32 = <(u64, u64, u64) as WitType>::SIZE;
    type Layout = <(u64, u64, u64) as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "array20".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        concat!(
            "    record array20 {\n",
            "        part1: u64,\n",
            "        part2: u64,\n",
            "        part3: u64,\n",
            "    }\n",
        )
        .into()
    }
}

impl WitLoad for [u8; 20] {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3): (u64, u64, u64) = WitLoad::load(memory, location)?;
        let mut dest = [0u8; 20];
        dest[0..8].copy_from_slice(&part1.to_be_bytes());
        dest[8..16].copy_from_slice(&part2.to_be_bytes());
        dest[16..20].copy_from_slice(&part3.to_be_bytes());
        Ok(dest)
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as crate::Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3): (u64, u64, u64) = WitLoad::lift_from(flat_layout, memory)?;
        let mut dest = [0u8; 20];
        dest[0..8].copy_from_slice(&part1.to_be_bytes());
        dest[8..16].copy_from_slice(&part2.to_be_bytes());
        dest[16..20].copy_from_slice(&part3.to_be_bytes());
        Ok(dest)
    }
}

impl WitStore for [u8; 20] {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let part1 = u64::from_be_bytes(self[0..8].try_into().unwrap());
        let part2 = u64::from_be_bytes(self[8..16].try_into().unwrap());
        let part3 = u64::from_be_bytes(self[16..20].try_into().unwrap());
        (part1, part2, part3).store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let part1 = u64::from_be_bytes(self[0..8].try_into().unwrap());
        let part2 = u64::from_be_bytes(self[8..16].try_into().unwrap());
        let part3 = u64::from_be_bytes(self[16..20].try_into().unwrap());
        (part1, part2, part3).lower(memory)
    }
}
