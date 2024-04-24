// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for types from the [`log`] crate.

use std::borrow::Cow;

use frunk::HList;
use num_bigint::BigUint;

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

impl WitType for BigUint {
    const SIZE: u32 = <Vec<u8> as WitType>::SIZE;

    type Layout = <Vec<u8> as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        // We encode the BigUint as a vector of u8.
        "list<u8>".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        // No type needed
        "".into()
    }
}

impl WitLoad for BigUint {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let entries = <Vec<u8> as WitLoad>::load(memory, location)?;
        let value = BigUint::from_bytes_be(&entries);
        Ok(value)
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let entries = <Vec<u8> as WitLoad>::lift_from(flat_layout, memory)?;
        let value = BigUint::from_bytes_be(&entries);
        Ok(value)
    }
}

impl WitStore for BigUint {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let entries = self.to_bytes_be();
        entries.store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let entries = self.to_bytes_be();
        entries.lower(memory)
    }
}
