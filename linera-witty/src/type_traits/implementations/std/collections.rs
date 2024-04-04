// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the Rust collection types.

use std::{borrow::Cow, collections::BTreeMap};

use frunk::HList;

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

impl<K, V> WitType for BTreeMap<K, V>
where
    K: WitType,
    V: WitType,
    (K, V): WitType,
{
    const SIZE: u32 = <Vec<(K, V)> as WitType>::SIZE;

    type Layout = <Vec<(K, V)> as WitType>::Layout;
    type Dependencies = HList![K, V];

    fn wit_type_name() -> Cow<'static, str> {
        <Vec<(K, V)> as WitType>::wit_type_name()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        <Vec<(K, V)> as WitType>::wit_type_declaration()
    }
}

impl<K, V> WitLoad for BTreeMap<K, V>
where
    K: WitType + Ord,
    V: WitType,
    (K, V): WitLoad,
{
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let entries = <Vec<(K, V)> as WitLoad>::load(memory, location)?;
        Ok(entries.into_iter().collect())
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let entries = <Vec<(K, V)> as WitLoad>::lift_from(flat_layout, memory)?;
        Ok(entries.into_iter().collect())
    }
}

impl<K, V> WitStore for BTreeMap<K, V>
where
    K: WitType,
    V: WitType,
    (K, V): WitStore,
    for<'a> (&'a K, &'a V): WitStore,
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
        let entries = self.iter().collect::<Vec<(&K, &V)>>();
        entries.store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::Layout, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let entries = self.iter().collect::<Vec<(&K, &V)>>();
        entries.lower(memory)
    }
}
