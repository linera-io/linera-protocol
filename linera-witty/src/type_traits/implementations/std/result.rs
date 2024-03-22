// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`Result`] type.

use std::borrow::Cow;

use frunk::{hlist, hlist_pat, HCons, HList};

use crate::{
    GuestPointer, InstanceWithMemory, JoinFlatLayouts, Layout, Memory, Merge, Runtime,
    RuntimeError, RuntimeMemory, WitLoad, WitStore, WitType,
};

impl<T, E> WitType for Result<T, E>
where
    T: WitType,
    E: WitType,
    T::Layout: Merge<E::Layout>,
    <T::Layout as Merge<E::Layout>>::Output: Layout,
{
    const SIZE: u32 = {
        let ok_alignment = <T::Layout as Layout>::ALIGNMENT;
        let err_alignment = <E::Layout as Layout>::ALIGNMENT;

        let padding = if ok_alignment > err_alignment {
            ok_alignment - 1
        } else {
            err_alignment - 1
        };

        if T::SIZE > E::SIZE {
            1 + padding + T::SIZE
        } else {
            1 + padding + E::SIZE
        }
    };

    type Layout = HCons<i8, <T::Layout as Merge<E::Layout>>::Output>;
    type Dependencies = HList![T, E];

    fn wit_type_name() -> Cow<'static, str> {
        format!("result<{}, {}>", T::wit_type_name(), E::wit_type_name()).into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        // The native `result` type doesn't need to be declared
        "".into()
    }
}

impl<T, E> WitLoad for Result<T, E>
where
    T: WitLoad,
    E: WitLoad,
    T::Layout: Merge<E::Layout>,
    <T::Layout as Merge<E::Layout>>::Output: Layout,
    <T::Layout as Layout>::Flat:
        JoinFlatLayouts<<<T::Layout as Merge<E::Layout>>::Output as Layout>::Flat>,
    <E::Layout as Layout>::Flat:
        JoinFlatLayouts<<<T::Layout as Merge<E::Layout>>::Output as Layout>::Flat>,
{
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let is_err = bool::load(memory, location)?;
        let location = location
            .after::<bool>()
            .after_padding_for::<T>()
            .after_padding_for::<E>();

        match is_err {
            true => Ok(Err(E::load(memory, location)?)),
            false => Ok(Ok(T::load(memory, location)?)),
        }
    }

    fn lift_from<Instance>(
        hlist_pat![is_err, ...value_layout]: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let is_err = bool::lift_from(hlist![is_err], memory)?;

        match is_err {
            false => Ok(Ok(T::lift_from(
                JoinFlatLayouts::from_joined(value_layout),
                memory,
            )?)),
            true => Ok(Err(E::lift_from(
                JoinFlatLayouts::from_joined(value_layout),
                memory,
            )?)),
        }
    }
}

impl<T, E> WitStore for Result<T, E>
where
    T: WitStore,
    E: WitStore,
    T::Layout: Merge<E::Layout>,
    <T::Layout as Merge<E::Layout>>::Output: Layout,
    <T::Layout as Layout>::Flat:
        JoinFlatLayouts<<<T::Layout as Merge<E::Layout>>::Output as Layout>::Flat>,
    <E::Layout as Layout>::Flat:
        JoinFlatLayouts<<<T::Layout as Merge<E::Layout>>::Output as Layout>::Flat>,
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
        let data_location = location
            .after::<bool>()
            .after_padding_for::<T>()
            .after_padding_for::<E>();

        match self {
            Ok(value) => {
                false.store(memory, location)?;
                value.store(memory, data_location)
            }
            Err(error) => {
                true.store(memory, location)?;
                error.store(memory, data_location)
            }
        }
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        match self {
            Ok(value) => Ok(false.lower(memory)? + value.lower(memory)?.into_joined()),
            Err(error) => Ok(true.lower(memory)? + error.lower(memory)?.into_joined()),
        }
    }
}
