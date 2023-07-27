// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`Option`] type.

use crate::{
    GuestPointer, InstanceWithMemory, JoinFlatLayouts, Layout, Memory, Merge, Runtime,
    RuntimeError, RuntimeMemory, WitLoad, WitType,
};
use frunk::{hlist, hlist_pat, HCons, HNil};

impl<T> WitType for Option<T>
where
    T: WitType,
    HNil: Merge<T::Layout>,
    <HNil as Merge<T::Layout>>::Output: Layout,
{
    const SIZE: u32 = {
        let padding = <T::Layout as Layout>::ALIGNMENT - 1;

        1 + padding + T::SIZE
    };

    type Layout = HCons<i8, <HNil as Merge<T::Layout>>::Output>;
}

impl<T> WitLoad for Option<T>
where
    T: WitLoad,
    HNil: Merge<T::Layout>,
    <HNil as Merge<T::Layout>>::Output: Layout,
    <T::Layout as Layout>::Flat:
        JoinFlatLayouts<<<HNil as Merge<T::Layout>>::Output as Layout>::Flat>,
{
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let is_some = bool::load(memory, location)?;

        match is_some {
            true => Ok(Some(T::load(
                memory,
                location.after::<bool>().after_padding_for::<T>(),
            )?)),
            false => Ok(None),
        }
    }

    fn lift_from<Instance>(
        hlist_pat![is_some, ...value_layout]: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let is_some = bool::lift_from(hlist![is_some], memory)?;

        if is_some {
            Ok(Some(T::lift_from(
                JoinFlatLayouts::from_joined(value_layout),
                memory,
            )?))
        } else {
            Ok(None)
        }
    }
}
