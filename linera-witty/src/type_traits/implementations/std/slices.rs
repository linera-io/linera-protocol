// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for slice types.

use std::{borrow::Cow, ops::Deref, rc::Rc, sync::Arc};

use frunk::{hlist, hlist_pat, HList};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

/// A macro to implement [`WitType`], [`WitLoad`] and [`WitStore`] for a slice wrapper type.
///
/// This assumes that:
/// - The type is `$wrapper<[T]>`
/// - The type implements `From<Box<[T]>>`
/// - The type implements `Deref<Target = [T]>`
macro_rules! impl_wit_traits_for_slice_wrapper {
    ($wrapper:ident) => {
        impl_wit_type_as_slice!($wrapper);
        impl_wit_store_as_slice!($wrapper);
        impl_wit_load_as_boxed_slice!($wrapper);
    };
}

/// A macro to implement [`WitType`] for a slice wrapper type.
///
/// This assumes that:
/// - The type is `$wrapper<[T]>`
macro_rules! impl_wit_type_as_slice {
    ($wrapper:ident) => {
        impl<T> WitType for $wrapper<[T]>
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
    };
}

/// A macro to implement [`WitStore`] for a slice wrapper type.
///
/// This assumes that:
/// - The type is `$wrapper<[T]>`
/// - The type implements `Deref<Target = [T]>`
macro_rules! impl_wit_store_as_slice {
    ($wrapper:ident) => {
        impl<T> WitStore for $wrapper<[T]>
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
    };
}

/// A macro to implement [`WitLoad`] for a slice wrapper type.
///
/// This assumes that:
/// - The type is `$wrapper<[T]>`
/// - The type implements `From<Box<[T]>>`
macro_rules! impl_wit_load_as_boxed_slice {
    ($wrapper:ident) => {
        impl<T> WitLoad for $wrapper<[T]>
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
                <Box<[T]> as WitLoad>::load(memory, location).map($wrapper::from)
            }

            fn lift_from<Instance>(
                flat_layout: <Self::Layout as Layout>::Flat,
                memory: &Memory<'_, Instance>,
            ) -> Result<Self, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                <Box<[T]> as WitLoad>::lift_from(flat_layout, memory).map($wrapper::from)
            }
        }
    };
}

impl<T> WitType for [T]
where
    T: WitType,
{
    const SIZE: u32 = 8;

    type Layout = HList![i32, i32];
    type Dependencies = HList![T];

    fn wit_type_name() -> Cow<'static, str> {
        format!("list<{}>", T::wit_type_name()).into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        // The native `list` type doesn't need to be declared
        "".into()
    }
}

impl<T> WitStore for [T]
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
        // There's no need to account for padding between the elements, because the element's size
        // is always aligned:
        // https://github.com/WebAssembly/component-model/blob/cbdd15d9033446558571824af52a78022aaa3f58/design/mvp/CanonicalABI.md#element-size
        let length = u32::try_from(self.len())?;
        let size = length * T::SIZE;

        let destination = memory.allocate(size, <T::Layout as Layout>::ALIGNMENT)?;

        destination.store(memory, location)?;
        length.store(memory, location.after::<GuestPointer>())?;

        self.iter()
            .zip(0..)
            .try_for_each(|(element, index)| element.store(memory, destination.index::<T>(index)))
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::Layout, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        // There's no need to account for padding between the elements, because the element's size
        // is always aligned:
        // https://github.com/WebAssembly/component-model/blob/cbdd15d9033446558571824af52a78022aaa3f58/design/mvp/CanonicalABI.md#element-size
        let length = u32::try_from(self.len())?;
        let size = length * T::SIZE;

        let destination = memory.allocate(size, <T::Layout as Layout>::ALIGNMENT)?;

        self.iter().zip(0..).try_for_each(|(element, index)| {
            element.store(memory, destination.index::<T>(index))
        })?;

        Ok(destination.lower(memory)? + hlist![length as i32])
    }
}

impl_wit_type_as_slice!(Box);
impl_wit_store_as_slice!(Box);

impl<T> WitLoad for Box<[T]>
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
        let address = GuestPointer::load(memory, location)?;
        let length = u32::load(memory, location.after::<GuestPointer>())?;

        (0..length)
            .map(|index| T::load(memory, address.index::<T>(index)))
            .collect()
    }

    fn lift_from<Instance>(
        hlist_pat![address, length]: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let address = GuestPointer(address.try_into()?);
        let length = length as u32;

        (0..length)
            .map(|index| T::load(memory, address.index::<T>(index)))
            .collect()
    }
}

impl_wit_traits_for_slice_wrapper!(Rc);
impl_wit_traits_for_slice_wrapper!(Arc);
