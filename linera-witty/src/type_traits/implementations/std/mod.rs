// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for types from the standard library.

/// A macro to implement [`WitType`][super::WitType], [`WitLoad`][super::WitLoad] and
/// [`WitStore`][super::WitStore] for a simple wrapper type.
///
/// This assumes that:
/// - The type is `$wrapper<T>`
/// - It can be constructed with `$wrapper::new(instance_to_wrap)`
/// - The type implements `Deref<Target = T>`
macro_rules! impl_for_wrapper_type {
    ($wrapper:ident) => {
        impl<T> crate::WitType for $wrapper<T>
        where
            T: crate::WitType,
        {
            const SIZE: u32 = T::SIZE;

            type Layout = T::Layout;
            type Dependencies = T::Dependencies;

            fn wit_type_name() -> ::std::borrow::Cow<'static, str> {
                T::wit_type_name()
            }

            fn wit_type_declaration() -> ::std::borrow::Cow<'static, str> {
                T::wit_type_declaration()
            }
        }

        impl<T> crate::WitLoad for $wrapper<T>
        where
            T: crate::WitLoad,
        {
            fn load<Instance>(
                memory: &crate::Memory<'_, Instance>,
                location: crate::GuestPointer,
            ) -> Result<Self, crate::RuntimeError>
            where
                Instance: crate::InstanceWithMemory,
                <Instance::Runtime as crate::Runtime>::Memory: crate::RuntimeMemory<Instance>,
            {
                T::load(memory, location).map($wrapper::new)
            }

            fn lift_from<Instance>(
                flat_layout: <Self::Layout as crate::Layout>::Flat,
                memory: &crate::Memory<'_, Instance>,
            ) -> Result<Self, crate::RuntimeError>
            where
                Instance: crate::InstanceWithMemory,
                <Instance::Runtime as crate::Runtime>::Memory: crate::RuntimeMemory<Instance>,
            {
                T::lift_from(flat_layout, memory).map($wrapper::new)
            }
        }

        impl<T> crate::WitStore for $wrapper<T>
        where
            T: crate::WitStore,
        {
            fn store<Instance>(
                &self,
                memory: &mut crate::Memory<'_, Instance>,
                location: crate::GuestPointer,
            ) -> Result<(), crate::RuntimeError>
            where
                Instance: crate::InstanceWithMemory,
                <Instance::Runtime as crate::Runtime>::Memory: crate::RuntimeMemory<Instance>,
            {
                <$wrapper<T> as ::std::ops::Deref>::deref(self).store(memory, location)
            }

            fn lower<Instance>(
                &self,
                memory: &mut crate::Memory<'_, Instance>,
            ) -> Result<<Self::Layout as crate::Layout>::Flat, crate::RuntimeError>
            where
                Instance: crate::InstanceWithMemory,
                <Instance::Runtime as crate::Runtime>::Memory: crate::RuntimeMemory<Instance>,
            {
                <$wrapper<T> as ::std::ops::Deref>::deref(self).lower(memory)
            }
        }
    };
}

mod r#box;
mod collections;
mod floats;
mod integers;
mod option;
mod phantom_data;
mod primitives;
mod rc;
mod result;
mod slices;
mod string;
mod sync;
mod time;
mod tuples;
mod vec;
