// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for float primitives.

use std::borrow::Cow;

use frunk::{hlist, hlist_pat, HList};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

macro_rules! impl_wit_traits {
    ($float:ty, $wit_name:literal, $size:expr) => {
        impl WitType for $float {
            const SIZE: u32 = $size;

            type Layout = HList![$float];
            type Dependencies = HList![];

            fn wit_type_name() -> Cow<'static, str> {
                $wit_name.into()
            }

            fn wit_type_declaration() -> Cow<'static, str> {
                // Primitive types don't need to be declared
                "".into()
            }
        }

        impl WitLoad for $float {
            fn load<Instance>(
                memory: &Memory<'_, Instance>,
                location: GuestPointer,
            ) -> Result<Self, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                let slice = memory.read(location, Self::SIZE)?;
                let bytes = (*slice).try_into().expect("Incorrect number of bytes read");

                Ok(Self::from_le_bytes(bytes))
            }

            fn lift_from<Instance>(
                hlist_pat![value]: <Self::Layout as Layout>::Flat,
                _memory: &Memory<'_, Instance>,
            ) -> Result<Self, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                Ok(value)
            }
        }

        impl WitStore for $float {
            fn store<Instance>(
                &self,
                memory: &mut Memory<'_, Instance>,
                location: GuestPointer,
            ) -> Result<(), RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                memory.write(location, &self.to_le_bytes())
            }

            fn lower<Instance>(
                &self,
                _memory: &mut Memory<'_, Instance>,
            ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                Ok(hlist![*self])
            }
        }
    };
}

impl_wit_traits!(f32, "float32", 4);
impl_wit_traits!(f64, "float64", 8);
