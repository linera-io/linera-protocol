// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for float primitives.

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitType,
};
use frunk::{hlist_pat, HList};

macro_rules! impl_wit_traits {
    ($float:ty, $size:expr) => {
        impl WitType for $float {
            const SIZE: u32 = $size;

            type Layout = HList![$float];
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
    };
}

impl_wit_traits!(f32, 4);
impl_wit_traits!(f64, 8);
