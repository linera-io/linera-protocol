// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for integer primitives.

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitType,
};
use frunk::{hlist_pat, HList};

macro_rules! impl_wit_traits {
    ($integer:ty, 1) => {
        impl WitType for $integer {
            const SIZE: u32 = 1;

            type Layout = HList![$integer];
        }

        impl WitLoad for $integer {
            fn load<Instance>(
                memory: &Memory<'_, Instance>,
                location: GuestPointer,
            ) -> Result<Self, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                let slice = memory.read(location, 1)?;
                Ok(slice[0] as $integer)
            }

            fn lift_from<Instance>(
                hlist_pat![value]: <Self::Layout as Layout>::Flat,
                _memory: &Memory<'_, Instance>,
            ) -> Result<Self, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                Ok(value as $integer)
            }
        }
    };

    ($integer:ty, $size:expr) => {
        impl_wit_traits!(
            $integer,
            $size,
            ($integer),
            hlist_pat![value] => value as Self
        );
    };

    (
        $integer:ty,
        $size:expr,
        ($( $simple_types:ty ),*),
        $lift_pattern:pat => $lift:expr
    ) => {
        impl WitType for $integer {
            const SIZE: u32 = $size;

            type Layout = HList![$( $simple_types ),*];
        }

        impl WitLoad for $integer {
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
                $lift_pattern: <Self::Layout as Layout>::Flat,
                _memory: &Memory<'_, Instance>,
            ) -> Result<Self, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                Ok($lift)
            }
        }
    };
}

impl_wit_traits!(u8, 1);
impl_wit_traits!(i8, 1);
impl_wit_traits!(u16, 2);
impl_wit_traits!(i16, 2);
impl_wit_traits!(u32, 4);
impl_wit_traits!(i32, 4);
impl_wit_traits!(u64, 8);
impl_wit_traits!(i64, 8);

impl_wit_traits!(
    u128,
    16,
    (u64, u64),
    hlist_pat![least_significant_bytes, most_significant_bytes] => {
        ((most_significant_bytes as Self) << 64)
        | (least_significant_bytes as Self & ((1 << 64) - 1))
    }
);

impl_wit_traits!(
    i128,
    16,
    (i64, i64),
    hlist_pat![least_significant_bytes, most_significant_bytes] => {
        ((most_significant_bytes as Self) << 64)
        | (least_significant_bytes as Self & ((1 << 64) - 1))
    }
);
