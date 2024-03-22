// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for integer primitives.

use std::borrow::Cow;

use frunk::{hlist, hlist_pat, HList};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

macro_rules! impl_wit_traits {
    ($integer:ty, $wit_name:literal, 1) => {
        impl WitType for $integer {
            const SIZE: u32 = 1;

            type Layout = HList![$integer];
            type Dependencies = HList![];

            fn wit_type_name() -> Cow<'static, str> {
                $wit_name.into()
            }

            fn wit_type_declaration() -> Cow<'static, str> {
                "".into()
            }
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

        impl WitStore for $integer {
            fn store<Instance>(
                &self,
                memory: &mut Memory<'_, Instance>,
                location: GuestPointer,
            ) -> Result<(), RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                memory.write(location, &[*self as u8])
            }

            fn lower<Instance>(
                &self,
                _memory: &mut Memory<'_, Instance>,
            ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                Ok(hlist![*self as i32])
            }
        }
    };

    ($integer:ty, $wit_name:literal, $size:expr, $flat_type:ty) => {
        impl_wit_traits!(
            $integer,
            $wit_name,
            "",
            $size,
            ($integer),
            ($flat_type),
            self -> hlist![*self as $flat_type],
            hlist_pat![value] => value as Self
        );
    };

    (
        $integer:ty,
        $wit_name:literal,
        $wit_declaration:literal,
        $size:expr,
        ($( $simple_types:ty ),*),
        ($( $flat_types:ty ),*),
        $this:ident -> $lower:expr,
        $lift_pattern:pat => $lift:expr
    ) => {
        impl WitType for $integer {
            const SIZE: u32 = $size;

            type Layout = HList![$( $simple_types ),*];
            type Dependencies = HList![];

            fn wit_type_name() -> Cow<'static, str> {
                $wit_name.into()
            }

            fn wit_type_declaration() -> Cow<'static, str> {
                $wit_declaration.into()
            }
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

        impl WitStore for $integer {
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
                &$this,
                _memory: &mut Memory<'_, Instance>,
            ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                Ok($lower)
            }
        }
    };
}

impl_wit_traits!(u8, "u8", 1);
impl_wit_traits!(i8, "s8", 1);
impl_wit_traits!(u16, "u16", 2, i32);
impl_wit_traits!(i16, "s16", 2, i32);
impl_wit_traits!(u32, "u32", 4, i32);
impl_wit_traits!(i32, "s32", 4, i32);
impl_wit_traits!(u64, "u64", 8, i64);
impl_wit_traits!(i64, "s64", 8, i64);

macro_rules! x128_lower {
    ($this:ident) => {
        hlist![
            ($this & ((1 << 64) - 1)) as i64,
            (($this >> 64) & ((1 << 64) - 1)) as i64,
        ]
    };
}

impl_wit_traits!(
    u128,
    "u128",
    "    type u128 = tuple<u64, u64>;\n",
    16,
    (u64, u64),
    (i64, i64),
    self -> x128_lower!(self),
    hlist_pat![least_significant_bytes, most_significant_bytes] => {
        ((most_significant_bytes as Self) << 64)
        | (least_significant_bytes as Self & ((1 << 64) - 1))
    }
);

impl_wit_traits!(
    i128,
    "s128",
    "    type s128 = tuple<s64, s64>;\n",
    16,
    (i64, i64),
    (i64, i64),
    self -> x128_lower!(self),
    hlist_pat![least_significant_bytes, most_significant_bytes] => {
        ((most_significant_bytes as Self) << 64)
        | (least_significant_bytes as Self & ((1 << 64) - 1))
    }
);
