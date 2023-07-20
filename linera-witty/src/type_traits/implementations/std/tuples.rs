// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the tuple types.

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitType,
};
use frunk::{hlist_pat, HList};

macro_rules! impl_wit_traits {
    ($( $names:ident : $types:ident ),*) => {
        impl<$( $types ),*> WitType for ($( $types, )*)
        where
            $( $types: WitType, )*
            HList![$( $types ),*]: WitType,
        {
            const SIZE: u32 = <HList![$( $types ),*] as WitType>::SIZE;

            type Layout = <HList![$( $types ),*] as WitType>::Layout;
        }

        impl<$( $types ),*> WitLoad for ($( $types, )*)
        where
            $( $types: WitLoad, )*
            HList![$( $types ),*]: WitLoad,
        {
            fn load<Instance>(
                memory: &Memory<'_, Instance>,
                location: GuestPointer,
            ) -> Result<Self, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                let hlist_pat![$( $names, )*] =
                    <HList![$( $types, )*] as WitLoad>::load(memory, location)?;

                Ok(($( $names, )*))
            }

            fn lift_from<Instance>(
                layout: <Self::Layout as Layout>::Flat,
                memory: &Memory<'_, Instance>,
            ) -> Result<Self, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                let hlist_pat![$( $names, )*] =
                    <HList![$( $types, )*] as WitLoad>::lift_from(layout, memory)?;

                Ok(($( $names, )*))
            }
        }
    };
}

repeat_macro!(
    impl_wit_traits =>
    a: A,
    b: B,
    c: C,
    d: D,
    e: E,
    f: F,
    g: G,
    h: H,
    i: I,
    j: J,
    k: K,
    l: L,
    m: M,
    n: N,
    o: O,
    p: P,
    q: Q,
    r: R,
    s: S,
    t: T,
    u: U,
    v: V,
    w: W,
    x: X,
    y: Y,
    z: Z,
);
