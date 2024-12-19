// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the tuple types.

use std::borrow::Cow;

use frunk::{hlist, hlist_pat, HList};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

/// Implement [`WitType`], [`WitLoad`] and [`WitStore`].
///
/// When implementing [`WitStore`] for tuples, it's necessary to deconstruct the tuple and rebuild
/// it as a heterogeneous list. However, because the methods receive `&self`, the deconstruction
/// leads to references to the elements. Therefore an extra constraint is necessary, which is that
/// the reference also implements [`WitStore`] and that the layout is the same as the referenced
/// type.
///
/// Using this clause for the unit type leads to a compiler error, because it tries to match the
/// layout type to itself, which the compiler doesn't handle correctly. The solution to this is to
/// only add the clause for the implementations that have elements.
macro_rules! impl_wit_traits {
    () => {
        impl_wit_traits_with_borrow_store_clause!(;);
    };

    ($( $names:ident : $types:ident ),*) => {
        impl_wit_traits_with_borrow_store_clause!(
            $( $names: $types ),* ;
            for<'a> HList![$( &'a $types ),*]:
                WitType<Layout = <HList![$( $types ),*] as WitType>::Layout> + WitStore,
        );
    };
}

/// Implement [`WitType`], [`WitLoad`] and [`WitStore`], using the optional extra where clause.
///
/// See [`impl_wit_traits`] above for why the extra clause is optional and can't be used with the
/// implementation for the unit type.
macro_rules! impl_wit_traits_with_borrow_store_clause {
    ($( $names:ident : $types:ident ),* ; $( $borrow_store_clause:tt )*) => {
        impl<$( $types ),*> WitType for ($( $types, )*)
        where
            $( $types: WitType, )*
            HList![$( $types ),*]: WitType,
        {
            const SIZE: u32 = <HList![$( $types ),*] as WitType>::SIZE;

            type Layout = <HList![$( $types ),*] as WitType>::Layout;
            type Dependencies = HList![$( $types ),*];

            fn wit_type_name() -> Cow<'static, str> {
                let elements: &[Cow<'static, str>] = &[
                    $( $types::wit_type_name(), )*
                ];

                format!("tuple<{}>", elements.join(", ")).into()
            }

            fn wit_type_declaration() -> Cow<'static, str> {
                // The native `tuple` type doesn't need to be declared
                "".into()
            }
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

        impl<$( $types ),*> WitStore for ($( $types, )*)
        where
            $( $types: WitStore, )*
            HList![$( $types ),*]: WitStore,
            $( $borrow_store_clause )*
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
                let ($( $names, )*) = self;

                hlist![$( $names ),*].store(memory, location)?;

                Ok(())
            }

            fn lower<Instance>(
                &self,
                memory: &mut Memory<'_, Instance>,
            ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
            where
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                let ($( $names, )*) = self;

                hlist![$( $names ),*].lower(memory)
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
