// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the tuple types.

use crate::WitType;
use frunk::HList;

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
