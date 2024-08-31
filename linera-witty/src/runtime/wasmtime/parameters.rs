// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of Wasmtime function parameter types.

use frunk::{hlist, hlist_pat, HList};
use wasmtime::{WasmParams, WasmTy};

use crate::{primitive_types::FlatType, Layout};

/// Conversions between flat layouts and Wasmtime parameter types.
pub trait WasmtimeParameters {
    /// The type Wasmtime uses to represent the parameters.
    type Parameters: WasmParams;

    /// Converts from this flat layout into Wasmtime's representation.
    fn into_wasmtime(self) -> Self::Parameters;

    /// Converts from Wasmtime's representation into a flat layout.
    fn from_wasmtime(parameters: Self::Parameters) -> Self;
}

/// Helper macro to implement [`WasmtimeParameters`] for flat layouts up to the maximum limit.
///
/// The maximum number of parameters is defined by the [canonical
/// ABI](https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening)
/// as the `MAX_FLAT_PARAMS` constant. There is no equivalent constant defined in Witty. Instead,
/// any attempt to use more than the limit should lead to a compiler error. Therefore, this macro
/// only implements the trait up to the limit. The same is done in other parts of the code, like
/// for example in
/// [`FlatHostParameters`][`crate::imported_function_interface::FlatHostParameters`].
macro_rules! parameters {
    ($( $names:ident : $types:ident ),*) => {
        impl<$( $types ),*> WasmtimeParameters for HList![$( $types ),*]
        where
            $( $types: FlatType + WasmTy, )*
        {
            type Parameters = ($( $types, )*);

            #[allow(clippy::unused_unit)]
            fn into_wasmtime(self) -> Self::Parameters {
                let hlist_pat![$( $names ),*] = self;

                ($( $names, )*)
            }

            fn from_wasmtime(($( $names, )*): Self::Parameters) -> Self {
                hlist![$( $names ),*]
            }
        }
    };
}

repeat_macro!(parameters =>
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
    q: Q
);

impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, Rest> WasmtimeParameters for HList![A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, ...Rest]
where
    A: FlatType,
    B: FlatType,
    C: FlatType,
    D: FlatType,
    E: FlatType,
    F: FlatType,
    G: FlatType,
    H: FlatType,
    I: FlatType,
    J: FlatType,
    K: FlatType,
    L: FlatType,
    M: FlatType,
    N: FlatType,
    O: FlatType,
    P: FlatType,
    Q: FlatType,
    R: FlatType,
    Rest: Layout,
{
    type Parameters = (i32,);

    fn into_wasmtime(self) -> Self::Parameters {
        unreachable!("Attempt to convert a list of flat parameters larger than the maximum limit");
    }

    fn from_wasmtime(_: Self::Parameters) -> Self {
        unreachable!(
            "Attempt to convert into a list of flat parameters larger than the maximum limit"
        );
    }
}
