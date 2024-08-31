// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of Wasmer function parameter types.

use frunk::{hlist, hlist_pat, HList};
use wasmer::FromToNativeWasmType;

use crate::{memory_layout::FlatLayout, primitive_types::FlatType};

/// Conversions between flat layouts and Wasmer parameter types.
pub trait WasmerParameters: FlatLayout {
    /// The type Wasmer uses to represent the parameters in a function imported from a guest.
    type ImportParameters;

    /// The type Wasmer uses to represent the parameters in a function exported from a host.
    type ExportParameters;

    /// Converts from this flat layout into Wasmer's representation for functions imported from a
    /// guest.
    fn into_wasmer(self) -> Self::ImportParameters;

    /// Converts from Wasmer's representation for functions exported from the host into this flat
    /// layout.
    fn from_wasmer(parameters: Self::ExportParameters) -> Self;
}

impl WasmerParameters for HList![] {
    type ImportParameters = ();
    type ExportParameters = ();

    fn into_wasmer(self) -> Self::ImportParameters {}

    fn from_wasmer((): Self::ExportParameters) -> Self {
        hlist![]
    }
}

impl<Parameter> WasmerParameters for HList![Parameter]
where
    Parameter: FlatType + FromToNativeWasmType,
{
    type ImportParameters = Parameter;
    type ExportParameters = (Parameter,);

    #[allow(clippy::unused_unit)]
    fn into_wasmer(self) -> Self::ImportParameters {
        let hlist_pat![parameter] = self;

        parameter
    }

    fn from_wasmer((parameter,): Self::ExportParameters) -> Self {
        hlist![parameter]
    }
}

/// Helper macro to implement [`WasmerParameters`] for flat layouts up to the maximum limit.
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
        impl<$( $types ),*> WasmerParameters for HList![$( $types ),*]
        where
            $( $types: FlatType + FromToNativeWasmType, )*
        {
            type ImportParameters = ($( $types, )*);
            type ExportParameters = ($( $types, )*);

            #[allow(clippy::unused_unit)]
            fn into_wasmer(self) -> Self::ImportParameters {
                let hlist_pat![$( $names ),*] = self;

                ($( $names, )*)
            }

            fn from_wasmer(($( $names, )*): Self::ExportParameters) -> Self {
                hlist![$( $names ),*]
            }
        }
    };
}

repeat_macro!(parameters =>
    a: A,
    b: B |
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
