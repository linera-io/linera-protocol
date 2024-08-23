// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of [`InstanceWithFunction`] for Wasmer instances.

use frunk::{hlist_pat, HList};
use wasmer::{AsStoreRef, Extern, FromToNativeWasmType, NativeWasmTypeInto, TypedFunction};

use super::{
    parameters::WasmerParameters, results::WasmerResults, EntrypointInstance, ReentrantInstance,
};
use crate::{
    memory_layout::FlatLayout, primitive_types::FlatType, InstanceWithFunction, Runtime,
    RuntimeError,
};

/// Implements [`InstanceWithFunction`] for functions with the provided amount of parameters for
/// the [`EntrypointInstance`] and [`ReentrantInstance`] types.
macro_rules! impl_instance_with_function {
    ($( $names:ident : $types:ident ),*) => {
        impl_instance_with_function_for!(EntrypointInstance<UserData>, $( $names: $types ),*);
        impl_instance_with_function_for!(ReentrantInstance<'_, UserData>, $( $names: $types ),*);
    };
}

/// Implements [`InstanceWithFunction`] for functions with the provided amount of parameters for
/// the provided `instance` type.
macro_rules! impl_instance_with_function_for {
    ($instance:ty, $( $names:ident : $types:ident ),*) => {
        impl<$( $types, )* Results, UserData> InstanceWithFunction<HList![$( $types ),*], Results>
            for $instance
        where
            $( $types: FlatType + FromToNativeWasmType + NativeWasmTypeInto, )*
            Results: FlatLayout + WasmerResults,
            UserData: 'static,
        {
            type Function = TypedFunction<
                <HList![$( $types ),*] as WasmerParameters>::ImportParameters,
                <Results as WasmerResults>::Results,
            >;

            fn function_from_export(
                &mut self,
                export: <Self::Runtime as Runtime>::Export,
            ) -> Result<Option<Self::Function>, RuntimeError> {
                Ok(match export {
                    Extern::Function(function) => Some(function.typed(&self.as_store_ref())?),
                    _ => None,
                })
            }

            fn call(
                &mut self,
                function: &Self::Function,
                hlist_pat![$( $names ),*]: HList![$( $types ),*],
            ) -> Result<Results, RuntimeError> {
                let results = function.call(&mut *self, $( $names ),*)?;

                Ok(Results::from_wasmer(results))
            }
        }
    };
}

repeat_macro!(impl_instance_with_function =>
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
