// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasmer support for host functions exported to guests Wasm instances.

#![allow(clippy::let_unit_value)]

use super::{InstanceBuilder, InstanceSlot};
use crate::{primitive_types::MaybeFlatType, ExportFunction, RuntimeError};
use std::error::Error;
use wasmer::{FromToNativeWasmType, Function, FunctionEnvMut, WasmTypeList};

/// Implements [`ExportFunction`] for [`InstanceBuilder`] using the supported function signatures.
macro_rules! export_function {
    ($( $names:ident: $types:ident ),*) => {
        impl<Handler, HandlerError, $( $types, )* FlatResult>
            ExportFunction<Handler, ($( $types, )*), FlatResult> for InstanceBuilder<()>
        where
            $( $types: FromToNativeWasmType, )*
            FlatResult: MaybeFlatType + WasmTypeList,
            HandlerError: Error + Send + Sync + 'static,
            Handler:
                Fn(
                    FunctionEnvMut<'_, InstanceSlot<()>>,
                    ($( $types, )*),
                ) -> Result<FlatResult, HandlerError>
                + Send
                + Sync
                + 'static,
        {
            fn export(
                &mut self,
                module_name: &str,
                function_name: &str,
                handler: Handler,
            ) -> Result<(), RuntimeError> {
                let environment = self.environment();

                let function = Function::new_typed_with_env(
                    self,
                    &environment,
                    move |
                        environment: FunctionEnvMut<'_, InstanceSlot<()>>,
                        $( $names: $types ),*
                    | -> Result<FlatResult, wasmer::RuntimeError> {
                        handler(environment, ($( $names, )*))
                            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
                                Box::new(error)
                            })
                            .map_err(wasmer::RuntimeError::user)
                    },
                );

                self.define(
                    module_name,
                    function_name,
                    function,
                );

                Ok(())
            }
        }
    };
}

repeat_macro!(export_function =>
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
);
