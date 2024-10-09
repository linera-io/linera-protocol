// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasmtime support for host functions exported to guests Wasm instances.

#![allow(clippy::let_unit_value)]

use wasmtime::{Caller, Linker, WasmRet, WasmTy};

use crate::{primitive_types::MaybeFlatType, ExportFunction, RuntimeError};

/// Implements [`ExportFunction`] for Wasmtime's [`Linker`] using the supported function
/// signatures.
macro_rules! export_function {
    ($( $names:ident: $types:ident ),*) => {
        impl<Handler, $( $types, )* FlatResult, Data>
            ExportFunction<Handler, ($( $types, )*), FlatResult> for Linker<Data>
        where
            $( $types: WasmTy, )*
            FlatResult: MaybeFlatType + WasmRet,
            Handler:
                Fn(Caller<'_, Data>, ($( $types, )*)) -> Result<FlatResult, RuntimeError>
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
                self.func_wrap(
                    module_name,
                    function_name,
                    move |
                        caller: Caller<'_, Data>,
                        $( $names: $types ),*
                    | -> anyhow::Result<FlatResult> {
                        let response = handler(caller, ($( $names, )*))?;
                        Ok(response)
                    },
                )
                .map_err(RuntimeError::Wasmtime)?;
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
    q: Q,
);
