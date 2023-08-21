// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module that calls a minimal function without parameters or return values.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("import-simple-function");

export_import_simple_function!(Implementation);

use self::{
    exports::witty_macros::test_modules::entrypoint::Entrypoint,
    witty_macros::test_modules::simple_function::*,
};

struct Implementation;

impl Entrypoint for Implementation {
    fn entrypoint() {
        simple();
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
