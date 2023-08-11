// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module with a minimal function without parameters or return values.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("export-simple-function");

export_export_simple_function!(Implementation);

use self::exports::witty_macros::test_modules::simple_function::SimpleFunction;

struct Implementation;

impl SimpleFunction for Implementation {
    fn simple() {}
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
