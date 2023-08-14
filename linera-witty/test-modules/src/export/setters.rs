// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module with some functions that have one parameter and no return values.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("export-setters");

export_export_setters!(Implementation);

use self::exports::witty_macros::test_modules::setters::Setters;

struct Implementation;

impl Setters for Implementation {
    fn set_bool(_value: bool) {}

    fn set_s8(_value: i8) {}

    fn set_u8(_value: u8) {}

    fn set_s16(_value: i16) {}

    fn set_u16(_value: u16) {}

    fn set_s32(_value: i32) {}

    fn set_u32(_value: u32) {}

    fn set_s64(_value: i64) {}

    fn set_u64(_value: u64) {}

    fn set_float32(_value: f32) {}

    fn set_float64(_value: f64) {}
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
