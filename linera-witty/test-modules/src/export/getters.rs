// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module with some functions that have no parameters but return values.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("export-getters");

export_export_getters!(Implementation);

use self::exports::witty_macros::test_modules::getters::Getters;

struct Implementation;

impl Getters for Implementation {
    fn get_true() -> bool {
        true
    }

    fn get_false() -> bool {
        false
    }

    fn get_s8() -> i8 {
        -125
    }

    fn get_u8() -> u8 {
        200
    }

    fn get_s16() -> i16 {
        -410
    }

    fn get_u16() -> u16 {
        60_000
    }

    fn get_s32() -> i32 {
        -100_000
    }

    fn get_u32() -> u32 {
        3_000_111
    }

    fn get_s64() -> i64 {
        -5_000_000
    }

    fn get_u64() -> u64 {
        10_000_000_000
    }

    fn get_float32() -> f32 {
        -0.125
    }

    fn get_float64() -> f64 {
        128.25
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
