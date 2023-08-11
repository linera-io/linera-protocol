// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module with some functions that have two parameters and one return value.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("export-operations");

export_export_operations!(Implementation);

use self::exports::witty_macros::test_modules::operations::Operations;

struct Implementation;

impl Operations for Implementation {
    fn and_bool(first: bool, second: bool) -> bool {
        first && second
    }

    fn add_s8(first: i8, second: i8) -> i8 {
        first + second
    }

    fn add_u8(first: u8, second: u8) -> u8 {
        first + second
    }

    fn add_s16(first: i16, second: i16) -> i16 {
        first + second
    }

    fn add_u16(first: u16, second: u16) -> u16 {
        first + second
    }

    fn add_s32(first: i32, second: i32) -> i32 {
        first + second
    }

    fn add_u32(first: u32, second: u32) -> u32 {
        first + second
    }

    fn add_s64(first: i64, second: i64) -> i64 {
        first + second
    }

    fn add_u64(first: u64, second: u64) -> u64 {
        first + second
    }

    fn add_float32(first: f32, second: f32) -> f32 {
        first + second
    }

    fn add_float64(first: f64, second: f64) -> f64 {
        first + second
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
