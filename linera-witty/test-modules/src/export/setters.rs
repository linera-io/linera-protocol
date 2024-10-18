// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module with some functions that have one parameter and no return values.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("export-setters");

export_export_setters!(Implementation);

use self::exports::witty_macros::test_modules::setters::Setters;

struct Implementation;

impl Setters for Implementation {
    #[expect(clippy::bool_assert_comparison)]
    fn set_bool(value: bool) {
        assert_eq!(value, false);
    }

    fn set_s8(value: i8) {
        assert_eq!(value, -100);
    }

    fn set_u8(value: u8) {
        assert_eq!(value, 201);
    }

    fn set_s16(value: i16) {
        assert_eq!(value, -20_000);
    }

    fn set_u16(value: u16) {
        assert_eq!(value, 50_000);
    }

    fn set_s32(value: i32) {
        assert_eq!(value, -2_000_000);
    }

    fn set_u32(value: u32) {
        assert_eq!(value, 4_000_000);
    }

    fn set_s64(value: i64) {
        assert_eq!(value, -25_000_000_000);
    }

    fn set_u64(value: u64) {
        assert_eq!(value, 7_000_000_000);
    }

    fn set_float32(value: f32) {
        assert_eq!(value, 10.4);
    }

    fn set_float64(value: f64) {
        assert_eq!(value, -0.000_08);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
