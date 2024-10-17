// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module for reentrancy tests with some functions that have multiple parameters and
//! return values.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("reentrant-operations");

export_reentrant_operations!(Implementation);

use self::{
    exports::witty_macros::test_modules::{entrypoint::Entrypoint, operations::Operations},
    witty_macros::test_modules::operations::*,
};

struct Implementation;

impl Entrypoint for Implementation {
    #[expect(clippy::bool_assert_comparison)]
    fn entrypoint() {
        assert_eq!(and_bool(true, true), true);
        assert_eq!(and_bool(true, false), false);
        assert_eq!(add_s8(-100, 40), -60);
        assert_eq!(add_u8(201, 32), 233);
        assert_eq!(add_s16(-20_000, 30_000), 10_000);
        assert_eq!(add_u16(50_000, 256), 50_256);
        assert_eq!(add_s32(-2_000_000, -1), -2_000_001);
        assert_eq!(add_u32(4_000_000, 1), 4_000_001);
        assert_eq!(add_s64(-16_000_000, 32_000_000), 16_000_000);
        assert_eq!(add_u64(3_000_000_000, 9_345_678_999), 12_345_678_999);
        assert_eq!(add_float32(10.5, 120.25), 130.75);
        assert_eq!(add_float64(-0.000_08, 1.0), 0.999_92);
    }
}

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
