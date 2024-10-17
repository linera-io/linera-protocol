// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module for reentrancy tests with some functions that return values.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("reentrant-getters");

export_reentrant_getters!(Implementation);

use self::{
    exports::witty_macros::test_modules::{entrypoint::Entrypoint, getters::Getters},
    witty_macros::test_modules::getters::*,
};

struct Implementation;

impl Entrypoint for Implementation {
    #[expect(clippy::bool_assert_comparison)]
    fn entrypoint() {
        assert_eq!(get_true(), true);
        assert_eq!(get_false(), false);
        assert_eq!(get_s8(), -125);
        assert_eq!(get_u8(), 200);
        assert_eq!(get_s16(), -410);
        assert_eq!(get_u16(), 60_000);
        assert_eq!(get_s32(), -100_000);
        assert_eq!(get_u32(), 3_000_111);
        assert_eq!(get_s64(), -5_000_000);
        assert_eq!(get_u64(), 10_000_000_000);
        assert_eq!(get_float32(), -0.125);
        assert_eq!(get_float64(), 128.25);
    }
}

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
