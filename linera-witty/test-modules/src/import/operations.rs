// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module that calls some functions that have two parameters and one return value.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("import-operations");

export_import_operations!(Implementation);

use self::{
    exports::witty_macros::test_modules::entrypoint::Entrypoint,
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

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
