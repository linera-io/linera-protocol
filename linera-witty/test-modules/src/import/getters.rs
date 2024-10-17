// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module that calls some functions that have no parameters but return values.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("import-getters");

export_import_getters!(Implementation);

use self::{
    exports::witty_macros::test_modules::entrypoint::Entrypoint,
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
        assert_eq!(get_float32(), -0.125);
        assert_eq!(get_float64(), 128.25);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
