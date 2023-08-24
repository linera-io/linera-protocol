// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper Wasm module for reentrancy tests with a global state, to check that it is persisted
//! across reentrant calls.

#![cfg_attr(target_arch = "wasm32", no_main)]

wit_bindgen::generate!("reentrant-global-state");

export_reentrant_global_state!(Implementation);

use self::{
    exports::witty_macros::test_modules::global_state::GlobalState,
    witty_macros::test_modules::get_host_value::*,
};

static mut GLOBAL_STATE: u32 = 0;

struct Implementation;

impl GlobalState for Implementation {
    fn entrypoint(value: u32) -> u32 {
        unsafe { GLOBAL_STATE = value };
        get_host_value()
    }

    fn get_global_state() -> u32 {
        unsafe { GLOBAL_STATE }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
