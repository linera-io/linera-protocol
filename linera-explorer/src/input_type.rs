// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::js_utils::{getf, unproxy};
use wasm_bindgen::prelude::*;

/// add a input line for lists
#[wasm_bindgen]
pub fn append_input(component: JsValue) {
    let t = getf(&component, "t");
    let input: js_sys::Array = getf(&t, "_input").into();
    let child = unproxy(&getf(&t, "ofType"));
    input.splice(input.length(), 0, &child);
}

/// remove an input line
#[wasm_bindgen]
pub fn remove_input(component: JsValue, index: u32) {
    let t = getf(&component, "t");
    let input: js_sys::Array = getf(&t, "_input").into();
    input.splice(index, 1, &JsValue::undefined());
}
