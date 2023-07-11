// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::js_utils::{getf, unproxy};
use wasm_bindgen::prelude::*;

/// Adds an input line for lists.
#[wasm_bindgen]
pub fn append_input(component: JsValue) {
    let element = getf(&component, "elt");
    let input: js_sys::Array = getf(&element, "_input").into();
    let child = unproxy(&getf(&element, "ofType"));
    input.splice(input.length(), 0, &child);
}

/// Removes an input line.
#[wasm_bindgen]
pub fn remove_input(component: JsValue, index: u32) {
    let element = getf(&component, "elt");
    let input: js_sys::Array = getf(&element, "_input").into();
    input.splice(index, 1, &JsValue::undefined());
}
