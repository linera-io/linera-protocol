// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// use serde::Serialize;
use serde_wasm_bindgen::Serializer;
use wasm_bindgen::prelude::*;

/// JS special serializer
pub const SER: Serializer = serde_wasm_bindgen::Serializer::json_compatible();

pub fn setf(target: &JsValue, field: &str, value: &JsValue) {
    js_sys::Reflect::set(target, &JsValue::from_str(field), value)
        .unwrap_or_else(|_| panic!("failed to set js field '{}'", field));
}

pub fn getf(target: &JsValue, field: &str) -> JsValue {
    js_sys::Reflect::get(target, &JsValue::from_str(field))
        .unwrap_or_else(|_| panic!("failed to get js field '{}'", field))
}

pub fn log(x: &JsValue) {
    web_sys::console::log_1(x)
}

pub fn log_str(s: &str) {
    log(&JsValue::from_str(s))
}
