// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// use serde::Serialize;
use serde_wasm_bindgen::Serializer;
use wasm_bindgen::prelude::*;

/// JS special serializer
pub(crate) const SER: Serializer =
    serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true);

pub fn setf(target: &JsValue, field: &str, value: &JsValue) {
    js_sys::Reflect::set(target, &JsValue::from_str(field), value)
        .unwrap_or_else(|_| panic!("failed to set JS field '{}'", field));
}

pub fn getf(target: &JsValue, field: &str) -> JsValue {
    js_sys::Reflect::get(target, &JsValue::from_str(field))
        .unwrap_or_else(|_| panic!("failed to get JS field '{}'", field))
}

pub fn log(x: &JsValue) {
    web_sys::console::log_1(x)
}

pub fn log_str(s: &str) {
    log(&JsValue::from_str(s))
}

pub fn parse(x: &str) -> JsValue {
    js_sys::JSON::parse(x).expect("failed to parse JSON")
}

pub fn stringify(x: &JsValue) -> String {
    js_sys::JSON::stringify(x)
        .expect("failed to stringify JSON")
        .into()
}

pub fn js_to_json(x: &JsValue) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(&stringify(x)).expect("failed to convert JS to JSON")
}

pub fn unproxy(x: &JsValue) -> JsValue {
    parse(&stringify(x))
}
