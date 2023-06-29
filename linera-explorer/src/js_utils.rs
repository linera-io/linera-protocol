// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// use serde::Serialize;
use serde_wasm_bindgen::Serializer;
use wasm_bindgen::prelude::*;

/// JS special serializer
pub const SER: Serializer =
    serde_wasm_bindgen::Serializer::json_compatible().serialize_bytes_as_arrays(true);

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

// pub fn log_json(v: &serde_json::Value) {
//     log(&v.serialize(&SER).expect("failed to serialize json"))
// }

pub fn parse(x: &str) -> JsValue {
    js_sys::JSON::parse(x).expect("parse json failed")
}

pub fn stringify(x: &JsValue) -> String {
    js_sys::JSON::stringify(x)
        .expect("stringify json failed")
        .into()
}

pub fn js_to_json(x: &JsValue) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(&stringify(x)).expect("js_to_json failed")
}

pub fn unproxy(x: &JsValue) -> JsValue {
    parse(&stringify(x))
}
