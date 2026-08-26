// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Decoding of application payloads against their published serde formats.
//!
//! Operations, messages and events in a block are opaque bytes: the protocol never
//! interprets them, so a block explorer can only show them as bytes unless it knows the
//! application's types. An application publishes that knowledge as a BCS-encoded
//! [`linera_sdk::formats::Formats`] blob, and [`Formats`] turns it into a decoder.
//!
//! Fetching the blob is the caller's job — where it lives is a deployment choice, and on
//! this network it is served by a formats-registry application, which
//! [`Application::query`](crate::chain::Application::query) can already read.

use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::JsResult;

/// A decoder for one application's operation, message, response and event payloads.
#[wasm_bindgen]
pub struct Formats(linera_sdk::formats::Formats);

#[wasm_bindgen]
impl Formats {
    /// Reads the BCS-encoded `Formats` blob an application publishes, so its payloads
    /// can be decoded.
    ///
    /// # Errors
    /// If the bytes are not a BCS-encoded `Formats`.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> JsResult<Formats> {
        Ok(Formats(linera_sdk::bcs::from_bytes(bytes)?))
    }

    /// Decodes the bytes of an operation into a plain JavaScript value.
    ///
    /// # Errors
    /// If the bytes don't match the application's operation format.
    #[wasm_bindgen(js_name = decodeOperation)]
    pub fn decode_operation(&self, bytes: &[u8]) -> JsResult<JsValue> {
        Ok(serde_wasm_bindgen::to_value(
            &self.0.decode_operation(bytes)?,
        )?)
    }

    /// Decodes the bytes of an operation's result into a plain JavaScript value.
    ///
    /// # Errors
    /// If the bytes don't match the application's response format.
    #[wasm_bindgen(js_name = decodeResponse)]
    pub fn decode_response(&self, bytes: &[u8]) -> JsResult<JsValue> {
        Ok(serde_wasm_bindgen::to_value(
            &self.0.decode_response(bytes)?,
        )?)
    }

    /// Decodes the bytes of a cross-chain message into a plain JavaScript value.
    ///
    /// # Errors
    /// If the bytes don't match the application's message format.
    #[wasm_bindgen(js_name = decodeMessage)]
    pub fn decode_message(&self, bytes: &[u8]) -> JsResult<JsValue> {
        Ok(serde_wasm_bindgen::to_value(
            &self.0.decode_message(bytes)?,
        )?)
    }

    /// Decodes the bytes of an event into a plain JavaScript value.
    ///
    /// # Errors
    /// If the bytes don't match the application's event format.
    #[wasm_bindgen(js_name = decodeEventValue)]
    pub fn decode_event_value(&self, bytes: &[u8]) -> JsResult<JsValue> {
        Ok(serde_wasm_bindgen::to_value(
            &self.0.decode_event_value(bytes)?,
        )?)
    }
}
