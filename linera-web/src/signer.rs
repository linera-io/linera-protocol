// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains various implementation of the [`Signer`] trait usable in the browser.
use std::fmt::Display;

use js_sys::Promise as JsPromise;
use linera_base::{
    crypto::{AccountPublicKey, AccountSignature, CryptoHash, Signer},
    identifiers::AccountOwner,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::wasm_bindgen;

#[repr(u8)]
#[wasm_bindgen(js_name = "SignerError")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmbeddedSignerError {
    MissingKey = 0,
    SigningError = 1,
    UknownError = 9,
}

impl Display for EmbeddedSignerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmbeddedSignerError::MissingKey => write!(f, "No key found for the given owner"),
            EmbeddedSignerError::SigningError => write!(f, "Error signing the value"),
            EmbeddedSignerError::UknownError => write!(f, "An unknown error occurred"),
        }
    }
}

impl From<JsValue> for EmbeddedSignerError {
    fn from(value: JsValue) -> Self {
        if let Some(fnum) = value.as_f64() {
            match fnum as u8 {
                0 => EmbeddedSignerError::MissingKey,
                1 => EmbeddedSignerError::SigningError,
                _ => EmbeddedSignerError::UknownError,
            }
        } else {
            EmbeddedSignerError::UknownError
        }
    }
}

impl std::error::Error for EmbeddedSignerError {}

#[wasm_bindgen]
pub struct EmbeddedSigner {
    sign_handler: js_sys::Function,
    get_public_key_handler: js_sys::Function,
    contains_key_handler: js_sys::Function,
}

#[wasm_bindgen(js_class = "EmbeddedSigner")]
impl EmbeddedSigner {
    /// Creates a new embedded signer with the provided handlers.
    #[wasm_bindgen(constructor)]
    pub fn new(
        sign_handler: js_sys::Function,
        get_public_key_handler: js_sys::Function,
        contains_key_handler: js_sys::Function,
    ) -> Self {
        Self {
            sign_handler,
            get_public_key_handler,
            contains_key_handler,
        }
    }
}

impl Signer for EmbeddedSigner {
    type Error = EmbeddedSignerError;

    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error> {
        let context = JsValue::null();
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let result = JsFuture::from(JsPromise::resolve(
            &self
                .contains_key_handler
                .call1(&context, &js_owner)
                .map_err(EmbeddedSignerError::from)?,
        ))
        .await
        .map_err(EmbeddedSignerError::from)?;

        Ok(serde_wasm_bindgen::from_value(result).unwrap())
    }

    async fn get_public_key(&self, owner: &AccountOwner) -> Result<AccountPublicKey, Self::Error> {
        let context = JsValue::null();
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let result = self
            .get_public_key_handler
            .call1(&context, &js_owner)
            .unwrap();
        Ok(serde_wasm_bindgen::from_value(result).unwrap())
    }

    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Self::Error> {
        let context = JsValue::null();
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let js_cryptohash = serde_wasm_bindgen::to_value(value).unwrap();
        let result = self
            .sign_handler
            .call2(&context, &js_owner, &js_cryptohash)
            .unwrap();
        Ok(serde_wasm_bindgen::from_value(result).unwrap())
    }
}
