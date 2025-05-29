// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains various implementation of the [`Signer`] trait usable in the browser.
use std::{fmt::Display, str::FromStr};

use linera_base::{
    crypto::{AccountPublicKey, AccountSignature, CryptoHash, EvmPublicKey, EvmSignature, Signer},
    identifiers::AccountOwner,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

#[repr(u8)]
#[wasm_bindgen(js_name = "SignerError")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JsSignerError {
    MissingKey = 0,
    SigningError = 1,
    PublicKeyParseError = 2,
    UknownError = 9,
}

impl Display for JsSignerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsSignerError::MissingKey => write!(f, "No key found for the given owner"),
            JsSignerError::SigningError => write!(f, "Error signing the value"),
            JsSignerError::PublicKeyParseError => write!(f, "Error parsing the public key"),
            JsSignerError::UknownError => write!(f, "An unknown error occurred"),
        }
    }
}

impl From<JsValue> for JsSignerError {
    fn from(value: JsValue) -> Self {
        if let Some(fnum) = value.as_f64() {
            match fnum as u8 {
                0 => JsSignerError::MissingKey,
                1 => JsSignerError::SigningError,
                2 => JsSignerError::PublicKeyParseError,
                _ => JsSignerError::UknownError,
            }
        } else {
            JsSignerError::UknownError
        }
    }
}

impl std::error::Error for JsSignerError {}

#[wasm_bindgen(js_name = "CryptoHash")]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsCryptoHash(CryptoHash);

#[wasm_bindgen(js_class = "CryptoHash")]
impl JsCryptoHash {
    #[wasm_bindgen(constructor)]
    pub fn new(value: &str) -> Result<JsCryptoHash, JsError> {
        Ok(JsCryptoHash(CryptoHash::from_str(value)?))
    }
}

#[wasm_bindgen(js_name = "AccountOwner")]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsAccountOwner(AccountOwner);

#[wasm_bindgen(js_class = "AccountOwner")]
impl JsAccountOwner {
    #[wasm_bindgen(constructor)]
    pub fn new(value: &str) -> Result<JsAccountOwner, JsError> {
        Ok(JsAccountOwner(AccountOwner::from_str(value).unwrap()))
    }
}

#[wasm_bindgen(js_name = "Signature")]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsSignature(EvmSignature);

#[wasm_bindgen(js_class = "Signature")]
impl JsSignature {
    #[wasm_bindgen(constructor)]
    pub fn new(value: &str) -> Result<JsSignature, JsError> {
        serde_wasm_bindgen::from_value(JsValue::from_str(value))
            .map(JsSignature)
            .map_err(|_| JsError::new("Failed to parse EvmSignature"))
    }
}

#[wasm_bindgen(typescript_custom_section)]
const JS_SIGNER_INTERFACE: &'static str = r#"
export interface IJsSigner {
  sign(owner: AccountOwner, value: CryptoHash): Promise<Signature>;
  get_public_key(owner: AccountOwner): Promise<string>;
  contains_key(owner: AccountOwner): Promise<boolean>;
}"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "IJsSigner")]
    pub type JsSigner;

    #[wasm_bindgen(catch, method)]
    async fn sign(this: &JsSigner, owner: JsValue, value: JsValue) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(catch, method)]
    async fn get_public_key(this: &JsSigner, owner: JsValue) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(catch, method)]
    async fn contains_key(this: &JsSigner, owner: JsValue) -> Result<JsValue, JsValue>;
}

impl Signer for JsSigner {
    type Error = JsSignerError;

    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error> {
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let result = self
            .contains_key(js_owner)
            .await
            .map_err(JsSignerError::from)?;

        Ok(serde_wasm_bindgen::from_value(result).unwrap())
    }

    async fn get_public_key(&self, owner: &AccountOwner) -> Result<AccountPublicKey, Self::Error> {
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let result = self
            .get_public_key(js_owner)
            .await
            .map_err(JsSignerError::from)?;
        let evm_public_key: EvmPublicKey = serde_wasm_bindgen::from_value(result)
            .map_err(|_| JsSignerError::PublicKeyParseError)?;
        Ok(AccountPublicKey::EvmSecp256k1(evm_public_key))
    }

    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Self::Error> {
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let js_cryptohash = serde_wasm_bindgen::to_value(value).unwrap();
        let result = self
            .sign(js_owner, js_cryptohash)
            .await
            .map_err(JsSignerError::from)?;
        let js_signature = serde_wasm_bindgen::from_value::<JsSignature>(result)
            .map_err(|_| JsSignerError::SigningError)?;
        Ok(AccountSignature::EvmSecp256k1(js_signature.0))
    }
}
