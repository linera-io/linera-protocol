// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains various implementation of the [`Signer`] trait usable in the browser.
use std::{fmt::Display, str::FromStr};

use linera_base::{
    crypto::{AccountPublicKey, AccountSignature, CryptoHash, EvmPublicKey, EvmSignature, Signer},
    identifiers::AccountOwner,
};
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

#[repr(u8)]
#[wasm_bindgen(js_name = "SignerError")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JsSignerError {
    MissingKey = 0,
    SigningError = 1,
    PublicKeyParse = 2,
    JsConversion = 3,
    UnexpectedSignatureFormat = 4,
    Unknown = 9,
}

impl Display for JsSignerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsSignerError::MissingKey => write!(f, "No key found for the given owner"),
            JsSignerError::SigningError => write!(f, "Error signing the value"),
            JsSignerError::PublicKeyParse => write!(f, "Error parsing the public key"),
            JsSignerError::JsConversion => write!(f, "Error converting JS value"),
            JsSignerError::UnexpectedSignatureFormat => {
                write!(f, "Unexpected signature format received from JS")
            }
            JsSignerError::Unknown => write!(f, "An unknown error occurred"),
        }
    }
}

impl From<JsValue> for JsSignerError {
    fn from(value: JsValue) -> Self {
        if let Some(fnum) = value.as_f64() {
            match fnum as u8 {
                0 => JsSignerError::MissingKey,
                1 => JsSignerError::SigningError,
                2 => JsSignerError::PublicKeyParse,
                3 => JsSignerError::JsConversion,
                4 => JsSignerError::UnexpectedSignatureFormat,
                _ => JsSignerError::Unknown,
            }
        } else {
            JsSignerError::Unknown
        }
    }
}

impl std::error::Error for JsSignerError {}

// An interface that will be compiled to TypeScript and exported for use in the browser.
#[wasm_bindgen(typescript_custom_section)]
const SIGNER_INTERFACE: &'static str = r#"
/**
 * Interface for signing and key management compatible with Ethereum (EVM) addresses.
 */
export interface Signer {
  /**
   * Signs a given value using the private key associated with the specified EVM address.
   * The signing process must follow the EIP-191 standard.
   *
   * @param owner - The EVM address whose private key will be used to sign the value.
   * @param value - The data to be signed, as a `Uint8Array`.
   * @returns A promise that resolves to the EIP-191-compatible signature in hexadecimal string format.
   */
  sign(owner: string, value: Uint8Array): Promise<string>;

  /**
   * Retrieves the public key corresponding to the private key associated with the specified EVM address.
   *
   * @param owner - The EVM address for which to retrieve the public key.
   * @returns A promise that resolves to the public key as a hexadecimal string.
   */
  getPublicKey(owner: string): Promise<string>;

  /**
   * Checks whether the instance holds a key whose associated address matches the given EVM address.
   *
   * @param owner - The EVM address to check for.
   * @returns A promise that resolves to `true` if the key exists and matches the given address, otherwise `false`.
   */
  containsKey(owner: string): Promise<boolean>;
}"#;

#[wasm_bindgen]
extern "C" {
    // We refer to the interface defined above.
    #[wasm_bindgen(typescript_type = "Signer")]
    pub type JsSigner;

    #[wasm_bindgen(catch, method)]
    async fn sign(this: &JsSigner, owner: JsValue, value: Vec<u8>) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(catch, method)]
    async fn get_public_key(this: &JsSigner, owner: JsValue) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(catch, method)]
    async fn contains_key(this: &JsSigner, owner: JsValue) -> Result<JsValue, JsValue>;
}

impl Signer for JsSigner {
    type Error = JsSignerError;

    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error> {
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let js_bool = self
            .contains_key(js_owner)
            .await
            .map_err(JsSignerError::from)?;

        Ok(serde_wasm_bindgen::from_value(js_bool).map_err(|_| JsSignerError::JsConversion)?)
    }

    async fn get_public_key(&self, owner: &AccountOwner) -> Result<AccountPublicKey, Self::Error> {
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let js_public_key = self
            .get_public_key(js_owner)
            .await
            .map_err(JsSignerError::from)?
            .as_string()
            .ok_or(JsSignerError::JsConversion)?;
        let pk =
            EvmPublicKey::from_str(&js_public_key).map_err(|_| JsSignerError::PublicKeyParse)?;
        Ok(AccountPublicKey::EvmSecp256k1(pk))
    }

    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Self::Error> {
        let js_owner = JsValue::from_str(&owner.to_string());
        // Pass CryptoHash without serializing as that adds bytes
        // to the serialized value which we don't want for signing.
        let js_cryptohash = value.as_bytes().0.to_vec();
        let js_signature = self
            .sign(js_owner, js_cryptohash)
            .await
            .map_err(JsSignerError::from)?
            .as_string()
            .ok_or(JsSignerError::JsConversion)?;
        let signature = EvmSignature::from_str(&js_signature)
            .map_err(|_| JsSignerError::UnexpectedSignatureFormat)?;
        Ok(AccountSignature::EvmSecp256k1(signature))
    }
}
