// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains various implementation of the [`Signer`] trait usable in the browser.
use std::{fmt::Display, str::FromStr};

use linera_base::{
    crypto::{AccountSignature, CryptoHash, EvmSignature},
    identifiers::AccountOwner,
};
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

// TODO(TODO) remove this in favour of passing up arbitrary `JsValue` errors
#[repr(u8)]
#[wasm_bindgen(js_name = "SignerError")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Error {
    MissingKey = 0,
    SigningError = 1,
    PublicKeyParse = 2,
    JsConversion = 3,
    UnexpectedSignatureFormat = 4,
    InvalidAccountOwnerType = 5,
    Unknown = 9,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MissingKey => write!(f, "No key found for the given owner"),
            Error::SigningError => write!(f, "Error signing the value"),
            Error::PublicKeyParse => write!(f, "Error parsing the public key"),
            Error::JsConversion => write!(f, "Error converting JS value"),
            Error::UnexpectedSignatureFormat => {
                write!(f, "Unexpected signature format received from JS")
            }
            Error::InvalidAccountOwnerType => {
                write!(
                    f,
                    "Invalid account owner type provided. Expected AccountOwner::Address20"
                )
            }
            Error::Unknown => write!(f, "An unknown error occurred"),
        }
    }
}

impl From<JsValue> for Error {
    fn from(value: JsValue) -> Self {
        match value.as_f64().and_then(num_traits::cast) {
            Some(0u8) => Error::MissingKey,
            Some(1) => Error::SigningError,
            Some(2) => Error::PublicKeyParse,
            Some(3) => Error::JsConversion,
            Some(4) => Error::UnexpectedSignatureFormat,
            Some(5) => Error::InvalidAccountOwnerType,
            _ => Error::Unknown,
        }
    }
}

impl std::error::Error for Error {}

#[wasm_bindgen]
extern "C" {
    // We refer to the interface defined above.
    #[wasm_bindgen(typescript_type = "Signer")]
    pub type Signer;

    #[wasm_bindgen(catch, method)]
    async fn sign(this: &Signer, owner: JsValue, value: Vec<u8>) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(catch, method, js_name = "containsKey")]
    async fn contains_key(this: &Signer, owner: JsValue) -> Result<JsValue, JsValue>;
}

impl linera_base::crypto::Signer for Signer {
    type Error = Error;

    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error> {
        let js_owner = serde_wasm_bindgen::to_value(owner).unwrap();
        let js_bool = self.contains_key(js_owner).await.map_err(Error::from)?;

        serde_wasm_bindgen::from_value(js_bool).map_err(|_| Error::JsConversion)
    }

    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Self::Error> {
        let address = match owner {
            AccountOwner::Address20(address) => *address,
            _ => return Err(Error::InvalidAccountOwnerType),
        };
        let js_owner = JsValue::from_str(&owner.to_string());
        // Pass CryptoHash without serializing as that adds bytes
        // to the serialized value which we don't want for signing.
        let js_cryptohash = value.as_bytes().0.to_vec();
        let js_signature = self
            .sign(js_owner, js_cryptohash)
            .await
            .map_err(Error::from)?
            .as_string()
            .ok_or(Error::JsConversion)?;
        let signature =
            EvmSignature::from_str(&js_signature).map_err(|_| Error::UnexpectedSignatureFormat)?;
        Ok(AccountSignature::EvmSecp256k1 { signature, address })
    }
}
