// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains various implementation of the [`Signer`] trait usable in the browser.
use std::fmt::Display;

use linera_base::{
    crypto::{AccountSignature, CryptoHash},
    identifiers::AccountOwner,
};
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

// TODO(#5150) remove this in favour of passing up arbitrary `JsValue` errors
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

#[wasm_bindgen(typescript_custom_section)]
const _: &str = r#"import type { Signer } from '../signer/index.js';"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "Signer")]
    pub type Signer;

    #[wasm_bindgen(catch, method)]
    async fn sign(
        this: &Signer,
        owner: AccountOwner,
        value: Vec<u8>,
    ) -> Result<js_sys::JsString, JsValue>;

    #[wasm_bindgen(catch, method, js_name = "containsKey")]
    async fn contains_key(this: &Signer, owner: AccountOwner) -> Result<JsValue, JsValue>;
}

impl linera_base::crypto::Signer for Signer {
    type Error = Error;

    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error> {
        Ok(self.contains_key(*owner).await?.is_truthy())
    }

    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Self::Error> {
        Ok(AccountSignature::EvmSecp256k1 {
            signature: String::from(
                self
                    // Pass CryptoHash without serializing as that adds bytes
                    // to the serialized value which we don't want for signing.
                    .sign(*owner, value.as_bytes().0.to_vec())
                    .await?,
            )
            .parse()
            .map_err(|_| Error::UnexpectedSignatureFormat)?,
            address: if let AccountOwner::Address20(address) = owner {
                *address
            } else {
                return Err(Error::InvalidAccountOwnerType);
            },
        })
    }
}
