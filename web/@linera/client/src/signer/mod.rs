// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains various implementation of the [`Signer`] trait usable in the browser.
use linera_base::{
    crypto::{AccountSignature, CryptoHash},
    identifiers::AccountOwner,
};
use wasm_bindgen::prelude::*;
use web_sys::wasm_bindgen;

use crate::error::Thrown;

/// Errors arising from the JavaScript [`Signer`] interface.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The signer threw. The thrown value is reported as it was received, including its
    /// `cause` chain.
    //
    // Not `#[error(transparent)]`: that forwards `source` past the `Thrown` to its cause,
    // putting the stack out of reach of `to_js_error`.
    #[error("{0}")]
    Thrown(#[from] Thrown),

    /// The signer returned a value that is not a signature of the scheme this owner uses.
    #[error("the signer returned a value that is not a valid signature for owner {0}")]
    SignatureFormat(AccountOwner),

    /// `getPublicKey` returned a value that is not an Ed25519 public key.
    #[error("the signer returned a value that is not a valid Ed25519 public key")]
    PublicKeyFormat,

    /// The signer signed with a key belonging to a different owner.
    #[error("the signer signed for owner {actual}, but {requested} was requested")]
    OwnerMismatch {
        requested: AccountOwner,
        actual: AccountOwner,
    },

    /// Signing was requested for an owner that has no signature scheme.
    #[error("cannot sign for the reserved owner {0}")]
    ReservedOwner(AccountOwner),
}

impl From<JsValue> for Error {
    fn from(value: JsValue) -> Self {
        Self::Thrown(value.into())
    }
}

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

    #[wasm_bindgen(catch, method, js_name = "getPublicKey")]
    async fn get_public_key(
        this: &Signer,
        owner: AccountOwner,
    ) -> Result<js_sys::JsString, JsValue>;
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
        // We pass `CryptoHash` bytes (not BCS-serialized) to the JS layer because the JS
        // signers sign the raw prehash directly.
        let prehash_bytes = value.as_bytes().0.to_vec();
        let sig_str: String = self.sign(*owner, prehash_bytes).await?.into();

        match owner {
            AccountOwner::Address20(address) => {
                let signature = sig_str
                    .parse()
                    .map_err(|_| Error::SignatureFormat(*owner))?;
                Ok(AccountSignature::EvmSecp256k1 {
                    signature,
                    address: *address,
                })
            }
            AccountOwner::Address32(_) => {
                let pub_str: String = self.get_public_key(*owner).await?.into();
                let public_key =
                    parse_ed25519_public_key(&pub_str).ok_or(Error::PublicKeyFormat)?;
                let signature =
                    parse_ed25519_signature(&sig_str).ok_or(Error::SignatureFormat(*owner))?;
                // Error early if signer returns a valid signature with the wrong public key.
                let actual = AccountOwner::from(public_key);
                if actual != *owner {
                    return Err(Error::OwnerMismatch {
                        requested: *owner,
                        actual,
                    });
                }
                Ok(AccountSignature::Ed25519 {
                    signature,
                    public_key,
                })
            }
            AccountOwner::Reserved(_) => Err(Error::ReservedOwner(*owner)),
        }
    }
}

fn parse_ed25519_public_key(hex_str: &str) -> Option<linera_base::crypto::Ed25519PublicKey> {
    let trimmed = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = hex::decode(trimmed).ok()?;
    linera_base::crypto::Ed25519PublicKey::from_slice(&bytes).ok()
}

fn parse_ed25519_signature(hex_str: &str) -> Option<linera_base::crypto::Ed25519Signature> {
    let trimmed = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = hex::decode(trimmed).ok()?;
    linera_base::crypto::Ed25519Signature::from_slice(&bytes).ok()
}
