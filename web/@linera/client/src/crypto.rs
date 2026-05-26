// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cryptographic helpers exposed to JavaScript.

use linera_base::{crypto::Ed25519PublicKey, identifiers::AccountOwner};
use wasm_bindgen::prelude::*;

/// Derives the `Address32` `AccountOwner` from a raw 32-byte Ed25519 public key.
///
/// Returns the canonical `0x`-prefixed lowercase hex string for the owner address,
/// matching `AccountOwner::Display`.
#[wasm_bindgen(js_name = "accountOwnerFromEd25519PublicKey")]
pub fn account_owner_from_ed25519_public_key(public_key: &[u8]) -> Result<String, JsError> {
    let pubkey =
        Ed25519PublicKey::from_slice(public_key).map_err(|e| JsError::new(&e.to_string()))?;
    Ok(AccountOwner::from(pubkey).to_string())
}
