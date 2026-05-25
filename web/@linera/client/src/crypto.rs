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
    if public_key.len() != 32 {
        return Err(JsError::new(&format!(
            "expected 32 bytes for an Ed25519 public key, got {}",
            public_key.len()
        )));
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(public_key);
    let pubkey = Ed25519PublicKey(bytes);
    Ok(AccountOwner::from(pubkey).to_string())
}
