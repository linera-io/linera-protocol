// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Validator key extraction helpers for the LightClient contract.

use alloy::primitives::{keccak256, Address};
use linera_base::crypto::ValidatorPublicKey;
use linera_execution::committee::Committee;

/// Extracts uncompressed validator public keys from a BCS-serialized committee blob.
///
/// Returns 64-byte uncompressed keys (without the 0x04 prefix) for each validator,
/// sorted by their compressed byte representation to match BCS canonical map ordering.
///
/// BCS serializes map entries sorted by serialized key bytes, which may differ from
/// Rust's `BTreeMap` iteration order (based on `Ord` for `VerifyingKey`).
pub fn extract_validator_keys(committee_blob: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
    let committee: Committee = bcs::from_bytes(committee_blob)?;
    let mut keys: Vec<ValidatorPublicKey> = committee.validators().keys().copied().collect();
    keys.sort_by_key(|a| a.as_bytes());
    Ok(keys.iter().map(validator_uncompressed_key).collect())
}

/// Derives the Ethereum address from a secp256k1 validator public key.
pub fn validator_evm_address(public: &ValidatorPublicKey) -> Address {
    let uncompressed = public.0.to_encoded_point(false);
    let hash = keccak256(&uncompressed.as_bytes()[1..]); // skip 0x04 prefix
    Address::from_slice(&hash[12..])
}

/// Returns the 64-byte uncompressed public key (without the 0x04 prefix).
pub fn validator_uncompressed_key(public: &ValidatorPublicKey) -> Vec<u8> {
    let uncompressed = public.0.to_encoded_point(false);
    uncompressed.as_bytes()[1..].to_vec()
}
