// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helpers for deriving EVM-facing validator keys from a Linera committee blob.

use alloy::primitives::{keccak256, Address};
use linera_base::crypto::ValidatorPublicKey;

/// Derives the Ethereum address from a secp256k1 validator public key.
pub fn validator_evm_address(public: &ValidatorPublicKey) -> Address {
    let hash = keccak256(&public.as_bytes()[1..]); // skip 0x04 prefix
    Address::from_slice(&hash[12..])
}
