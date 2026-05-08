// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical ABI types for the EVM→Linera bridge application.
//!
//! These types define the BCS-stable wire format used between:
//! - the on-chain `evm-bridge` Wasm contract,
//! - the off-chain relay,
//! - and end-to-end tests.

use linera_base::identifiers::ApplicationId;
use serde::{Deserialize, Serialize};

/// Parameters for a deployed bridge instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeParameters {
    /// EVM chain ID of the source chain (e.g. 8453 for Base).
    pub source_chain_id: u64,
    /// ERC-20 token address on the source EVM chain.
    pub token_address: [u8; 20],
}

/// Operations accepted by the bridge contract.
#[derive(Debug, Deserialize, Serialize)]
pub enum BridgeOperation {
    /// Register the wrapped-fungible token application.
    /// Can only be called once, by the chain owner.
    RegisterFungibleApp { app_id: ApplicationId },
    /// Verify a deposit proof and mint wrapped tokens.
    ProcessDeposit {
        block_header_rlp: Vec<u8>,
        receipt_rlp: Vec<u8>,
        proof_nodes: Vec<Vec<u8>>,
        tx_index: u64,
        log_index: u64,
    },
    /// Verify that an EVM block hash is authentic and finalized.
    VerifyBlockHash { block_hash: [u8; 32] },
    /// Register the EVM FungibleBridge contract address.
    /// Can only be called once, by the chain owner.
    /// Must be set before ProcessDeposit can succeed.
    RegisterFungibleBridge { address: [u8; 20] },
    /// Replace the bridge's RPC endpoint. Chain-owner-only.
    /// Empty string disables finality verification.
    SetRpcEndpoint { rpc_endpoint: String },
}

/// Initial mutable state passed at `create_application`.
///
/// Distinct from `BridgeParameters` (immutable, part of the ApplicationId
/// hash). Fields here are written to the contract's state during
/// `instantiate` and may be mutated later by operations such as
/// `SetRpcEndpoint`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeInstantiationArgument {
    /// JSON-RPC endpoint of the source EVM chain for finality verification.
    /// Empty string means "skip finality verification" (test / local-dev mode).
    pub rpc_endpoint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instantiation_argument_json_roundtrip() {
        let arg = BridgeInstantiationArgument {
            rpc_endpoint: "https://example.com/rpc".to_string(),
        };
        let json = serde_json::to_string(&arg).unwrap();
        let back: BridgeInstantiationArgument = serde_json::from_str(&json).unwrap();
        assert_eq!(arg, back);

        // Empty string is a valid value (skip-finality mode).
        let empty = BridgeInstantiationArgument::default();
        let json = serde_json::to_string(&empty).unwrap();
        assert_eq!(json, r#"{"rpc_endpoint":""}"#);
    }
}
