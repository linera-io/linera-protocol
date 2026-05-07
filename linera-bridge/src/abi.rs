// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical ABI types for the EVM→Linera bridge application.
//!
//! These types define the BCS-stable wire format used between:
//! - the on-chain `evm-bridge` Wasm contract (which deserializes operations),
//! - the off-chain relay (which serializes operations to submit them),
//! - and end-to-end tests (which exercise the full pipeline).
//!
//! Hosting them here ensures all three sites stay in lockstep without manual copying.

use linera_base::identifiers::ApplicationId;
use serde::{Deserialize, Serialize};

/// Parameters for a deployed bridge instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeParameters {
    /// EVM chain ID of the source chain (e.g. 8453 for Base).
    pub source_chain_id: u64,
    /// ERC-20 token address on the source EVM chain.
    pub token_address: [u8; 20],
    /// JSON-RPC endpoint of the source EVM chain for finality verification.
    /// When non-empty, `ProcessDeposit` requires the block hash to be verified first
    /// via `VerifyBlockHash`.
    pub rpc_endpoint: String,
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
    ///
    /// Queries the EVM node to confirm the block exists and its number is at or below
    /// the latest finalized block. Caches the hash only when submitted by an
    /// authenticated signer (chain owner) to prevent state bloat.
    VerifyBlockHash { block_hash: [u8; 32] },
    /// Register the EVM FungibleBridge contract address.
    /// Can only be called once, by the chain owner.
    /// Must be set before ProcessDeposit can succeed.
    RegisterFungibleBridge { address: [u8; 20] },
}
