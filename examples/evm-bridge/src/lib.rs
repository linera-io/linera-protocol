// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI and types for the EVM→Linera bridge application.
//!
//! This bridge verifies MPT inclusion proofs for `DepositInitiated` events
//! on EVM and mints wrapped tokens on Linera via the wrapped-fungible app.

use async_graphql::{Request, Response};
use linera_sdk::linera_base_types::{ApplicationId, ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

/// Parameters for a deployed bridge instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeParameters {
    /// EVM chain ID of the source chain (e.g. 8453 for Base).
    pub source_chain_id: u64,
    /// Address of the FungibleBridge contract on EVM.
    pub bridge_contract_address: [u8; 20],
    /// Application ID of the wrapped-fungible token app on Linera.
    pub fungible_app_id: ApplicationId,
    /// ERC-20 token address on the source EVM chain.
    pub token_address: [u8; 20],
    /// JSON-RPC endpoint of the source EVM chain for finality verification.
    /// When non-empty, `ProcessDeposit` requires the block hash to be verified first
    /// via `VerifyBlockHash`.
    pub rpc_endpoint: String,
}

/// Replay-protection key for processed deposits.
///
/// On-chain, only the [`DepositKey::hash`] is stored (32 bytes) rather than the
/// full struct, so the `processed_deposits` SetView uses `[u8; 32]`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct DepositKey {
    pub source_chain_id: u64,
    pub block_hash: [u8; 32],
    pub tx_index: u64,
    pub log_index: u64,
}

impl DepositKey {
    /// Deterministic keccak-256 hash of the deposit key fields.
    pub fn hash(&self) -> [u8; 32] {
        let mut data = [0u8; 56];
        data[0..8].copy_from_slice(&self.source_chain_id.to_le_bytes());
        data[8..40].copy_from_slice(&self.block_hash);
        data[40..48].copy_from_slice(&self.tx_index.to_le_bytes());
        data[48..56].copy_from_slice(&self.log_index.to_le_bytes());
        alloy_primitives::keccak256(&data).0
    }
}

/// Operations accepted by the bridge contract.
#[derive(Debug, Deserialize, Serialize)]
pub enum BridgeOperation {
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
}

pub struct EvmBridgeAbi;

impl ContractAbi for EvmBridgeAbi {
    type Operation = BridgeOperation;
    type Response = ();
}

impl ServiceAbi for EvmBridgeAbi {
    type Query = Request;
    type QueryResponse = Response;
}
