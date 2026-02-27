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
}

/// Replay-protection key for processed deposits.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct DepositKey {
    pub source_chain_id: u64,
    pub block_hash: [u8; 32],
    pub tx_index: u64,
    pub log_index: u64,
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
}

/// Responses from the bridge contract.
#[derive(Debug, Deserialize, Serialize)]
pub enum BridgeResponse {
    Ok,
}

pub struct EvmBridgeAbi;

impl ContractAbi for EvmBridgeAbi {
    type Operation = BridgeOperation;
    type Response = BridgeResponse;
}

impl ServiceAbi for EvmBridgeAbi {
    type Query = Request;
    type QueryResponse = Response;
}
