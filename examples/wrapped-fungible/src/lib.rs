// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI and types for a wrapped (bridged) fungible token with Mint/Burn support.

use async_graphql::{Request, Response};
pub use linera_sdk::abis::fungible::{
    Account, FungibleOperation, FungibleResponse, InitialState, InitialStateBuilder,
};
use linera_sdk::linera_base_types::{AccountOwner, ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

/// Parameters for a wrapped fungible token backed by an EVM bridge.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WrappedParameters {
    /// Ticker symbol (e.g. "USDC")
    pub ticker_symbol: String,
    /// The account owner authorized to mint and burn tokens
    pub minter: AccountOwner,
    /// The ERC-20 token address on the source EVM chain
    pub evm_token_address: [u8; 20],
    /// The EVM chain ID of the source chain (e.g. 8453 for Base)
    pub evm_source_chain_id: u64,
}

/// ABI for the wrapped fungible token application.
pub struct WrappedFungibleTokenAbi;

impl ContractAbi for WrappedFungibleTokenAbi {
    type Operation = FungibleOperation;
    type Response = FungibleResponse;
}

impl ServiceAbi for WrappedFungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
}
