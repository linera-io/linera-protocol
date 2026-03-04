// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI and types for a wrapped (bridged) fungible token with Mint/Burn support.

use async_graphql::{Request, Response};
pub use fungible::Message;
pub use linera_sdk::abis::fungible::{
    Account, FungibleResponse, InitialState, InitialStateBuilder,
};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, Amount, ContractAbi, ServiceAbi},
};
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

/// Operations for the wrapped fungible token application.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum WrappedFungibleOperation {
    /// Requests an account balance.
    Balance {
        /// Owner to query the balance for
        owner: AccountOwner,
    },
    /// Requests this fungible token's ticker symbol.
    TickerSymbol,
    /// Approve the transfer of tokens
    Approve {
        /// Owner to transfer from
        owner: AccountOwner,
        /// The spender account
        spender: AccountOwner,
        /// Maximum amount to be transferred
        allowance: Amount,
    },
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account.
    Transfer {
        /// Owner to transfer from
        owner: AccountOwner,
        /// Amount to be transferred
        amount: Amount,
        /// Target account to transfer the amount to
        target_account: Account,
    },
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account by using the allowance.
    TransferFrom {
        /// Owner to transfer from
        owner: AccountOwner,
        /// The spender of the amount.
        spender: AccountOwner,
        /// Amount to be transferred
        amount: Amount,
        /// Target account to transfer the amount to
        target_account: Account,
    },
    /// Same as `Transfer` but the source account may be remote. Depending on its
    /// configuration, the target chain may take time or refuse to process
    /// the message.
    Claim {
        /// Source account to claim amount from
        source_account: Account,
        /// Amount to be claimed
        amount: Amount,
        /// Target account to claim the amount into
        target_account: Account,
    },
    /// Mints new tokens to a target account. Only the authorized minter can call this.
    Mint {
        /// Account to receive the minted tokens
        target_account: Account,
        /// Amount of tokens to mint
        amount: Amount,
    },
    /// Burns tokens from an account. Only the authorized minter can call this.
    Burn {
        /// Account owner whose tokens to burn
        owner: AccountOwner,
        /// Amount of tokens to burn
        amount: Amount,
    },
}

/// ABI for the wrapped fungible token application.
pub struct WrappedFungibleTokenAbi;

impl ContractAbi for WrappedFungibleTokenAbi {
    type Operation = WrappedFungibleOperation;
    type Response = FungibleResponse;
}

impl ServiceAbi for WrappedFungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
}
