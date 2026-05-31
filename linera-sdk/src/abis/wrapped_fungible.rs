// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An ABI for applications that implement a wrapped (bridged) fungible token with Mint/Burn.

use std::collections::BTreeMap;

use async_graphql::{Request, Response};
pub use linera_base::identifiers::Account;
use linera_base::{
    abi::{ContractAbi, ServiceAbi},
    data_types::U128,
    identifiers::{AccountOwner, ApplicationId, ChainId},
};
use linera_sdk_derive::{GraphQLMutationRootInCrate, StableEnumInCrate};
use serde::{Deserialize, Serialize};

/// Parameters for a wrapped fungible token backed by an EVM bridge.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WrappedParameters {
    /// Ticker symbol (e.g. "USDC")
    pub ticker_symbol: String,
    /// Number of decimal places used by the source ERC-20 (e.g. 6 for USDC).
    pub decimals: u8,
    /// If set, only this account owner can sign mint operations
    pub minter: Option<AccountOwner>,
    /// If set, minting and auto-burning are restricted to this chain
    pub mint_chain_id: Option<ChainId>,
    /// The ERC-20 token address on the source EVM chain
    pub evm_token_address: [u8; 20],
    /// The EVM chain ID of the source chain (e.g. 8453 for Base)
    pub evm_source_chain_id: u64,
    /// If set, only this application can call Mint (via cross-app call)
    pub bridge_app_id: Option<ApplicationId>,
}

/// Event emitted when tokens are auto-burned on the bridge chain.
/// The relayer observes these on the "burns" stream and forwards
/// to EVM to release the corresponding ERC-20 tokens.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BurnEvent {
    /// The Ethereum address to receive the unlocked ERC-20 tokens
    pub target: [u8; 20],
    /// Amount of tokens burned, in raw sub-units of the source ERC-20.
    pub amount: U128,
}

/// Initial accounts and balances for the wrapped fungible token application.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitialState {
    pub accounts: BTreeMap<AccountOwner, U128>,
}

/// Builder for [`InitialState`].
#[derive(Debug, Default)]
pub struct InitialStateBuilder {
    account_balances: BTreeMap<AccountOwner, U128>,
}

impl InitialStateBuilder {
    pub fn with_account(mut self, account: AccountOwner, balance: U128) -> Self {
        self.account_balances.insert(account, balance);
        self
    }

    pub fn build(&self) -> InitialState {
        InitialState {
            accounts: self.account_balances.clone(),
        }
    }
}

/// Response variants returned by the wrapped fungible token application.
#[derive(Debug, StableEnumInCrate, Default)]
pub enum FungibleResponse {
    #[default]
    Ok,
    Balance(U128),
    TickerSymbol(String),
}

/// Operations for the wrapped fungible token application.
#[derive(Debug, StableEnumInCrate, GraphQLMutationRootInCrate)]
pub enum WrappedFungibleOperation {
    /// Requests an account balance.
    Balance { owner: AccountOwner },
    /// Requests this fungible token's ticker symbol.
    TickerSymbol,
    /// Approve the transfer of tokens.
    Approve {
        owner: AccountOwner,
        spender: AccountOwner,
        allowance: U128,
    },
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account.
    Transfer {
        owner: AccountOwner,
        amount: U128,
        target_account: Account,
    },
    /// Transfers tokens from a (locally owned) account using a previously approved allowance.
    TransferFrom {
        owner: AccountOwner,
        spender: AccountOwner,
        amount: U128,
        target_account: Account,
    },
    /// Same as `Transfer` but the source account may be remote.
    Claim {
        source_account: Account,
        amount: U128,
        target_account: Account,
    },
    /// Mints new tokens and transfers them to a target account. Only the authorized
    /// minter can call this.
    MintAndTransfer {
        target_account: Account,
        amount: U128,
    },
    /// Burns tokens from an account. Rejected by the contract — burning happens
    /// automatically when tokens are transferred to an Address20 on the bridge chain.
    Burn { owner: AccountOwner, amount: U128 },
}

/// Cross-chain message used by the wrapped fungible token application.
/// Amounts are [`U128`] in the source ERC-20's decimal scale.
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    /// Credits the given `target` account, unless the message is bouncing, in which case
    /// `source` is credited instead.
    Credit {
        target: AccountOwner,
        amount: U128,
        source: AccountOwner,
    },

    /// Withdraws from the given account and starts a transfer to the target account.
    Withdraw {
        owner: AccountOwner,
        amount: U128,
        target_account: Account,
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
