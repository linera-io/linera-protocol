// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An ABI for applications that implement a wrapped (bridged) fungible token with Mint/Burn.

use std::collections::BTreeMap;

use async_graphql::{Request, Response};
use linera_base::{
    abi::{ContractAbi, ServiceAbi},
    data_types::U128,
    identifiers::{AccountOwner, ApplicationId, ChainId},
};
use linera_sdk_derive::GraphQLMutationRootInCrate;
use serde::{Deserialize, Serialize};

pub use super::fungible::Account;

/// Parameters for a wrapped fungible token backed by an EVM bridge.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WrappedParameters {
    /// Ticker symbol (e.g. "USDC")
    pub ticker_symbol: String,
    /// Number of decimal places used by the source ERC-20 (e.g. 6 for USDC).
    pub decimals: u8,
    /// The chain on which minting and burning are allowed (the bridge chain).
    pub mint_chain_id: ChainId,
    /// The ERC-20 token address on the source EVM chain
    pub evm_token_address: [u8; 20],
    /// The EVM chain ID of the source chain (e.g. 8453 for Base)
    pub evm_source_chain_id: u64,
}

/// Event emitted by the bridge application on its "burns" stream when it burns
/// wrapped tokens on the bridge chain. The relayer observes these and forwards
/// them to EVM to release the corresponding ERC-20 tokens.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BurnEvent {
    /// The Ethereum address to receive the unlocked ERC-20 tokens
    pub target: [u8; 20],
    /// Amount of tokens burned, in raw sub-units of the source ERC-20.
    pub amount: U128,
}

/// Initial accounts and balances for the wrapped fungible token application.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[allow(missing_docs)]
pub struct InitialState {
    pub accounts: BTreeMap<AccountOwner, U128>,
}

/// Builder for [`InitialState`].
#[derive(Debug, Default)]
pub struct InitialStateBuilder {
    account_balances: BTreeMap<AccountOwner, U128>,
}

impl InitialStateBuilder {
    /// Adds an account with the given initial balance.
    pub fn with_account(mut self, account: AccountOwner, balance: U128) -> Self {
        self.account_balances.insert(account, balance);
        self
    }

    /// Builds the [`InitialState`] from the configured accounts.
    pub fn build(&self) -> InitialState {
        InitialState {
            accounts: self.account_balances.clone(),
        }
    }
}

/// Response variants returned by the wrapped fungible token application.
#[derive(Debug, Deserialize, Serialize, Default)]
#[allow(missing_docs)]
pub enum FungibleResponse {
    #[default]
    Ok,
    Balance(U128),
    TickerSymbol(String),
}

/// Operations for the wrapped fungible token application.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRootInCrate)]
#[allow(missing_docs)]
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
    /// Mints new tokens to a target account. Driven by the registered authorized
    /// caller (see [`Self::RegisterAuthorizedCaller`]) on the designated mint chain.
    Mint {
        target_account: Account,
        amount: U128,
    },
    /// Burns tokens from an account. Authorized only via the registered authorized
    /// caller (see [`Self::RegisterAuthorizedCaller`]) on the designated mint chain.
    Burn { owner: AccountOwner, amount: U128 },
    /// Registers the application authorized to drive `Mint`/`Burn`. Must run on
    /// the designated `mint_chain_id` — the only chain where it is consulted — and
    /// requires an authenticated signer. Because an authorized caller may take
    /// this token's id as a creation parameter, the two cannot reference each
    /// other at creation; this token is created first and registers its caller
    /// afterwards.
    RegisterAuthorizedCaller { app_id: ApplicationId },
}

/// Cross-chain message used by the wrapped fungible token application.
/// Amounts are [`U128`] in the source ERC-20's decimal scale.
#[derive(Debug, Deserialize, Serialize)]
#[allow(missing_docs)]
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
