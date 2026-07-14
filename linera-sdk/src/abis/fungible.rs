// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An ABI for applications that implement a fungible token.

use std::collections::BTreeMap;

use async_graphql::{Request, Response};
use linera_base::{
    abi::{ContractAbi, ServiceAbi},
    data_types::U128,
    identifiers::{Account, AccountOwner},
};
use linera_sdk_derive::{GraphQLMutationRootInCrate, StableEnumInCrate};
use serde::{Deserialize, Serialize};

/// An operation
#[derive(Debug, StableEnumInCrate, GraphQLMutationRootInCrate)]
pub enum FungibleOperation {
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
        allowance: U128,
    },
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account.
    Transfer {
        /// Owner to transfer from
        owner: AccountOwner,
        /// Amount to be transferred
        amount: U128,
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
        amount: U128,
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
        amount: U128,
        /// Target account to claim the amount into
        target_account: Account,
    },
}

/// An ABI for applications that implement a fungible token.
pub struct FungibleTokenAbi;

impl ContractAbi for FungibleTokenAbi {
    type Operation = FungibleOperation;
    type Response = FungibleResponse;
}

impl ServiceAbi for FungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// A native fungible response
#[derive(Debug, StableEnumInCrate, Default)]
pub enum FungibleResponse {
    /// OK response
    #[default]
    Ok,
    /// Balance response
    Balance(U128),
    /// Ticker symbol response
    TickerSymbol(String),
}

/// The initial state to instantiate fungible with
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct InitialState {
    /// Accounts and their respective initial balances
    pub accounts: BTreeMap<AccountOwner, U128>,
}

/// The default number of decimal places used to display token amounts.
fn default_decimals() -> u8 {
    18
}

/// The parameters to instantiate fungible with
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Parameters {
    /// Ticker symbol for the fungible
    pub ticker_symbol: String,
    /// Number of decimal places used to display token amounts
    #[serde(default = "default_decimals")]
    pub decimals: u8,
}

impl Parameters {
    /// Instantiate parameters with the default precision of 18 decimals.
    pub fn new(ticker_symbol: &str) -> Self {
        let ticker_symbol = ticker_symbol.to_string();
        Self {
            ticker_symbol,
            decimals: default_decimals(),
        }
    }

    /// Sets the number of decimal places used to display token amounts.
    pub fn with_decimals(mut self, decimals: u8) -> Self {
        self.decimals = decimals;
        self
    }
}

/// A builder type for constructing the initial state of the application.
#[derive(Debug, Default)]
pub struct InitialStateBuilder {
    /// Accounts and their respective initial balances
    account_balances: BTreeMap<AccountOwner, U128>,
}

impl InitialStateBuilder {
    /// Adds an account to the initial state of the application.
    pub fn with_account(mut self, account: AccountOwner, balance: impl Into<U128>) -> Self {
        self.account_balances.insert(account, balance.into());
        self
    }

    /// Returns the serialized initial state of the application, ready to use as the
    /// initialization argument.
    pub fn build(&self) -> InitialState {
        InitialState {
            accounts: self.account_balances.clone(),
        }
    }
}
