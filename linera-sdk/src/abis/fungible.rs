// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An ABI for applications that implement a fungible token.

use std::collections::BTreeMap;

use async_graphql::{InputObject, Request, Response, SimpleObject};
use linera_base::{
    abi::{ContractAbi, ServiceAbi},
    data_types::Amount,
    identifiers::{AccountOwner, ChainId},
};
use linera_sdk_derive::GraphQLMutationRootInCrate;
use serde::{Deserialize, Serialize};

/// An ABI for applications that implement a fungible token.
pub struct FungibleTokenAbi;

impl ContractAbi for FungibleTokenAbi {
    type Operation = Operation;
    type Response = FungibleResponse;
}

impl ServiceAbi for FungibleTokenAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// An operation
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRootInCrate)]
pub enum Operation {
    /// Requests an account balance.
    Balance {
        /// Owner to query the balance for
        owner: AccountOwner,
    },
    /// Requests this fungible token's ticker symbol.
    TickerSymbol,
    /// Transfers tokens from a (locally owned) account to a (possibly remote) account.
    Transfer {
        /// Owner to transfer from
        owner: AccountOwner,
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
}

/// A fungible response
#[derive(Debug, Deserialize, Serialize, Default)]
pub enum FungibleResponse {
    /// Ok response
    #[default]
    Ok,
    /// Balance response
    Balance(Amount),
    /// Ticker symbol response
    TickerSymbol(String),
}

/// The initial state to instantiate fungible with
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct InitialState {
    /// Accounts and their respective initial balances
    pub accounts: BTreeMap<AccountOwner, Amount>,
}

/// The parameters to instantiate fungible with
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Parameters {
    /// Ticker symbol for the fungible
    pub ticker_symbol: String,
}

impl Parameters {
    /// Instantiate parameters
    pub fn new(ticker_symbol: &str) -> Self {
        let ticker_symbol = ticker_symbol.to_string();
        Self { ticker_symbol }
    }
}

/// An account.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    SimpleObject,
    InputObject,
)]
#[graphql(input_name = "FungibleAccount")]
pub struct Account {
    /// Chain ID of the account
    pub chain_id: ChainId,
    /// Owner of the account
    pub owner: AccountOwner,
}

/// A builder type for constructing the initial state of the application.
#[derive(Debug, Default)]
pub struct InitialStateBuilder {
    /// Accounts and their respective initial balances
    account_balances: BTreeMap<AccountOwner, Amount>,
}

impl InitialStateBuilder {
    /// Adds an account to the initial state of the application.
    pub fn with_account(mut self, account: AccountOwner, balance: impl Into<Amount>) -> Self {
        self.account_balances.insert(account, balance.into());
        self
    }

    /// Returns the serialized initial state of the application, ready to used as the
    /// initilization argument.
    pub fn build(&self) -> InitialState {
        InitialState {
            accounts: self.account_balances.clone(),
        }
    }
}
