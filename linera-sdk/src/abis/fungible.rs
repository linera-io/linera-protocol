// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An ABI for applications that implement a fungible token.

use std::collections::BTreeMap;

use async_graphql::{InputObject, SimpleObject};
use linera_base::{
    data_types::Amount,
    identifiers::{AccountOwner, ChainId},
};
use serde::{Deserialize, Serialize};

/// A native fungible response
#[derive(Debug, Deserialize, Serialize, Default)]
pub enum FungibleResponse {
    /// OK response
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

    /// Returns the serialized initial state of the application, ready to use as the
    /// initialization argument.
    pub fn build(&self) -> InitialState {
        InitialState {
            accounts: self.account_balances.clone(),
        }
    }
}
