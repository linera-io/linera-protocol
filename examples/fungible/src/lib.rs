// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{scalar, InputObject};
use linera_sdk::base::{Amount, ApplicationId, ChainId, Owner};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// An operation.
#[derive(Deserialize, Serialize)]
pub enum Operation {
    /// Obtain the total balance of the accounts
    TotalBalance { },
    /// A transfer from a (locally owned) account to a (possibly remote) account.
    Transfer {
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
    },
    /// Same as transfer but the source account may be remote. Depending on its
    /// configuration (see also #464), the target chain may take time or refuse to process
    /// the message.
    Claim {
        source_account: Account,
        amount: Amount,
        target_account: Account,
    },
}

/// An effect.
#[derive(Deserialize, Serialize)]
pub enum Effect {
    /// Credit the given account.
    Credit { owner: AccountOwner, amount: Amount },

    /// Withdraw from the given account and starts a transfer to the target account.
    Withdraw {
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
    },
}

/// A cross-application call.
#[derive(Deserialize, Serialize)]
pub enum ApplicationCall {
    /// A request for an account balance.
    Balance { owner: AccountOwner },
    /// A transfer from an account.
    Transfer {
        owner: AccountOwner,
        amount: Amount,
        destination: Destination,
    },
    /// Same as transfer but the source account may be remote.
    Claim {
        source_account: Account,
        amount: Amount,
        target_account: Account,
    },
}

/// A cross-application call into a session.
#[derive(Deserialize, Serialize)]
pub enum SessionCall {
    /// A request for the session's balance.
    Balance,
    /// A transfer from the session.
    Transfer {
        amount: Amount,
        destination: Destination,
    },
}

scalar!(AccountOwner);

/// An account owner.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum AccountOwner {
    /// An account owned by a user.
    User(Owner),
    /// An account for an application.
    Application(ApplicationId),
}

impl<T> From<T> for AccountOwner
where
    T: Into<Owner>,
{
    fn from(owner: T) -> Self {
        AccountOwner::User(owner.into())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct InitialState {
    pub accounts: BTreeMap<AccountOwner, Amount>,
}

impl std::fmt::Display for InitialState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let bytes = bcs::to_bytes(self).expect("Serialization failed");
        write!(f, "{}", hex::encode(bytes))
    }
}

/// An account.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, InputObject,
)]
pub struct Account {
    pub chain_id: ChainId,
    pub owner: AccountOwner,
}

#[derive(Deserialize, Serialize)]
pub enum Destination {
    Account(Account),
    NewSession,
}

scalar!(Destination);

/// A builder type for constructing the initial state of the application.
#[derive(Debug, Default)]
pub struct InitialStateBuilder {
    account_balances: BTreeMap<AccountOwner, Amount>,
}

impl InitialStateBuilder {
    /// Adds an account to the initial state of the application.
    pub fn with_account(mut self, account: AccountOwner, balance: impl Into<Amount>) -> Self {
        self.account_balances.insert(account, balance.into());
        self
    }

    /// Returns the serialized initial state of the application, ready to used as the
    /// initialization argument.
    pub fn build(&self) -> Vec<u8> {
        bcs::to_bytes(&self.account_balances).expect("Failed to serialize initial state")
    }
}
