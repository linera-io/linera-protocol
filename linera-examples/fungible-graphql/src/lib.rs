// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::InputObject;
use linera_sdk::base::{Amount, ApplicationId, ChainId, Owner};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, InputObject)]
pub struct Transfer {
    pub owner: AccountOwner,
    pub amount: Amount,
    pub target_account: Account,
}

/// An effect.
#[derive(Deserialize, Serialize)]
pub enum Effect {
    Credit { owner: AccountOwner, amount: Amount },
}

/// A cross-application call.
#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum ApplicationCall {
    /// A request for an account balance.
    Balance { owner: AccountOwner },
    /// A transfer from an account.
    Transfer {
        owner: AccountOwner,
        amount: Amount,
        destination: Destination,
    },
}

/// A cross-application call into a session.
#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum SessionCall {
    /// A request for the session's balance.
    Balance,
    /// A transfer from the session.
    Transfer {
        amount: Amount,
        destination: Destination,
    },
}

/// An account owner.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, InputObject,
)]
pub struct AccountOwner {
    /// An account owned by a user.
    user: Option<Owner>,
    /// An account for an application.
    application: Option<ApplicationId>,
}

impl AccountOwner {
    pub fn with_user(user: Owner) -> Self {
        Self {
            user: Some(user),
            application: None,
        }
    }

    pub fn with_application(application_id: ApplicationId) -> Self {
        Self {
            user: None,
            application: Some(application_id),
        }
    }

    pub fn user(&self) -> Option<&Owner> {
        self.user.as_ref()
    }

    pub fn application(&self) -> Option<&ApplicationId> {
        self.application.as_ref()
    }
}

/// An account.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, InputObject)]
pub struct Account {
    pub chain_id: ChainId,
    pub owner: AccountOwner,
}

#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum Destination {
    Account(Account),
    NewSession,
}
