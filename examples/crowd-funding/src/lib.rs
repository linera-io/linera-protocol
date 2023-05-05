// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::SimpleObject;
use fungible::AccountOwner;
use linera_sdk::base::{Amount, Timestamp};
use serde::{Deserialize, Serialize};

/// The initialization arguments required to create a crowd-funding campaign.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, SimpleObject)]
pub struct InitializationArguments {
    /// The receiver of the pledges of a successful campaign.
    pub owner: AccountOwner,
    /// The deadline of the campaign, after which it can be cancelled if it hasn't met its target.
    pub deadline: Timestamp,
    /// The funding target of the campaign.
    pub target: Amount,
}

impl std::fmt::Display for InitializationArguments {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).expect("Serialization failed")
        )
    }
}

/// Operations that can be sent to the application.
#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum Operation {
    /// Pledge some tokens to the campaign (from an account on the current chain to the campaign chain).
    PledgeWithTransfer { owner: AccountOwner, amount: Amount },
    /// Collect the pledges after the campaign has reached its target (campaign chain only).
    Collect,
    /// Cancel the campaign and refund all pledges after the campaign has reached its deadline (campaign chain only).
    Cancel,
}

/// Effects that can be processed by the application.
#[derive(Debug, Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum Effect {
    /// Pledge some tokens to the campaign (from an account on the receiver chain).
    PledgeWithAccount { owner: AccountOwner, amount: Amount },
}

/// A cross-application call. This is meant to mimic operations, except triggered by another contract.
#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum ApplicationCall {
    /// Pledge some tokens to the campaign (from an account on the current chain).
    PledgeWithTransfer { owner: AccountOwner, amount: Amount },
    /// Pledge some tokens to the campaign from a session (for now, campaign chain only).
    PledgeWithSessions { source: AccountOwner },
    /// Collect the pledges after the campaign has reached its target (campaign chain only).
    Collect,
    /// Cancel the campaign and refund all pledges after the campaign has reached its deadline (campaign chain only).
    Cancel,
}
