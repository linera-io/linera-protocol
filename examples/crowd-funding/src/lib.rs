// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{Request, Response, SimpleObject};
use fungible::AccountOwner;
use linera_sdk::base::{Amount, ApplicationId, ContractAbi, ServiceAbi, Timestamp};
use serde::{Deserialize, Serialize};

// TODO(#768): Remove the derive macros.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct CrowdFundingAbi;

impl ContractAbi for CrowdFundingAbi {
    type InitializationArgument = InitializationArgument;
    type Parameters = ApplicationId<fungible::FungibleTokenAbi>;
    type Operation = Operation;
    type ApplicationCall = ApplicationCall;
    type Effect = Effect;
    type SessionCall = ();
    type Response = ();
    type SessionState = ();
}

impl ServiceAbi for CrowdFundingAbi {
    type Parameters = ApplicationId<fungible::FungibleTokenAbi>;
    type Query = Request;
    type QueryResponse = Response;
}

/// The initialization data required to create a crowd-funding campaign.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, SimpleObject)]
pub struct InitializationArgument {
    /// The receiver of the pledges of a successful campaign.
    pub owner: AccountOwner,
    /// The deadline of the campaign, after which it can be cancelled if it hasn't met its target.
    pub deadline: Timestamp,
    /// The funding target of the campaign.
    pub target: Amount,
}

impl std::fmt::Display for InitializationArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).expect("Serialization failed")
        )
    }
}

/// Operations that can be sent to the application.
#[derive(Debug, Deserialize, Serialize)]
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
#[derive(Debug, Deserialize, Serialize)]
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
