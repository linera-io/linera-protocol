// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Crowd-funding Example Application */

use async_graphql::{Request, Response, SimpleObject};
use linera_sdk::{
    base::{AccountOwner, Amount, ContractAbi, ServiceAbi, Timestamp},
    graphql::GraphQLMutationRoot,
};
use serde::{Deserialize, Serialize};

pub struct CrowdFundingAbi;

impl ContractAbi for CrowdFundingAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for CrowdFundingAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// The instantiation data required to create a crowd-funding campaign.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, SimpleObject)]
pub struct InstantiationArgument {
    /// The receiver of the pledges of a successful campaign.
    pub owner: AccountOwner,
    /// The deadline of the campaign, after which it can be cancelled if it hasn't met its target.
    pub deadline: Timestamp,
    /// The funding target of the campaign.
    pub target: Amount,
}

impl std::fmt::Display for InstantiationArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).expect("Serialization failed")
        )
    }
}

/// Operations that can be executed by the application.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Pledge some tokens to the campaign (from an account on the current chain to the campaign chain).
    Pledge { owner: AccountOwner, amount: Amount },
    /// Collect the pledges after the campaign has reached its target (campaign chain only).
    Collect,
    /// Cancel the campaign and refund all pledges after the campaign has reached its deadline (campaign chain only).
    Cancel,
}

/// Messages that can be exchanged across chains from the same application instance.
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    /// Pledge some tokens to the campaign (from an account on the receiver chain).
    PledgeWithAccount { owner: AccountOwner, amount: Amount },
}
