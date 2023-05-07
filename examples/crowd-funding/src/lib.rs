// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::AccountOwner;
use linera_sdk::base::Amount;
use serde::{Deserialize, Serialize};
use linera_sdk::base::ApplicationId;
use async_graphql::SimpleObject;
use linera_sdk::base::Timestamp;

/// The parameters required to create a crowd-funding campaign.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, SimpleObject)]
pub struct Parameters {
    /// The receiver of the pledges of a successful campaign.
    pub owner: AccountOwner,
    /// The token to use for pledges.
    pub token: ApplicationId,
    /// The deadline of the campaign, after which it can be cancelled if it hasn't met its target.
    pub deadline: Timestamp,
    /// The funding target of the campaign.
    pub target: Amount,
}

impl std::fmt::Display for Parameters {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let bytes = bcs::to_bytes(self).expect("Serialization failed");
        write!(f, "{}", hex::encode(bytes))
    }
}

/// A cross-application call.
#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum ApplicationCall {
    PledgeWithTransfer { owner: AccountOwner, amount: Amount },
    PledgeWithSessions { source: AccountOwner },
    Collect,
    Cancel,
}

/// Operations that can be sent to the application.
#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum Operation {
    /// Pledge some tokens to the campaign.
    PledgeWithTransfer { owner: AccountOwner, amount: Amount },
    /// Collect the pledges after the campaign has reached its target.
    Collect,
    /// Cancel the campaign and refund all pledges after the campaign has reached its deadline.
    Cancel,
}
