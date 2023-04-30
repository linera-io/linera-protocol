// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::AccountOwner;
use linera_sdk::base::Amount;
use serde::{Deserialize, Serialize};

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

/// Queries that can be made to the [`CrowdFunding`] application service.
#[derive(Clone, Copy, Debug, Deserialize)]
pub enum Query {
    /// The current [`Status`] of the crowd-funding campaign.
    Status,
    /// The total amount pledged to the crowd-funding campaign.
    Pledged,
    /// The crowd-funding campaign's target.
    Target,
    /// The crowd-funding campaign's deadline.
    Deadline,
    /// The recipient of the pledged amount.
    Owner,
}
