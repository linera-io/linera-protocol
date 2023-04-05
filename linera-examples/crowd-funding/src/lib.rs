// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::AccountOwner;
use linera_sdk::base::{Amount, ApplicationId, Timestamp};
use serde::{Deserialize, Serialize};

/// The initialization parameters of a crowd-funding campaign. As usual, these are meant
/// to be BCS-serialized and passed when instantiating the bytecode.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Parameters {
    /// The receiver of the pledges of a successful campaign (same chain as the campaign).
    pub owner: AccountOwner,
    /// The token to use for pledges.
    pub token: ApplicationId,
    /// The deadline of the campaign, after which it can be cancelled if it hasn't met its target.
    pub deadline: Timestamp,
    /// The funding target of the campaign.
    pub target: Amount,
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
#[derive(Deserialize, Serialize)]
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
