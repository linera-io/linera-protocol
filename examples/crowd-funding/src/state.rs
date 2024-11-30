// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::scalar;
use crowd_funding::InstantiationArgument;
use linera_sdk::{
    base::{AccountOwner, Amount},
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};
use serde::{Deserialize, Serialize};

/// The status of a crowd-funding campaign.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub enum Status {
    /// The campaign is active and can receive pledges.
    #[default]
    Active,
    /// The campaign has ended successfully and still receive additional pledges.
    Complete,
    /// The campaign was cancelled, all pledges have been returned and no more pledges can be made.
    Cancelled,
}

scalar!(Status);

/// The crowd-funding campaign's state.
#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct CrowdFundingState {
    /// The status of the campaign.
    pub status: RegisterView<Status>,
    /// The map of pledges that will be collected if the campaign succeeds.
    pub pledges: MapView<AccountOwner, Amount>,
    /// The instantiation data that determine the details the campaign.
    pub instantiation_argument: RegisterView<Option<InstantiationArgument>>,
}

#[allow(dead_code)]
impl Status {
    /// Returns `true` if the campaign status is [`Status::Complete`].
    pub fn is_complete(&self) -> bool {
        matches!(self, Status::Complete)
    }
}
