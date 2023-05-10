// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::scalar;
use crowd_funding::InitializationArguments;
use fungible::AccountOwner;
use linera_sdk::base::Amount;
use linera_views::{
    map_view::MapView,
    register_view::RegisterView,
    views::{GraphQLView, RootView},
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
#[derive(RootView, GraphQLView)]
pub struct CrowdFunding<C> {
    /// The status of the campaign.
    pub status: RegisterView<C, Status>,
    /// The map of pledges that will be collected if the campaign succeeds.
    pub pledges: MapView<C, AccountOwner, Amount>,
    /// The initialization arguments that determine the details the campaign.
    pub initialization_arguments: RegisterView<C, Option<InitializationArguments>>,
}

#[allow(dead_code)]
impl Status {
    /// Returns `true` if the campaign status is [`Status::Complete`].
    pub fn is_complete(&self) -> bool {
        matches!(self, Status::Complete)
    }
}
