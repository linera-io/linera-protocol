// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crowd_funding::Parameters;
use fungible::AccountOwner;
use linera_sdk::base::Amount;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

/// The crowd-funding campaign's state.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CrowdFunding {
    /// The status of the campaign.
    pub status: Status,
    /// The map of pledges that will be collected if the campaign succeeds.
    pub pledges: BTreeMap<AccountOwner, Amount>,
    /// The parameters that determine the details the campaign.
    pub parameters: Option<Parameters>,
}

#[allow(dead_code)]
impl Status {
    /// Returns `true` if the campaign status is [`Status::Complete`].
    pub fn is_complete(&self) -> bool {
        matches!(self, Status::Complete)
    }
}

impl CrowdFunding {
    /// Retrieves the campaign [`Parameters`] stored in the application's state.
    pub fn parameters(&self) -> &Parameters {
        self.parameters
            .as_ref()
            .expect("Application was not initialized")
    }
}
