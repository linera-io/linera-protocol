// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::ApplicationId;
use serde::{Deserialize, Serialize};

/// The application state.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct SimpleMetaCounter {
    pub counter_id: Option<ApplicationId>,
}
