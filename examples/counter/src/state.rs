// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

/// The application state.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Counter {
    pub value: u64,
}
