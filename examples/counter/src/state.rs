// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::InputObject;
use serde::{Deserialize, Serialize};

/// The application state.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Counter {
    pub value: u64,
}

#[derive(Default, Serialize, Deserialize, Debug, InputObject)]
pub struct CounterOperation {
    pub increment: u64,
}