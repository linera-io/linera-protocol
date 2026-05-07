// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};

/// Stores the seed derived from the runtime context and a couple of samples
/// drawn from the seeded RNG.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct RandomSourceState {
    pub seed: RegisterView<u64>,
    pub sample1: RegisterView<u64>,
    pub sample2: RegisterView<u64>,
}
