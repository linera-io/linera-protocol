// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{RegisterView, RootView, ViewStorageContext};

/// The application state.
#[derive(RootView)]
#[view(context = "ViewStorageContext")]
pub struct ReentrantCounter {
    pub value: RegisterView<u64>,
}
