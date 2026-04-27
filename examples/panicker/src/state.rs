// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};

// `RootView` derive requires at least one field, so keep a single empty register.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct PanickerState {
    pub _marker: RegisterView<()>,
}
