// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use cost_tracking::LogEntry;
use linera_sdk::views::{linera_views, LogView, RegisterView, RootView, ViewStorageContext};

/// The application state.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct CostTrackingState {
    /// Log of (label, fuel) entries tracking remaining fuel at various points.
    pub logs: LogView<LogEntry>,
    /// A counter used for testing storage access.
    pub counter: RegisterView<u64>,
    /// A string used for testing serialization.
    pub data: RegisterView<String>,
}
