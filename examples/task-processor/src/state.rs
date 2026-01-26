// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, QueueView, RegisterView, ViewStorageContext};
use linera_sdk::RootView;
use serde::{Deserialize, Serialize};

/// A pending task stored in the application state.
#[derive(Clone, Debug, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct PendingTask {
    /// The operator to execute the task.
    pub operator: String,
    /// The input to pass to the operator.
    pub input: String,
}

/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct TaskProcessorState {
    /// Pending tasks to be executed.
    pub pending_tasks: QueueView<PendingTask>,
    /// Results from completed tasks.
    pub results: QueueView<String>,
    /// Counter for tracking how many tasks have been processed.
    pub task_count: RegisterView<u64>,
}
