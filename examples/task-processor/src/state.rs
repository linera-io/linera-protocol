// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, SyncQueueView, SyncRegisterView, SyncView, ViewStorageContext};
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
#[derive(SyncView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct TaskProcessorState {
    /// Pending tasks to be executed.
    pub pending_tasks: SyncQueueView<PendingTask>,
    /// Results from completed tasks.
    pub results: SyncQueueView<String>,
    /// Counter for tracking how many tasks have been processed.
    pub task_count: SyncRegisterView<u64>,
}
