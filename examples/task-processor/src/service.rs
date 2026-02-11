// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use linera_sdk::{
    linera_base_types::{Timestamp, WithServiceAbi},
    task_processor::{ProcessorActions, Task, TaskOutcome},
    views::View,
    Service, ServiceRuntime,
};
use task_processor::{TaskProcessorAbi, TaskProcessorOperation};

use self::state::TaskProcessorState;

pub struct TaskProcessorService {
    state: Arc<TaskProcessorState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(TaskProcessorService);

impl WithServiceAbi for TaskProcessorService {
    type Abi = TaskProcessorAbi;
}

impl Service for TaskProcessorService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = TaskProcessorState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        TaskProcessorService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            QueryRoot {
                state: self.state.clone(),
                runtime: self.runtime.clone(),
            },
            MutationRoot {
                runtime: self.runtime.clone(),
            },
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

struct QueryRoot {
    state: Arc<TaskProcessorState>,
    runtime: Arc<ServiceRuntime<TaskProcessorService>>,
}

#[Object]
impl QueryRoot {
    /// Returns the current task count.
    async fn task_count(&self) -> u64 {
        *self.state.task_count.get()
    }

    /// Returns the stored results in order.
    async fn results(&self) -> Vec<String> {
        let count = self.state.results.count();
        self.state
            .results
            .read_front(count)
            .await
            .unwrap_or_default()
    }

    /// Returns the pending tasks and callback requests for the task processor.
    async fn next_actions(
        &self,
        _last_requested_callback: Option<Timestamp>,
        _now: Timestamp,
    ) -> ProcessorActions {
        let mut actions = ProcessorActions::default();

        // Get all pending tasks from the queue.
        let count = self.state.pending_tasks.count();
        if let Ok(pending_tasks) = self.state.pending_tasks.read_front(count).await {
            for pending in pending_tasks {
                actions.execute_tasks.push(Task {
                    operator: pending.operator,
                    input: pending.input,
                });
            }
        }

        actions
    }

    /// Processes the outcome of a completed task and schedules operations.
    async fn process_task_outcome(&self, outcome: TaskOutcome) -> bool {
        // Schedule an operation to store the result and remove the pending task.
        let operation = TaskProcessorOperation::StoreResult {
            result: outcome.output,
        };
        self.runtime.schedule_operation(&operation);
        true
    }
}

struct MutationRoot {
    runtime: Arc<ServiceRuntime<TaskProcessorService>>,
}

#[Object]
impl MutationRoot {
    /// Requests a task to be processed by an off-chain operator.
    async fn request_task(&self, operator: String, input: String) -> [u8; 0] {
        let operation = TaskProcessorOperation::RequestTask { operator, input };
        self.runtime.schedule_operation(&operation);
        []
    }
}
