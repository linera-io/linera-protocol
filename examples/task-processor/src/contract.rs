// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};
use task_processor::{Message, TaskProcessorAbi, TaskProcessorOperation};

use self::state::{PendingTask, TaskProcessorState};

pub struct TaskProcessorContract {
    state: TaskProcessorState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(TaskProcessorContract);

impl WithContractAbi for TaskProcessorContract {
    type Abi = TaskProcessorAbi;
}

impl Contract for TaskProcessorContract {
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = TaskProcessorState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        TaskProcessorContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: TaskProcessorOperation) {
        match operation {
            TaskProcessorOperation::RequestTask { operator, input } => {
                self.add_pending_task(operator, input);
            }
            TaskProcessorOperation::RequestTaskOn {
                chain_id,
                operator,
                input,
            } => {
                self.runtime
                    .prepare_message(Message::RequestTask { operator, input })
                    .send_to(chain_id);
            }
            TaskProcessorOperation::StoreResult { id, result } => {
                self.remove_pending_task(id).await;
                self.state.results.push_back(result);
                let count = self.state.task_count.get() + 1;
                self.state.task_count.set(count);
            }
        }
    }

    async fn execute_message(&mut self, message: Message) {
        match message {
            Message::RequestTask { operator, input } => {
                self.add_pending_task(operator, input);
            }
        }
    }

    async fn store(self) {
        self.state
            .save_and_drop()
            .await
            .expect("Failed to save state");
    }
}

impl TaskProcessorContract {
    /// Queues a task under a fresh identifier.
    fn add_pending_task(&mut self, operator: String, input: String) {
        let id = *self.state.next_task_id.get();
        self.state.next_task_id.set(id + 1);
        self.state.pending_tasks.push_back(PendingTask {
            id,
            operator,
            input,
        });
    }

    /// Removes the pending task with the given identifier.
    ///
    /// It is not necessarily the oldest one: the task processor reports the outcome of an
    /// identified task as soon as it is available, even if a task requested earlier is
    /// still failing.
    async fn remove_pending_task(&mut self, id: u64) {
        let count = self.state.pending_tasks.count();
        let pending_tasks = self
            .state
            .pending_tasks
            .read_front(count)
            .await
            .expect("Failed to read pending tasks");
        self.state.pending_tasks.clear();
        for task in pending_tasks {
            if task.id != id {
                self.state.pending_tasks.push_back(task);
            }
        }
    }
}
