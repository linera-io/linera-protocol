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

    fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = TaskProcessorState::load(runtime.root_view_storage_context())
            .expect("Failed to load state");
        TaskProcessorContract { state, runtime }
    }

    fn instantiate(&mut self, _argument: ()) {
        self.runtime.application_parameters();
    }

    fn execute_operation(&mut self, operation: TaskProcessorOperation) {
        match operation {
            TaskProcessorOperation::RequestTask { operator, input } => {
                self.state
                    .pending_tasks
                    .push_back(PendingTask { operator, input });
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
            TaskProcessorOperation::StoreResult { result } => {
                // Remove the first pending task (the one that was just processed).
                self.state.pending_tasks.delete_front();
                self.state.results.push_back(result);
                let count = self.state.task_count.get() + 1;
                self.state.task_count.set(count);
            }
        }
    }

    fn execute_message(&mut self, message: Message) {
        match message {
            Message::RequestTask { operator, input } => {
                self.state
                    .pending_tasks
                    .push_back(PendingTask { operator, input });
            }
        }
    }

    fn store(self) {
        self.state.save_and_drop().expect("Failed to save state");
    }
}
