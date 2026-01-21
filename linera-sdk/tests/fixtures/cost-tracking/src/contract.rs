// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use cost_tracking::{CostTrackingAbi, LogEntry, Operation};
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::CostTrackingState;

pub struct CostTrackingContract {
    state: CostTrackingState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(CostTrackingContract);

impl WithContractAbi for CostTrackingContract {
    type Abi = CostTrackingAbi;
}

impl CostTrackingContract {
    /// Logs an entry with the current remaining fuel.
    fn log_entry(&mut self, label: impl Into<String>) {
        let fuel = self.runtime.remaining_fuel();
        self.state.logs.push(LogEntry {
            label: label.into(),
            fuel,
        });
    }

    /// Test storage read operations.
    async fn test_storage_read(&mut self) {
        self.log_entry("before_storage_read");
        let _value = self.state.counter.get();
        self.log_entry("after_storage_read");
    }

    /// Test storage write operations.
    async fn test_storage_write(&mut self) {
        self.log_entry("before_storage_write");
        self.state.counter.set(42);
        self.log_entry("after_storage_write");
    }

    /// Test serialization.
    fn test_serialization(&mut self) {
        self.log_entry("before_serialization");

        // Serialize a complex structure
        let data = LogEntry {
            label: "test_data".to_string(),
            fuel: 12345,
        };
        let _serialized = serde_json::to_vec(&data).expect("serialization failed");

        self.log_entry("after_serialization");
    }

    /// Test deserialization.
    fn test_deserialization(&mut self) {
        self.log_entry("before_deserialization");

        // Deserialize a JSON string
        let json_str = r#"{"label":"test_label","fuel":999}"#;
        let _deserialized: LogEntry = serde_json::from_str(json_str).expect("deserialization failed");

        self.log_entry("after_deserialization");
    }

    /// Test basic runtime operations.
    fn test_runtime_operations(&mut self) {
        self.log_entry("before_chain_id");
        let _chain_id = self.runtime.chain_id();
        self.log_entry("after_chain_id");

        self.log_entry("before_block_height");
        let _block_height = self.runtime.block_height();
        self.log_entry("after_block_height");

        self.log_entry("before_system_time");
        let _system_time = self.runtime.system_time();
        self.log_entry("after_system_time");

        self.log_entry("before_application_id");
        let _app_id = self.runtime.application_id();
        self.log_entry("after_application_id");

        self.log_entry("before_application_parameters");
        let _params = self.runtime.application_parameters();
        self.log_entry("after_application_parameters");
    }

    /// Test string operations (allocations).
    fn test_string_operations(&mut self) {
        self.log_entry("before_string_concat");

        let mut s = String::new();
        for i in 0..100 {
            s.push_str(&format!("item_{} ", i));
        }

        self.log_entry("after_string_concat");

        self.log_entry("before_string_parse");
        let _parsed: Vec<&str> = s.split_whitespace().collect();
        self.log_entry("after_string_parse");
    }

    /// Test vector operations.
    fn test_vector_operations(&mut self) {
        self.log_entry("before_vector_alloc");

        let mut vec: Vec<u64> = Vec::with_capacity(1000);
        for i in 0..1000 {
            vec.push(i);
        }

        self.log_entry("after_vector_alloc");

        self.log_entry("before_vector_sort");
        vec.sort_by(|a, b| b.cmp(a)); // reverse sort
        self.log_entry("after_vector_sort");

        self.log_entry("before_vector_sum");
        let _sum: u64 = vec.iter().sum();
        self.log_entry("after_vector_sum");
    }

    /// Run all cost tracking operations.
    async fn run_all(&mut self) {
        self.log_entry("start");

        // Storage operations
        self.test_storage_read().await;
        self.test_storage_write().await;

        // Serialization/deserialization
        self.test_serialization();
        self.test_deserialization();

        // Runtime operations
        self.test_runtime_operations();

        // Memory/allocation operations
        self.test_string_operations();
        self.test_vector_operations();

        self.log_entry("end");
    }
}

impl Contract for CostTrackingContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = CostTrackingState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");

        CostTrackingContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        self.runtime.application_parameters();
        self.state.counter.set(0);
        self.state.data.set(String::new());
    }

    async fn execute_operation(&mut self, operation: Operation) -> () {
        match operation {
            Operation::RunAll => {
                self.run_all().await;
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {}

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
