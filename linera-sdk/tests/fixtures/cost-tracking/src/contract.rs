// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::hint::black_box;

use cost_tracking::{CostTrackingAbi, LogEntry, Message, Operation};
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount, WithContractAbi},
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
        let value = self.state.counter.get();
        black_box(value);
        self.log_entry("after_storage_read");
    }

    /// Test storage write operations.
    async fn test_storage_write(&mut self) {
        self.log_entry("before_storage_write");
        self.state.counter.set(42);
        self.log_entry("after_storage_write");
    }

    /// Test JSON serialization.
    fn test_json_serialization(&mut self) {
        self.log_entry("before_json_serialize");

        let data = LogEntry {
            label: "test_data".to_string(),
            fuel: 12345,
        };
        let serialized = serde_json::to_vec(&data).expect("serialization failed");
        black_box(&serialized);

        self.log_entry("after_json_serialize");
    }

    /// Test JSON deserialization.
    fn test_json_deserialization(&mut self) {
        let json_str = r#"{"label":"test_label","fuel":999}"#;
        self.log_entry("before_json_deserialize");
        let deserialized: LogEntry =
            serde_json::from_str(json_str).expect("deserialization failed");
        black_box(&deserialized);

        self.log_entry("after_json_deserialize");
    }

    /// Test BCS serialization.
    fn test_bcs_serialization(&mut self) {
        let data = LogEntry {
            label: "test_data".to_string(),
            fuel: 12345,
        };
        self.log_entry("before_bcs_serialize");
        let serialized = bcs::to_bytes(&data).expect("bcs serialization failed");
        black_box(&serialized);

        self.log_entry("after_bcs_serialize");
    }

    /// Test BCS deserialization.
    fn test_bcs_deserialization(&mut self) {
        // First serialize to get valid BCS bytes
        let data = LogEntry {
            label: "test_label".to_string(),
            fuel: 999,
        };
        let bytes = bcs::to_bytes(&data).expect("bcs serialization failed");
        self.log_entry("before_bcs_deserialize");
        let deserialized: LogEntry = bcs::from_bytes(&bytes).expect("bcs deserialization failed");
        black_box(&deserialized);

        self.log_entry("after_bcs_deserialize");
    }

    /// Test bincode serialization.
    fn test_bincode_serialization(&mut self) {
        let data = LogEntry {
            label: "test_data".to_string(),
            fuel: 12345,
        };
        self.log_entry("before_bincode_serialize");
        let serialized = bincode::serialize(&data).expect("bincode serialization failed");
        black_box(&serialized);

        self.log_entry("after_bincode_serialize");
    }

    /// Test bincode deserialization.
    fn test_bincode_deserialization(&mut self) {
        // First serialize to get valid bincode bytes
        let data = LogEntry {
            label: "test_label".to_string(),
            fuel: 999,
        };
        let bytes = bincode::serialize(&data).expect("bincode serialization failed");
        self.log_entry("before_bincode_deserialize");
        let deserialized: LogEntry =
            bincode::deserialize(&bytes).expect("bincode deserialization failed");
        black_box(&deserialized);

        self.log_entry("after_bincode_deserialize");
    }

    /// Test basic runtime operations.
    fn test_runtime_operations(&mut self) {
        self.log_entry("before_chain_id");
        let chain_id = self.runtime.chain_id();
        black_box(&chain_id);
        self.log_entry("after_chain_id");

        self.log_entry("before_block_height");
        let block_height = self.runtime.block_height();
        black_box(&block_height);
        self.log_entry("after_block_height");

        self.log_entry("before_system_time");
        let system_time = self.runtime.system_time();
        black_box(&system_time);
        self.log_entry("after_system_time");

        self.log_entry("before_application_id");
        let app_id = self.runtime.application_id();
        black_box(&app_id);
        self.log_entry("after_application_id");

        self.log_entry("before_application_parameters");
        black_box(&self.runtime.application_parameters());
        self.log_entry("after_application_parameters");
    }

    /// Test string operations (allocations).
    fn test_string_operations(&mut self) {
        self.log_entry("before_string_concat");

        let mut s = String::new();
        for i in 0..100 {
            s.push_str(&format!("item_{} ", i));
        }
        black_box(&s);

        self.log_entry("after_string_concat");

        self.log_entry("before_string_parse");
        let parsed: Vec<&str> = s.split_whitespace().collect();
        black_box(&parsed);
        self.log_entry("after_string_parse");
    }

    /// Test vector operations.
    fn test_vector_operations(&mut self) {
        self.log_entry("before_vector_alloc");

        let mut vec: Vec<u64> = Vec::with_capacity(1000);
        for i in 0..1000 {
            vec.push(i);
        }
        black_box(&vec);

        self.log_entry("after_vector_alloc");

        self.log_entry("before_vector_sort");
        vec.sort_by(|a, b| b.cmp(a)); // reverse sort
        black_box(&vec);
        self.log_entry("after_vector_sort");

        self.log_entry("before_vector_sum");
        let sum: u64 = vec.iter().sum();
        black_box(&sum);
        self.log_entry("after_vector_sum");
    }

    /// Test MapView operations.
    async fn test_map_operations(&mut self) {
        // Test insert
        self.log_entry("before_map_insert");
        self.state.map.insert(&"key1".to_string(), 100).unwrap();
        self.log_entry("after_map_insert");

        // Test get
        self.log_entry("before_map_get");
        let value = self.state.map.get(&"key1".to_string()).await.unwrap();
        black_box(&value);
        self.log_entry("after_map_get");

        // Test contains_key
        self.log_entry("before_map_contains_key");
        let contains = self
            .state
            .map
            .contains_key(&"key1".to_string())
            .await
            .unwrap();
        black_box(&contains);
        self.log_entry("after_map_contains_key");

        // Insert multiple entries
        self.log_entry("before_map_insert_10");
        for i in 0..10 {
            self.state
                .map
                .insert(&format!("key_{}", i), i as u64)
                .unwrap();
        }
        self.log_entry("after_map_insert_10");

        // Test remove
        self.log_entry("before_map_remove");
        self.state.map.remove(&"key1".to_string()).unwrap();
        self.log_entry("after_map_remove");
    }

    /// Test transfer operations.
    fn test_transfer(&mut self) {
        // Transfer from chain balance to chain balance (same chain)
        let chain_id = self.runtime.chain_id();
        let destination = linera_sdk::linera_base_types::Account {
            chain_id,
            owner: AccountOwner::CHAIN,
        };
        // Transfer a small amount (1 unit = 10^-18 tokens)
        self.log_entry("before_transfer");
        self.runtime
            .transfer(AccountOwner::CHAIN, destination, Amount::from_attos(1));

        self.log_entry("after_transfer");
    }

    /// Test message emission.
    fn test_send_message(&mut self) {
        let chain_id = self.runtime.chain_id();

        self.log_entry("before_send_message");
        self.runtime.send_message(chain_id, Message::Ping);
        self.log_entry("after_send_message");

        // Test prepare_message with tracking
        self.log_entry("before_send_message_tracked");
        self.runtime
            .prepare_message(Message::Ping)
            .with_tracking()
            .send_to(chain_id);
        self.log_entry("after_send_message_tracked");
    }

    /// Run all cost tracking operations.
    async fn run_all(&mut self) {
        self.log_entry("start");

        // Storage operations
        self.test_storage_read().await;
        self.test_storage_write().await;

        // JSON serialization/deserialization
        self.test_json_serialization();
        self.test_json_deserialization();

        // BCS serialization/deserialization
        self.test_bcs_serialization();
        self.test_bcs_deserialization();

        // Bincode serialization/deserialization
        self.test_bincode_serialization();
        self.test_bincode_deserialization();

        // Runtime operations
        self.test_runtime_operations();

        // Memory/allocation operations
        self.test_string_operations();
        self.test_vector_operations();

        // MapView operations
        self.test_map_operations().await;

        // Transfer operations
        self.test_transfer();

        // Message operations
        self.test_send_message();

        self.log_entry("end");
    }
}

impl Contract for CostTrackingContract {
    type Message = Message;
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

    async fn execute_operation(&mut self, operation: Operation) {
        match operation {
            Operation::RunAll => {
                self.run_all().await;
            }
        }
    }

    async fn execute_message(&mut self, message: Message) {
        match message {
            Message::Ping => {
                // No-op, just for benchmarking message reception
            }
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
