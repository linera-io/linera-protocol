// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use counter_no_graphql::{CounterNoGraphQlAbi, CounterOperation, CounterRequest};
use create_and_call::{CreateAndCallAbi, CreateAndCallOperation};
use linera_sdk::{
    linera_base_types::{Bytecode, VmRuntime, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::CreateAndCallState;

pub struct CreateAndCallContract {
    state: CreateAndCallState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(CreateAndCallContract);

impl WithContractAbi for CreateAndCallContract {
    type Abi = CreateAndCallAbi;
}

impl Contract for CreateAndCallContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = CreateAndCallState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        CreateAndCallContract { state, runtime }
    }

    async fn instantiate(&mut self, _value: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();

        self.state.value.set(None);
    }

    async fn execute_operation(&mut self, operation: CreateAndCallOperation) -> u64 {
        let CreateAndCallOperation::CreateAndCall(
            contract_bytes,
            service_bytes,
            initialization_value,
            increment_value,
        ) = operation;

        // Step 1: Convert Vec<u8> to Bytecode and publish module with Wasm runtime
        let contract_bytecode = Bytecode::new(contract_bytes);
        let service_bytecode = Bytecode::new(service_bytes);
        let module_id =
            self.runtime
                .publish_module(contract_bytecode, service_bytecode, VmRuntime::Wasm);

        // Step 2: Create application with initialization value
        let application_id = self
            .runtime
            .create_application::<CounterNoGraphQlAbi, (), u64>(
                module_id,
                &(),
                &initialization_value,
                vec![],
            );
        self.state.value.set(Some(application_id));

        // Step 3: Call the service. It should return the value before
        // the initialization of this contract and thus zero.
        let counter_request = CounterRequest::Query;
        let value = self.runtime.query_service(application_id, counter_request);
        assert_eq!(value, 0);

        // Step 4: Call the contract with counter increment operation
        let counter_operation = CounterOperation::Increment(increment_value);
        self.runtime
            .call_application(true, application_id, &counter_operation)
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Create and call application doesn't support any cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
