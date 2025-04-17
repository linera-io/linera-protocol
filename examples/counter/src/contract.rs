// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use counter::CounterAbi;
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::CounterState;

// ANCHOR: contract_struct
linera_sdk::contract!(CounterContract);

pub struct CounterContract {
    state: CounterState,
    runtime: ContractRuntime<Self>,
}
// ANCHOR_END: contract_struct

// ANCHOR: declare_abi
impl WithContractAbi for CounterContract {
    type Abi = CounterAbi;
}
// ANCHOR_END: declare_abi

impl Contract for CounterContract {
    type Message = ();
    type InstantiationArgument = u64;
    type Parameters = ();
    type EventValue = ();

    // ANCHOR: load
    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = CounterState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        CounterContract { state, runtime }
    }
    // ANCHOR_END: load

    // ANCHOR: instantiate
    async fn instantiate(&mut self, value: u64) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();

        self.state.value.set(value);
    }
    // ANCHOR_END: instantiate

    // ANCHOR: execute_operation
    async fn execute_operation(&mut self, operation: u64) -> u64 {
        let new_value = self.state.value.get() + operation;
        self.state.value.set(new_value);
        new_value
    }
    // ANCHOR_END: execute_operation

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    // ANCHOR: store
    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
    // ANCHOR_END: store
}

#[cfg(test)]
mod tests {
    use futures::FutureExt as _;
    use linera_sdk::{util::BlockingWait, views::View, Contract, ContractRuntime};

    use super::{CounterContract, CounterState};

    // ANCHOR: counter_test
    #[test]
    fn operation() {
        let runtime = ContractRuntime::new().with_application_parameters(());
        let state = CounterState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");
        let mut counter = CounterContract { state, runtime };

        let initial_value = 72_u64;
        counter
            .instantiate(initial_value)
            .now_or_never()
            .expect("Initialization of counter state should not await anything");

        let increment = 42_308_u64;
        let response = counter
            .execute_operation(increment)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        let expected_value = initial_value + increment;

        assert_eq!(response, expected_value);
        assert_eq!(*counter.state.value.get(), initial_value + increment);
    }
    // ANCHOR_END: counter_test

    #[test]
    #[should_panic(expected = "Counter application doesn't support any cross-chain messages")]
    fn message() {
        let initial_value = 72_u64;
        let mut counter = create_and_instantiate_counter(initial_value);

        counter
            .execute_message(())
            .now_or_never()
            .expect("Execution of counter operation should not await anything");
    }

    #[test]
    fn cross_application_call() {
        let initial_value = 2_845_u64;
        let mut counter = create_and_instantiate_counter(initial_value);

        let increment = 8_u64;

        let response = counter
            .execute_operation(increment)
            .now_or_never()
            .expect("Execution of counter operation should not await anything");

        let expected_value = initial_value + increment;

        assert_eq!(response, expected_value);
        assert_eq!(*counter.state.value.get(), expected_value);
    }

    fn create_and_instantiate_counter(initial_value: u64) -> CounterContract {
        let runtime = ContractRuntime::new().with_application_parameters(());
        let mut contract = CounterContract {
            state: CounterState::load(runtime.root_view_storage_context())
                .blocking_wait()
                .expect("Failed to read from mock key value store"),
            runtime,
        };

        contract
            .instantiate(initial_value)
            .now_or_never()
            .expect("Initialization of counter state should not await anything");

        assert_eq!(*contract.state.value.get(), initial_value);

        contract
    }
}
