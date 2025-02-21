// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use alloy::primitives::U256;
use ethereum_tracker::{EthereumTrackerAbi, InstantiationArgument};
use linera_sdk::{
    base::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::EthereumTrackerState;

pub struct EthereumTrackerContract {
    state: EthereumTrackerState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(EthereumTrackerContract);

impl WithContractAbi for EthereumTrackerContract {
    type Abi = EthereumTrackerAbi;
}

impl Contract for EthereumTrackerContract {
    type Message = ();
    type InstantiationArgument = InstantiationArgument;
    type Parameters = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = EthereumTrackerState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EthereumTrackerContract { state, runtime }
    }

    async fn instantiate(&mut self, argument: InstantiationArgument) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
        let InstantiationArgument {
            ethereum_endpoint,
            contract_address,
            start_block,
        } = argument;

        self.state.ethereum_endpoint.set(ethereum_endpoint);
        self.state.contract_address.set(contract_address);
        self.state.start_block.set(start_block);
        self.state
            .save()
            .await
            .expect("Failed to write updated storage");

        self.read_initial().await;
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        // The only input is updating the database
        match operation {
            Self::Operation::Update { to_block } => self.update(to_block).await,
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Messages not supported");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl EthereumTrackerContract {
    /// Reads the initial event emitted by the Ethereum contract, with the initial account and its
    /// balance.
    async fn read_initial(&mut self) {
        let request = async_graphql::Request::new("query { readInitialEvent }");

        let application_id = self.runtime.application_id();
        let response = self.runtime.query_service(application_id, request);

        let async_graphql::Value::Object(data_object) = response.data else {
            panic!("Unexpected response from `readInitialEvent`: {response:#?}");
        };
        let async_graphql::Value::Object(ref initial_event) = data_object["readInitialEvent"]
        else {
            panic!("Unexpected response data from `readInitialEvent`: {data_object:#?}");
        };
        let async_graphql::Value::String(ref address) = initial_event["address"] else {
            panic!("Unexpected address in initial event: {initial_event:#?}");
        };
        let async_graphql::Value::String(ref balance_string) = initial_event["balance"] else {
            panic!("Unexpected balance in initial event: {initial_event:#?}");
        };

        let balance = balance_string
            .parse::<U256>()
            .expect("Balance could not be parsed");

        self.state
            .accounts
            .insert(address, balance.into())
            .expect("Failed to insert initial balance");
    }

    /// Updates the accounts based on the transfer events emitted up to the `end_block`.
    async fn update(&mut self, end_block: u64) {
        let request = async_graphql::Request::new(format!(
            r#"query {{ readTransferEvents(endBlock: {end_block}) }}"#
        ));

        let application_id = self.runtime.application_id();
        let response = self.runtime.query_service(application_id, request);

        let async_graphql::Value::Object(data_object) = response.data else {
            panic!("Unexpected response from `readTransferEvents`: {response:#?}");
        };
        let async_graphql::Value::List(ref events) = data_object["readTransferEvents"] else {
            panic!("Unexpected response data from `readTransferEvents`: {data_object:#?}");
        };

        for event_value in events {
            let async_graphql::Value::Object(event) = event_value else {
                panic!("Unexpected event returned from `readTransferEvents`: {event_value:#?}");
            };

            let async_graphql::Value::String(ref source) = event["source"] else {
                panic!("Unexpected source address in transfer event: {event:#?}");
            };
            let async_graphql::Value::String(ref destination) = event["destination"] else {
                panic!("Unexpected destination address in transfer event: {event:#?}");
            };
            let async_graphql::Value::String(ref value_string) = event["value"] else {
                panic!("Unexpected balance in transfer event: {event:#?}");
            };

            let value = value_string
                .parse::<U256>()
                .expect("Balance could not be parsed");

            {
                let source_balance = self
                    .state
                    .accounts
                    .get_mut_or_default(source)
                    .await
                    .expect("Failed to read account balance for source address");
                source_balance.value -= value;
            }
            {
                let destination_balance = self
                    .state
                    .accounts
                    .get_mut_or_default(destination)
                    .await
                    .expect("Failed to read account balance for destination address");
                destination_balance.value += value;
            }
        }

        self.state.start_block.set(end_block);
    }
}
