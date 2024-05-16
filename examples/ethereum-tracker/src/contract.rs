// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use ethereum_tracker::{EthereumTrackerAbi, InstantiationArgument, U256Cont};
use linera_ethereum::client::EthereumQueries as _;
use linera_sdk::{
    base::WithContractAbi,
    ethereum::{EthereumClient, EthereumDataType},
    views::{RootView, View, ViewStorageContext},
    Contract, ContractRuntime,
};

use self::state::EthereumTracker;

pub struct EthereumTrackerContract {
    state: EthereumTracker,
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
        let state = EthereumTracker::load(ViewStorageContext::from(runtime.key_value_store()))
            .await
            .expect("Failed to load state");
        EthereumTrackerContract { state, runtime }
    }

    async fn instantiate(&mut self, argument: InstantiationArgument) {
        // Validate that the application parameters were configured correctly.
        let _ = self.runtime.application_parameters();
        self.state.argument.set(argument);
        self.state.last_block.set(0);
        self.read_initial().await;
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        // The only input is updating the database
        match operation {
            Self::Operation::Update => self.update().await,
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
    fn get_endpoints(&self) -> (EthereumClient, String) {
        let argument = self.state.argument.get();
        let url = argument.ethereum_endpoint.clone();
        let contract_address = argument.contract_address.clone();
        let ethereum_client = EthereumClient { url };
        (ethereum_client, contract_address)
    }

    async fn read_initial(&mut self) {
        let event_name_expanded = "Initial(address,uint256)";
        let (ethereum_client, contract_address) = self.get_endpoints();
        let events = ethereum_client
            .read_events(&contract_address, event_name_expanded, 0)
            .await
            .expect("Read the Initial event");
        assert_eq!(events.len(), 1);
        let event = events[0].clone();
        let EthereumDataType::Address(address) = event.values[0].clone() else {
            panic!("wrong type for the first entry");
        };
        let EthereumDataType::Uint256(value) = event.values[1] else {
            panic!("wrong type for the second entry");
        };
        let value = U256Cont { value };
        self.state.accounts.insert(&address, value).unwrap();
    }

    async fn update(&mut self) {
        let event_name_expanded = "Transfer(address indexed,address indexed,uint256)";
        let (ethereum_client, contract_address) = self.get_endpoints();
        let start_block = self.state.last_block.get_mut();
        let events = ethereum_client
            .read_events(&contract_address, event_name_expanded, *start_block)
            .await
            .expect("Read a transfer event");
        for event in events {
            let EthereumDataType::Address(from) = event.values[0].clone() else {
                panic!("wrong type for the first entry");
            };
            let EthereumDataType::Address(to) = event.values[1].clone() else {
                panic!("wrong type for the second entry");
            };
            let EthereumDataType::Uint256(value) = event.values[2] else {
                panic!("wrong type for the third entry");
            };
            {
                let value_from = self.state.accounts.get_mut_or_default(&from).await.unwrap();
                value_from.value -= value;
            }
            {
                let value_to = self.state.accounts.get_mut_or_default(&to).await.unwrap();
                value_to.value += value;
            }
        }
    }
}
