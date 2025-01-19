// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use alloy::primitives::U256;
use async_graphql::{EmptySubscription, Request, Response, Schema};
use ethereum_tracker::{Operation, U256Cont};
use linera_sdk::{
    base::WithServiceAbi,
    ethereum::{EthereumDataType, EthereumEvent, EthereumQueries, ServiceEthereumClient},
    graphql::GraphQLMutationRoot,
    views::{MapView, View},
    Service, ServiceRuntime,
};
use serde::{Deserialize, Serialize};

use self::state::EthereumTrackerState;

#[derive(Clone)]
pub struct EthereumTrackerService {
    state: Arc<EthereumTrackerState>,
}

linera_sdk::service!(EthereumTrackerService);

impl WithServiceAbi for EthereumTrackerService {
    type Abi = ethereum_tracker::EthereumTrackerAbi;
}

impl Service for EthereumTrackerService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = EthereumTrackerState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EthereumTrackerService {
            state: Arc::new(state),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            Query(self.clone()),
            Operation::mutation_root(),
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

/// The service handler for GraphQL queries.
pub struct Query(EthereumTrackerService);

#[async_graphql::Object]
impl Query {
    /// Returns the Ethereum endpoint the service is using.
    async fn ethereum_endpoint(&self) -> String {
        self.0.state.ethereum_endpoint.get().clone()
    }

    /// Returns the Ethereum contract address that the application is monitoring.
    async fn contract_address(&self) -> String {
        self.0.state.contract_address.get().clone()
    }

    /// Returns the Ethereum block height where monitoring started.
    async fn start_block(&self) -> u64 {
        *self.0.state.start_block.get()
    }

    /// Returns the map of known Ethereum addresses that have used the contract and their balances.
    async fn accounts(&self) -> &MapView<String, U256Cont> {
        &self.0.state.accounts
    }

    /// Reads the initial Ethereum event emitted by the monitored contract.
    async fn read_initial_event(&self) -> InitialEvent {
        let start_block = *self.0.state.start_block.get();
        let events = self
            .read_events("Initial(address,uint256)", start_block, start_block + 1)
            .await;

        assert_eq!(events.len(), 1);
        let event = &events[0];
        let EthereumDataType::Address(address) = event.values[0].clone() else {
            panic!("wrong type for the first entry");
        };
        let EthereumDataType::Uint256(value) = event.values[1] else {
            panic!("wrong type for the second entry");
        };

        InitialEvent {
            address,
            balance: value,
        }
    }

    /// Reads the transfer events emitted by the monitored Ethereum contract.
    async fn read_transfer_events(&self, end_block: u64) -> Vec<TransferEvent> {
        let start_block = *self.0.state.start_block.get();
        let events = self
            .read_events(
                "Transfer(address indexed,address indexed,uint256)",
                start_block,
                end_block,
            )
            .await;

        events
            .into_iter()
            .map(|event| {
                let EthereumDataType::Address(source) = event.values[0].clone() else {
                    panic!("wrong type for the first entry");
                };
                let EthereumDataType::Address(destination) = event.values[1].clone() else {
                    panic!("wrong type for the second entry");
                };
                let EthereumDataType::Uint256(value) = event.values[2] else {
                    panic!("wrong type for the third entry");
                };

                TransferEvent {
                    source,
                    value,
                    destination,
                }
            })
            .collect()
    }
}

impl Query {
    /// Reads events of type `event_name` from the monitored contract emitted during the requested
    /// block height range.
    async fn read_events(
        &self,
        event_name: &str,
        start_block: u64,
        end_block: u64,
    ) -> Vec<EthereumEvent> {
        let url = self.0.state.ethereum_endpoint.get().clone();
        let contract_address = self.0.state.contract_address.get().clone();
        let ethereum_client = ServiceEthereumClient { url };

        ethereum_client
            .read_events(&contract_address, event_name, start_block, end_block)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "Failed to read Ethereum events for {contract_address} \
                    from block {start_block} to block {end_block}: {error}"
                )
            })
    }
}

/// The initial event emitted by the contract.
#[derive(Clone, Default, Deserialize, Serialize)]
pub struct InitialEvent {
    address: String,
    balance: U256,
}
async_graphql::scalar!(InitialEvent);

/// The transfer events emitted by the contract.
#[derive(Clone, Default, Deserialize, Serialize)]
pub struct TransferEvent {
    source: String,
    value: U256,
    destination: String,
}
async_graphql::scalar!(TransferEvent);
