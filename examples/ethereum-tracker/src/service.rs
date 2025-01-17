// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use alloy::primitives::U256;
use async_graphql::{EmptySubscription, Request, Response, Schema};
use ethereum_tracker::Operation;
use linera_sdk::{
    base::WithServiceAbi,
    ethereum::{EthereumDataType, EthereumEvent, EthereumQueries, ServiceEthereumClient},
    graphql::GraphQLMutationRoot,
    views::View,
    Service, ServiceRuntime,
};
use serde::{Deserialize, Serialize};

use self::state::EthereumTrackerState;

#[derive(Clone, async_graphql::SimpleObject)]
pub struct EthereumTrackerService {
    #[graphql(flatten)]
    state: Arc<EthereumTrackerState>,
    #[graphql(skip)]
    runtime: Arc<ServiceRuntime<Self>>,
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
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            Query {
                service: self.clone(),
            },
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

/// The service handler for GraphQL queries.
#[derive(async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Query {
    #[graphql(flatten)]
    service: EthereumTrackerService,
}

#[async_graphql::ComplexObject]
impl Query {
    /// Reads the initial Ethereum event emitted by the monitored contract.
    async fn read_initial_event(&self) -> InitialEvent {
        let start_block = *self.service.state.start_block.get();
        let mut events = self
            .read_events("Initial(address,uint256)", start_block, start_block + 1)
            .await;

        assert_eq!(events.len(), 1);
        let mut event_values = events.pop().unwrap().values.into_iter();

        let address_value = event_values.next().expect("Missing initial address value");
        let balance_value = event_values.next().expect("Missing initial balance value");

        let EthereumDataType::Address(address) = address_value else {
            panic!("wrong type for the first entry");
        };
        let EthereumDataType::Uint256(balance) = balance_value else {
            panic!("wrong type for the second entry");
        };

        InitialEvent { address, balance }
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
        let url = self.service.state.ethereum_endpoint.get().clone();
        let contract_address = self.service.state.contract_address.get().clone();
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
