// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use ethereum_tracker::{Operation, U256Cont};
use linera_sdk::{
    base::WithServiceAbi,
    graphql::GraphQLMutationRoot,
    views::{MapView, View},
    Service, ServiceRuntime,
};

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
}
