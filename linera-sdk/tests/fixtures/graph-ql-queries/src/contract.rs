// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use graph_ql_queries::{GraphQlQueriesAbi, GraphQlQueriesOperation};
use graph_ql_queries::GraphQlQueriesOperation::*;
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::GraphQlQueriesState;

pub struct GraphQlQueriesContract {
    state: GraphQlQueriesState,
}

linera_sdk::contract!(GraphQlQueriesContract);

impl WithContractAbi for GraphQlQueriesContract {
    type Abi = GraphQlQueriesAbi;
}

impl Contract for GraphQlQueriesContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = GraphQlQueriesState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        GraphQlQueriesContract { state }
    }

    async fn instantiate(&mut self, _value: ()) {
    }

    async fn execute_operation(&mut self, operation: GraphQlQueriesOperation) {
        match operation {
            InsertField4 { key1, key2, value } => {
                let subview = self.state.coll_map.load_entry_mut(&key1).await.unwrap();
                subview.insert(&key2, value).unwrap();
            },
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
