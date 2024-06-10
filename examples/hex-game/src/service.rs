// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::{Arc, Mutex};

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use hex_game::{Board, Operation, Player};
use linera_sdk::{
    base::{Owner, WithServiceAbi},
    graphql::GraphQLMutationRoot,
    views::{View, ViewStorageContext},
    Service, ServiceRuntime,
};

use self::state::HexState;

#[derive(Clone)]
pub struct HexService {
    runtime: Arc<Mutex<ServiceRuntime<HexService>>>,
    state: Arc<HexState>,
}

linera_sdk::service!(HexService);

impl WithServiceAbi for HexService {
    type Abi = hex_game::HexAbi;
}

impl Service for HexService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = HexState::load(ViewStorageContext::from(runtime.key_value_store()))
            .await
            .expect("Failed to load state");
        HexService {
            runtime: Arc::new(Mutex::new(runtime)),
            state: Arc::new(state),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema =
            Schema::build(self.clone(), Operation::mutation_root(), EmptySubscription).finish();
        schema.execute(request).await
    }
}

#[Object]
impl HexService {
    async fn winner(&self) -> Option<Player> {
        if let Some(winner) = self.state.board.get().winner() {
            return Some(winner);
        }
        let active = self.state.board.get().active_player();
        let block_time = self.runtime.lock().unwrap().system_time();
        if self.state.clock.get().timed_out(block_time, active) {
            return Some(active.other());
        }
        None
    }

    async fn owners(&self) -> [Owner; 2] {
        self.state.owners.get().unwrap()
    }

    async fn board(&self) -> &Board {
        self.state.board.get()
    }
}
