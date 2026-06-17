// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{ComplexObject, Context, EmptySubscription, Request, Response, Schema};
use hex_game::{Operation, Player};
use linera_sdk::{
    graphql::GraphQLMutationRoot, linera_base_types::WithServiceAbi, views::View, Service,
    ServiceRuntime,
};

use self::state::HexState;

#[derive(Clone)]
pub struct HexService {
    runtime: Arc<ServiceRuntime<HexService>>,
    state: Arc<HexState>,
}

linera_sdk::service!(HexService);

impl WithServiceAbi for HexService {
    type Abi = hex_game::HexAbi;
}

impl Service for HexService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = HexState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        HexService {
            runtime: Arc::new(runtime),
            state: Arc::new(state),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        self.schema().execute(request).await
    }
}

impl HexService {
    /// Builds the GraphQL schema served by [`Self::handle_query`]. Extracted so that the
    /// SDL can be snapshotted (see the `schema_sdl` test) without drifting from what the
    /// service actually serves.
    fn schema(
        &self,
    ) -> Schema<
        Arc<HexState>,
        <Operation as GraphQLMutationRoot<HexService>>::MutationRoot,
        EmptySubscription,
    > {
        Schema::build(
            self.state.clone(),
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .data(self.runtime.clone())
        .finish()
    }
}

#[ComplexObject]
impl HexState {
    async fn winner(&self, ctx: &Context<'_>) -> Option<Player> {
        if let Some(winner) = self.board.get().winner() {
            return Some(winner);
        }
        let active = self.board.get().active_player();
        let runtime = ctx.data::<Arc<ServiceRuntime<HexService>>>().unwrap();
        let block_time = runtime.system_time();
        if self.clock.get().timed_out(block_time, active) {
            return Some(active.other());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{futures_util::FutureExt, Request};
    use linera_sdk::{util::BlockingWait, views::View, Service, ServiceRuntime};
    use serde_json::json;

    use super::*;

    #[test]
    fn query() {
        let runtime = ServiceRuntime::<HexService>::new();
        let state = HexState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = HexService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        let response = service
            .handle_query(Request::new("{ clock { increment } }"))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        assert_eq!(response, json!({"clock" : {"increment": 0}}))
    }

    #[test]
    fn schema_sdl() {
        let runtime = ServiceRuntime::<HexService>::new();
        let state = HexState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = HexService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        insta::assert_snapshot!(service.schema().sdl());
    }
}
