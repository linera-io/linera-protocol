// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{ComplexObject, EmptySubscription, Request, Response, Schema};
use gol_challenge::{game::Board, Operation};
use linera_sdk::{
    graphql::GraphQLMutationRoot, linera_base_types::WithServiceAbi, views::View, Service,
    ServiceRuntime,
};

use self::state::GolChallengeState;

#[derive(Clone)]
pub struct GolChallengeService {
    runtime: Arc<ServiceRuntime<GolChallengeService>>,
    state: Arc<GolChallengeState>,
}

linera_sdk::service!(GolChallengeService);

impl WithServiceAbi for GolChallengeService {
    type Abi = gol_challenge::GolChallengeAbi;
}

impl Service for GolChallengeService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = GolChallengeState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        GolChallengeService {
            runtime: Arc::new(runtime),
            state: Arc::new(state),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.state.clone(),
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .data(self.runtime.clone())
        .finish();
        schema.execute(request).await
    }
}

#[ComplexObject]
impl GolChallengeState {
    async fn advance_board_once(&self, board: Board) -> Board {
        board.advance_once()
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{futures_util::FutureExt, Request};
    use linera_sdk::{util::BlockingWait, views::View, Service, ServiceRuntime};
    use serde_json::json;

    use super::*;

    #[test]
    fn query_advance_board_once() {
        let runtime = ServiceRuntime::<GolChallengeService>::new();
        let state = GolChallengeState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = GolChallengeService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        let response = service
            .handle_query(Request::new(
                "{
advanceBoardOnce(board: {size: 3, liveCells: [ {x: 1, y: 1}, {x: 1, y: 0}, {x: 1, y: 2} ]}) {
    size
    liveCells
}
}",
            ))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        assert_eq!(
            response,
            json!(
                { "advanceBoardOnce": {
                    "size": 3,
                    "liveCells": [
                        {
                            "x": 0,
                            "y": 1
                        },
                        {
                            "x": 1,
                            "y": 1
                        },
                        {
                            "x": 2,
                            "y": 1
                        }
                    ]
                }}
            )
        );
    }
}
