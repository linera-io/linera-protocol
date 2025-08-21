// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{ComplexObject, Context, EmptySubscription, Request, Response, Schema};
use gol_challenge::{
    game::{Board, Puzzle, ValidationResult},
    Operation,
};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{DataBlobHash, WithServiceAbi},
    views::View,
    Service, ServiceRuntime,
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
    /// Advance a board by one step using Conway's Game of Life rules.
    async fn advance_board_once(&self, board: Board) -> Board {
        board.advance_once()
    }

    /// Advance a board by multiple steps.
    async fn advance_board(&self, board: Board, steps: u16) -> Board {
        board.advance(steps)
    }

    /// Check if a board solves a puzzle.
    async fn validate_solution(
        &self,
        ctx: &Context<'_>,
        board: Board,
        puzzle_id: DataBlobHash,
        steps: u16,
    ) -> ValidationResult {
        let runtime = ctx
            .data::<Arc<ServiceRuntime<GolChallengeService>>>()
            .unwrap();
        let puzzle_bytes = runtime.read_data_blob(puzzle_id);
        let puzzle = bcs::from_bytes(&puzzle_bytes).expect("Failed to deserialize puzzle");

        match board.check_puzzle(&puzzle, steps) {
            Ok(()) => ValidationResult {
                is_valid: true,
                error_message: None,
                error_details: None,
            },
            Err(error) => ValidationResult {
                is_valid: false,
                error_message: Some(error.to_string()),
                error_details: Some(error),
            },
        }
    }

    /// Print the ASCII representation of a board.
    async fn print_board(&self, board: Board) -> String {
        format!("{}", board)
    }

    /// Print the pretty ASCII representation of a board with coordinates.
    async fn pretty_print_board(&self, board: Board) -> String {
        format!("{:#}", board)
    }

    /// Retrieve a puzzle by its ID.
    async fn puzzle(&self, ctx: &Context<'_>, puzzle_id: DataBlobHash) -> Option<Puzzle> {
        let runtime = ctx
            .data::<Arc<ServiceRuntime<GolChallengeService>>>()
            .unwrap();
        let puzzle_bytes = runtime.read_data_blob(puzzle_id);
        bcs::from_bytes::<Puzzle>(&puzzle_bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::{futures_util::FutureExt, Request};
    use linera_sdk::{
        linera_base_types::{BlobContent, CryptoHash},
        util::BlockingWait,
        views::View,
        Service, ServiceRuntime,
    };
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

    #[test]
    fn query_advance_board_multiple_steps() {
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
                    advanceBoard(board: {size: 3, liveCells: [ {x: 1, y: 1}, {x: 1, y: 0}, {x: 1, y: 2} ]}, steps: 2) {
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
            json!({
                "advanceBoard": {
                    "size": 3,
                    "liveCells": [
                        { "x": 1, "y": 0 },
                        { "x": 1, "y": 1 },
                        { "x": 1, "y": 2 }
                    ]
                }
            })
        );
    }

    #[test]
    fn query_validate_solution() {
        use gol_challenge::game::{Condition, Difficulty, Position, Puzzle};

        let runtime = ServiceRuntime::<GolChallengeService>::new();
        let state = GolChallengeState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = GolChallengeService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        // Create a simple puzzle: a single cell that dies after 1 step.
        let puzzle = Puzzle {
            title: "Single Cell Death".to_string(),
            summary: "A single cell should die after one step".to_string(),
            difficulty: Difficulty::Easy,
            size: 3,
            minimal_steps: 1,
            maximal_steps: 1,
            initial_conditions: vec![Condition::TestPosition {
                position: Position { x: 1, y: 1 },
                is_live: true,
            }],
            final_conditions: vec![Condition::TestPosition {
                position: Position { x: 1, y: 1 },
                is_live: false,
            }],
        };

        // Serialize the puzzle and store it as a data blob.
        let puzzle_bytes = bcs::to_bytes(&puzzle).expect("Failed to serialize puzzle");
        // Create a dummy DataBlobHash for testing.
        let puzzle_id = DataBlobHash(CryptoHash::new(&BlobContent::new_data(
            puzzle_bytes.clone(),
        )));
        service.runtime.set_blob(puzzle_id, puzzle_bytes);

        // Test with a valid solution.
        let response = service
            .handle_query(Request::new(format!(
                r#"{{
                    validateSolution(
                        board: {{size: 3, liveCells: [{{x: 1, y: 1}}]}},
                        puzzleId: "{}",
                        steps: 1
                    ) {{
                        isValid
                        errorMessage
                        errorDetails
                    }}
                }}"#,
                puzzle_id.0
            )))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        assert_eq!(
            response,
            json!({
                "validateSolution": {
                    "isValid": true,
                    "errorMessage": null,
                    "errorDetails": null
                }
            })
        );

        // Test with an invalid solution (wrong initial state)
        let response = service
            .handle_query(Request::new(format!(
                r#"{{
                    validateSolution(
                        board: {{size: 3, liveCells: [{{x: 0, y: 0}}]}},
                        puzzleId: "{}",
                        steps: 1
                    ) {{
                        isValid
                        errorMessage
                        errorDetails
                    }}
                }}"#,
                puzzle_id.0
            )))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        // Check that it's invalid and has an error message.
        let result = &response["validateSolution"];
        assert_eq!(result["isValid"], false);
        assert!(result["errorMessage"]
            .as_str()
            .unwrap()
            .contains("Initial condition 0 failed"));
        assert!(result["errorDetails"].is_object());

        // Test with invalid steps (too many).
        let response = service
            .handle_query(Request::new(format!(
                r#"{{
                    validateSolution(
                        board: {{size: 3, liveCells: [{{x: 1, y: 1}}]}},
                        puzzleId: "{}",
                        steps: 2
                    ) {{
                        isValid
                        errorMessage
                        errorDetails
                    }}
                }}"#,
                puzzle_id.0
            )))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        // Check that it's invalid and has an error message about steps.
        let result = &response["validateSolution"];
        assert_eq!(result["isValid"], false);
        assert!(result["errorMessage"]
            .as_str()
            .unwrap()
            .contains("Steps 2 is outside the allowed range"));
        assert!(result["errorDetails"].is_object());
    }

    #[test]
    fn query_print_board() {
        let runtime = ServiceRuntime::<GolChallengeService>::new();
        let state = GolChallengeState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = GolChallengeService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        // Test printing a board with a simple pattern.
        let response = service
            .handle_query(Request::new(
                "{
                    printBoard(board: {size: 3, liveCells: [ {x: 1, y: 0}, {x: 1, y: 1}, {x: 1, y: 2} ]})
                }",
            ))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        // The response should contain a string representation of the board.
        // This will be the compact format (without line numbers).
        let expected_output = "·●·\n·●·\n·●·\n";
        assert_eq!(response, json!({ "printBoard": expected_output }));

        // Test with an empty board.
        let response = service
            .handle_query(Request::new(
                "{
                    printBoard(board: {size: 2, liveCells: []})
                }",
            ))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        let expected_output = "··\n··\n";
        assert_eq!(response, json!({ "printBoard": expected_output }));
    }

    #[test]
    fn query_pretty_print_board() {
        let runtime = ServiceRuntime::<GolChallengeService>::new();
        let state = GolChallengeState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = GolChallengeService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        // Test pretty printing a board with a simple pattern.
        let response = service
            .handle_query(Request::new(
                "{
                    prettyPrintBoard(board: {size: 3, liveCells: [ {x: 1, y: 0}, {x: 1, y: 1}, {x: 1, y: 2} ]})
                }",
            ))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        // The response should contain the pretty string representation with coordinates.
        let expected_output = "    0 1 2\n 0  · ● ·\n 1  · ● ·\n 2  · ● ·\n";
        assert_eq!(response, json!({ "prettyPrintBoard": expected_output }));
    }

    #[test]
    fn query_puzzle() {
        use gol_challenge::game::{Condition, Difficulty, Position, Puzzle};

        let runtime = ServiceRuntime::<GolChallengeService>::new();
        let state = GolChallengeState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = GolChallengeService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        // Create a test puzzle.
        let puzzle = Puzzle {
            title: "Test Puzzle".to_string(),
            summary: "A simple test puzzle".to_string(),
            difficulty: Difficulty::Easy,
            size: 3,
            minimal_steps: 1,
            maximal_steps: 2,
            initial_conditions: vec![Condition::TestPosition {
                position: Position { x: 1, y: 1 },
                is_live: true,
            }],
            final_conditions: vec![Condition::TestPosition {
                position: Position { x: 1, y: 1 },
                is_live: false,
            }],
        };

        // Serialize the puzzle and store it as a data blob.
        let puzzle_bytes = bcs::to_bytes(&puzzle).expect("Failed to serialize puzzle");
        let puzzle_id = DataBlobHash(CryptoHash::new(&BlobContent::new_data(
            puzzle_bytes.clone(),
        )));
        service.runtime.set_blob(puzzle_id, puzzle_bytes);

        // Test retrieving the puzzle.
        let response = service
            .handle_query(Request::new(format!(
                r#"{{
                    puzzle(puzzleId: "{}") {{
                        title
                        summary
                        difficulty
                        size
                        minimalSteps
                        maximalSteps
                        initialConditions
                        finalConditions
                    }}
                }}"#,
                puzzle_id.0
            )))
            .now_or_never()
            .expect("Query should not await anything")
            .data
            .into_json()
            .expect("Response should be JSON");

        // The response should contain the puzzle object with all fields.
        assert_eq!(
            response,
            json!({
                "puzzle": {
                    "title": "Test Puzzle",
                    "summary": "A simple test puzzle",
                    "difficulty": "EASY",
                    "size": 3,
                    "minimalSteps": 1,
                    "maximalSteps": 2,
                    "initialConditions": [
                        {
                            "TestPosition": {
                                "is_live": true,
                                "position": {
                                    "x": 1,
                                    "y": 1
                                }
                            }
                        }
                    ],
                    "finalConditions": [
                        {
                            "TestPosition": {
                                "is_live": false,
                                "position": {
                                    "x": 1,
                                    "y": 1
                                }
                            }
                        }
                    ]
                }
            })
        );
    }
}
