// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! Main library for the Game-of-Life challenge. Should compile for Wasm and Rust. */

#![deny(missing_docs)]

/// Core library for the game engine and puzzles.
pub mod game;

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ContractAbi, DataBlobHash, ServiceAbi},
};
use serde::{Deserialize, Serialize};

use crate::game::Board;

/// The ABI of the Game-of-Life challenge.
pub struct GolChallengeAbi;

/// Type for on-chain operations of the Game-of-Life challenge.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Submit a solution for verification.
    SubmitSolution {
        /// The ID of the puzzle in blob storage.
        puzzle_id: DataBlobHash,
        /// The board of the solution.
        board: Board,
        /// The number of steps of the solution.
        steps: u16,
    },
}

impl ContractAbi for GolChallengeAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for GolChallengeAbi {
    type Query = Request;
    type QueryResponse = Response;
}
