// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{InputObject, SimpleObject};
use gol_challenge::game::Board;
use linera_sdk::{
    linera_base_types::{DataBlobHash, Timestamp},
    views::{linera_views, MapView, RootView, ViewStorageContext},
};
use serde::{Deserialize, Serialize};

/// The application state.
#[derive(RootView, SimpleObject)]
#[graphql(complex)]
#[view(context = ViewStorageContext)]
pub struct GolChallengeState {
    pub solutions: MapView<DataBlobHash, Solution>,
}

/// A verified solution to a GoL puzzle.
#[derive(Debug, Clone, Serialize, Deserialize, InputObject, SimpleObject)]
pub struct Solution {
    /// The ID of the puzzle.
    pub puzzle_id: DataBlobHash,
    /// The initial state of the board solving the puzzle.
    pub board: Board,
    /// The number of steps.
    pub steps: u16,
    /// Timestamp of the submission.
    pub timestamp: Timestamp,
}
