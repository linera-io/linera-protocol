// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::SimpleObject;
use hex_game::{Board, Clock};
use linera_sdk::{
    base::Owner,
    views::{linera_views, RegisterView, RootView, ViewStorageContext},
};

/// The application state.
#[derive(RootView, SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct HexState {
    /// The `Owner`s controlling players `One` and `Two`.
    pub owners: RegisterView<Option<[Owner; 2]>>,
    /// The current game state.
    pub board: RegisterView<Board>,
    /// The game clock.
    pub clock: RegisterView<Clock>,
}
