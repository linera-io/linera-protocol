// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use async_graphql::SimpleObject;
use hex_game::{Board, Clock, Timeouts};
use linera_sdk::{
    base::{ChainId, MessageId, Owner},
    views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext},
};
use serde::{Deserialize, Serialize};

/// The IDs of a temporary chain for a single game of Hex.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, SimpleObject)]
pub struct GameChain {
    /// The ID of the `OpenChain` message that created the chain.
    pub message_id: MessageId,
    /// The ID of the temporary game chain itself.
    pub chain_id: ChainId,
}

/// The application state.
#[derive(RootView, SimpleObject)]
#[graphql(complex)]
#[view(context = "ViewStorageContext")]
pub struct HexState {
    /// The `Owner`s controlling players `One` and `Two`.
    pub owners: RegisterView<Option<[Owner; 2]>>,
    /// The current game state.
    pub board: RegisterView<Board>,
    /// The game clock.
    pub clock: RegisterView<Clock>,
    /// The timeouts.
    pub timeouts: RegisterView<Timeouts>,
    /// Temporary chains for individual games, by player.
    pub game_chains: MapView<Owner, BTreeSet<GameChain>>,
}
