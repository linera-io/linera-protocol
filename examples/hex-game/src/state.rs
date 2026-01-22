// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use async_graphql::SimpleObject;
use hex_game::{Board, Clock, Timeouts};
use linera_sdk::{
    linera_base_types::{AccountOwner, ChainId},
    views::{linera_views, SyncMapView, SyncRegisterView, SyncView, ViewStorageContext},
};
use serde::{Deserialize, Serialize};

/// The IDs of a temporary chain for a single game of Hex.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, SimpleObject)]
pub struct GameChain {
    /// The ID of the temporary game chain itself.
    pub chain_id: ChainId,
}

/// The application state.
#[derive(SyncView, SimpleObject)]
#[graphql(complex)]
#[view(context = ViewStorageContext)]
pub struct HexState {
    /// The `AccountOwner`s controlling players `One` and `Two`.
    pub owners: SyncRegisterView<Option<[AccountOwner; 2]>>,
    /// The current game state.
    pub board: SyncRegisterView<Board>,
    /// The game clock.
    pub clock: SyncRegisterView<Clock>,
    /// The timeouts.
    pub timeouts: SyncRegisterView<Timeouts>,
    /// Temporary chains for individual games, by player.
    pub game_chains: SyncMapView<AccountOwner, BTreeSet<GameChain>>,
}
