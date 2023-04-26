// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::SimpleObject;
use fungible::AccountOwner;
use linera_sdk::{
    base::{Amount, ArithmeticError},
    views::{CustomCollectionView, MapView, QueueView, RegisterView, ViewStorageContext},
};
use linera_views::views::{RootView, View, ViewError};
use matching_engine::{OrderId, OrderNature, Price};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::collections::BTreeSet;

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum MatchingEngineError {
    /// Invalid query.
    #[error("Invalid query")]
    InvalidQuery(#[from] serde_json::Error),

    /// Action can only be executed on the chain that created the matching engine.
    #[error("Action can only be executed on the chain that created the matching engine")]
    MatchingEngineChainOnly,

    /// Too large modify order
    #[error("Too large modify order")]
    TooLargeModifyOrder,

    /// Order is not present therefore cannot be cancelled
    #[error("Order is not present therefore cannot be cancelled")]
    OrderNotPresent,

    /// Owner of order is incorrect
    #[error("The owner of the order is not matching with the owner put")]
    WrongOwnerOfOrder,

    /// Matching-Engine application doesn't support any cross-application sessions.
    #[error("Matching-Engine application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),

    #[error(transparent)]
    ViewError(#[from] ViewError),

    #[error(transparent)]
    BcsError(#[from] bcs::Error),
}

use linera_views::views::GraphQLView;

/// The order entry in the order book
#[derive(Clone, Debug, Deserialize, Serialize, SimpleObject)]
pub struct OrderEntry {
    /// The number of token1 being bought or sold
    pub amount: Amount,
    /// The one who has created the order
    pub owner: AccountOwner,
    /// The order_id (needed for possible cancel or modification)
    pub order_id: OrderId,
}

/// Transfer operation back to the owners
#[derive(Clone)]
pub struct Transfer {
    /// Beneficiary of the transfer
    pub owner: AccountOwner,
    /// Amount being transferred
    pub amount: Amount,
    /// Index of the token being transferred (0 or 1)
    pub token_idx: u32,
}

/// This is the entry present in the state so that we can access
/// information from the order_id.
#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject)]
pub struct KeyBook {
    /// The corresponding price
    pub price: Price,
    /// The nature of the order
    pub nature: OrderNature,
    /// The owner used for checks
    pub owner: AccountOwner,
}

/// The AccountInfo used for storing which order_id are owned by
/// each owner.
#[derive(Default, Clone, Debug, Serialize, Deserialize, SimpleObject)]
pub struct AccountInfo {
    /// The list of orders
    pub orders: BTreeSet<OrderId>,
}

/// The price level is contained in a QueueView
/// The queue starts with the oldest order to the newest.
/// When an order is cancelled it is zero. But if that
/// cancelled order is not the oldest, then it remains
/// though with a size zero.
#[derive(View, GraphQLView)]
#[view(context = "ViewStorageContext")]
pub struct LevelView {
    pub queue: QueueView<OrderEntry>,
}

/// The matching engine containing the information.
/// Meaning of the entries:
/// * The next_order_number contains the order_id so that
///   the order_id gets created from 0, to infinity.
/// * bids containing the bids starting from the best one
///   (highest proposed price by buyer) to the worse
/// * asks containing the asks starting from the best one
///   (smallest asked price by seller) to the worse.
/// * orders containing the map from the order_id to the
///   key information
/// * The account_info containing the list of order_id
///   by the owner.
#[derive(RootView, GraphQLView)]
#[view(context = "ViewStorageContext")]
pub struct MatchingEngine {
    /// The lowest order number that has not been used yet.
    pub next_order_number: RegisterView<OrderId>,
    /// The map of the outstanding bids, by the bitwise complement of the price
    pub bids: CustomCollectionView<Price, LevelView>,
    /// The map of the outstanding asks, by asking price
    pub asks: CustomCollectionView<Price, LevelView>,
    /// The map with the list of orders
    pub orders: MapView<OrderId, KeyBook>,
    /// The map with the information on the account owner
    pub account_info: MapView<AccountOwner, AccountInfo>,
}
