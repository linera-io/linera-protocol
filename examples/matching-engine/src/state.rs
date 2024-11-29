// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use async_graphql::SimpleObject;
use fungible::Account;
use linera_sdk::{
    base::{AccountOwner, Amount},
    views::{
        linera_views, CustomCollectionView, MapView, QueueView, RegisterView, RootView, View,
        ViewStorageContext,
    },
};
use matching_engine::{OrderId, OrderNature, Price, PriceAsk, PriceBid};
use serde::{Deserialize, Serialize};

/// The order entry in the order book
#[derive(Clone, Debug, Deserialize, Serialize, SimpleObject)]
pub struct OrderEntry {
    /// The number of token1 being bought or sold
    pub amount: Amount,
    /// The one who has created the order
    pub account: Account,
    /// The order_id (needed for possible cancel or modification)
    pub order_id: OrderId,
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
    pub account: Account,
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
#[derive(View, SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct LevelView {
    pub queue: QueueView<OrderEntry>,
}

/// The matching engine containing the information.
#[derive(RootView, SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct MatchingEngineState {
    ///The next_order_number contains the order_id so that
    ///the order_id gets created from 0, to infinity.
    pub next_order_number: RegisterView<OrderId>,
    /// The map of the outstanding bids, by the bitwise complement of
    /// the revert of the price. The order is from the best price
    /// level (highest proposed by buyer) to the worse
    pub bids: CustomCollectionView<PriceBid, LevelView>,
    /// The map of the outstanding asks, by the bitwise complement of
    /// the price. The order is from the best one (smallest asked price
    /// by seller) to the worse.
    pub asks: CustomCollectionView<PriceAsk, LevelView>,
    /// The map with the list of orders giving for each order_id the
    /// fundamental information on the order (nature, owner, amount)
    pub orders: MapView<OrderId, KeyBook>,
    /// The map giving for each account owner the set of order_id
    /// owned by that owner.
    pub account_info: MapView<AccountOwner, AccountInfo>,
}
