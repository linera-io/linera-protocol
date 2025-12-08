// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::{
    cmp::min,
    collections::{BTreeMap, BTreeSet},
};

use async_graphql::SimpleObject;
use linera_sdk::{
    linera_base_types::{Account, AccountOwner, Amount, ChainId},
    views::{linera_views, linera_views::context::Context as _},
    KeyValueStore,
};
use linera_views::views::{RootView, View};
use matching_engine::{
    OrderId, OrderNature, Parameters, PendingOrderInfo, Price, PriceAsk, PriceBid,
};
use serde::{Deserialize, Serialize};

pub type Context = linera_views::context::ViewContext<Parameters, KeyValueStore>;

pub type CustomCollectionView<K, V> =
    linera_views::collection_view::CustomCollectionView<Context, K, V>;

pub type MapView<K, V> = linera_views::map_view::MapView<Context, K, V>;

pub type QueueView<T> = linera_views::queue_view::QueueView<Context, T>;

pub type RegisterView<T> = linera_views::register_view::RegisterView<Context, T>;

/// The order entry in the order book
#[derive(Clone, Debug, Deserialize, Serialize, SimpleObject)]
pub struct OrderEntry {
    /// The number of token1 being bought or sold
    pub quantity: Amount,
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
/// each owner on the matching engine chain.
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
#[view(context = Context)]
pub struct LevelView {
    pub queue: QueueView<OrderEntry>,
}

/// The matching engine containing the information.
#[derive(RootView, SimpleObject)]
#[view(context = Context)]
pub struct MatchingEngineState {
    /// Pending orders tracked on user chains (not used on matching engine chain).
    /// Maps owner to a map of order ID to pending order details.
    pub pending_orders: MapView<AccountOwner, BTreeMap<OrderId, PendingOrderInfo>>,

    // -- Matching engine chain only --
    /// The next order_id to be used.
    pub next_order_id: RegisterView<OrderId>,
    /// The map of the outstanding bids, by the bitwise complement of
    /// the revert of the price. The order is from the best price
    /// level (highest proposed by buyer) to the worst
    pub bids: CustomCollectionView<PriceBid, LevelView>,
    /// The map of the outstanding asks, by the bitwise complement of
    /// the price. The order is from the best one (smallest asked price
    /// by seller) to the worst.
    pub asks: CustomCollectionView<PriceAsk, LevelView>,
    /// The map with the list of orders giving for each order_id the
    /// fundamental information on the order (price, nature, account)
    pub orders: MapView<OrderId, KeyBook>,
    /// The map giving for each account owner the set of order_id
    /// owned by that owner (used on the matching engine chain).
    pub account_info: MapView<AccountOwner, AccountInfo>,
}

impl MatchingEngineState {
    fn parameters(&self) -> Parameters {
        *self.context().extra()
    }

    /// Returns the [`LevelView`] for a specified ask `price`.
    pub async fn ask_level(&mut self, price: &PriceAsk) -> &mut LevelView {
        self.asks
            .load_entry_mut(price)
            .await
            .expect("Failed to load `LevelView` for an ask price")
    }

    /// Returns the [`LevelView`] for a specified bid `price`.
    pub async fn bid_level(&mut self, price: &PriceBid) -> &mut LevelView {
        self.bids
            .load_entry_mut(price)
            .await
            .expect("Failed to load `LevelView` for a bid price")
    }

    /// Checks that the order exists and has been issued by the claimed owner.
    pub async fn check_order_id(&self, order_id: &OrderId, owner: &AccountOwner) {
        let value = self
            .orders
            .get(order_id)
            .await
            .expect("Failed to load order");
        match value {
            None => panic!("Order is not present therefore cannot be cancelled"),
            Some(value) => {
                assert_eq!(
                    &value.account.owner, owner,
                    "The owner of the order is not matching with the owner put"
                );
            }
        }
    }

    /// Modifies the order from the order_id.
    /// This means that some transfers have to be done and the size depends
    /// whether ask or bid.
    pub async fn modify_order(
        &mut self,
        order_id: OrderId,
        new_quantity: Amount,
    ) -> Option<Transfer> {
        let key_book = self
            .orders
            .get(&order_id)
            .await
            .expect("Failed to load order")?;
        let transfer = match key_book.nature {
            OrderNature::Bid => {
                let view = self.bid_level(&key_book.price.to_bid()).await;
                let (cancel_quantity, remove_order_id) =
                    view.modify_order_level(order_id, new_quantity).await?;
                if remove_order_id {
                    self.remove_order_id((key_book.account.owner, order_id))
                        .await;
                }
                let cancel_amount = self
                    .parameters()
                    .product_price_amount(key_book.price, cancel_quantity);
                Transfer {
                    account: key_book.account,
                    amount: cancel_amount,
                    token_idx: 0,
                }
            }
            OrderNature::Ask => {
                let view = self.ask_level(&key_book.price.to_ask()).await;
                let (cancel_quantity, remove_order_id) =
                    view.modify_order_level(order_id, new_quantity).await?;
                if remove_order_id {
                    self.remove_order_id((key_book.account.owner, order_id))
                        .await;
                }
                Transfer {
                    account: key_book.account,
                    amount: cancel_quantity,
                    token_idx: 1,
                }
            }
        };
        Some(transfer)
    }

    /// Gets the order_id that increases starting from 0.
    fn get_new_order_id(&mut self) -> OrderId {
        let value = self.next_order_id.get_mut();
        let value_ret = *value;
        *value += 1;
        value_ret
    }

    /// Inserts the order_id and insert it into:
    /// * account_info which give the orders by owner
    /// * The orders which contain the symbolic information and the key_book.
    async fn insert_order(
        &mut self,
        account: Account,
        nature: OrderNature,
        order_id: OrderId,
        price: Price,
    ) {
        let account_info = self
            .account_info
            .get_mut_or_default(&account.owner)
            .await
            .expect("Failed to load account information");
        account_info.orders.insert(order_id);
        let key_book = KeyBook {
            price,
            nature,
            account,
        };
        self.orders
            .insert(&order_id, key_book)
            .expect("Failed to insert order");
    }

    /// Removes one single (owner, order_id) from the database
    /// * This is done for the info by owners
    /// * And the symbolic information of orders
    async fn remove_order_id(&mut self, entry: (AccountOwner, OrderId)) {
        let (owner, order_id) = entry;
        let account_info = self
            .account_info
            .get_mut(&owner)
            .await
            .expect("account_info")
            .unwrap();
        account_info.orders.remove(&order_id);
    }

    /// Removes a bunch of order_id
    async fn remove_order_ids(&mut self, entries: Vec<(AccountOwner, OrderId)>) {
        for entry in entries {
            self.remove_order_id(entry).await;
        }
    }

    /// Inserts an order into the matching engine and this creates several things:
    /// * The price levels that matches are selected
    /// * Getting from the best matching price to the least good the price levels
    ///   are cleared.
    /// * That clearing creates a number of transfer orders.
    /// * If after the level clearing the order is completely filled then it is not
    ///   inserted. Otherwise, it became a liquidity order in the matching engine
    ///
    /// Returns: `(transfers, order_id, remaining_amount, filled_orders)`
    /// where `filled_orders` contains `(chain_id, owner, order_id)` for each filled order
    pub async fn insert_and_uncross_market(
        &mut self,
        account: &Account,
        quantity: Amount,
        nature: OrderNature,
        price: &Price,
    ) -> (
        Vec<Transfer>,
        OrderId,
        Amount,
        Vec<(ChainId, AccountOwner, OrderId)>,
    ) {
        // Bids are ordered from the highest bid (most preferable) to the smallest bid.
        // Asks are ordered from the smallest (most preferable) to the highest.
        // The prices have custom serialization so that they are in increasing order.
        // To reverse the order of the bids, we take the bitwise complement of the price.
        let order_id = self.get_new_order_id();
        let mut final_quantity = quantity;
        let mut transfers = Vec::new();
        let mut filled_orders = Vec::new();
        match nature {
            OrderNature::Bid => {
                let mut matching_price_asks = Vec::new();
                self.asks
                    .for_each_index_while(|price_ask| {
                        let matches = price_ask.to_price() <= *price;
                        if matches {
                            matching_price_asks.push(price_ask);
                        }
                        Ok(matches)
                    })
                    .await
                    .expect("Failed to iterate over ask prices");
                for price_ask in matching_price_asks {
                    let view = self.ask_level(&price_ask).await;
                    let remove_entry = view
                        .level_clearing(
                            account,
                            &mut final_quantity,
                            &mut transfers,
                            &nature,
                            price_ask.to_price(),
                            *price,
                        )
                        .await;
                    if view.queue.count() == 0 {
                        self.asks
                            .remove_entry(&price_ask)
                            .expect("Failed to remove ask level");
                    }
                    // Collect filled orders with chain ID information
                    for (owner, order_id) in &remove_entry {
                        if let Some(key_book) = self
                            .orders
                            .get(order_id)
                            .await
                            .expect("Failed to load order")
                        {
                            filled_orders.push((key_book.account.chain_id, *owner, *order_id));
                        }
                    }
                    self.remove_order_ids(remove_entry).await;
                    if final_quantity == Amount::ZERO {
                        break;
                    }
                }
                if final_quantity != Amount::ZERO {
                    let view = self.bid_level(&price.to_bid()).await;
                    let order = OrderEntry {
                        quantity: final_quantity,
                        account: *account,
                        order_id,
                    };
                    view.queue.push_back(order);
                    self.insert_order(*account, OrderNature::Bid, order_id, *price)
                        .await;
                }
            }
            OrderNature::Ask => {
                let mut matching_price_bids = Vec::new();
                self.bids
                    .for_each_index_while(|price_bid| {
                        let matches = price_bid.to_price() >= *price;
                        if matches {
                            matching_price_bids.push(price_bid);
                        }
                        Ok(matches)
                    })
                    .await
                    .expect("Failed to iterate over bid prices");
                for price_bid in matching_price_bids {
                    let view = self.bid_level(&price_bid).await;
                    let remove_entry = view
                        .level_clearing(
                            account,
                            &mut final_quantity,
                            &mut transfers,
                            &nature,
                            price_bid.to_price(),
                            *price,
                        )
                        .await;
                    if view.queue.count() == 0 {
                        self.bids
                            .remove_entry(&price_bid)
                            .expect("Failed to remove bid level");
                    }
                    // Collect filled orders with chain ID information
                    for (owner, order_id) in &remove_entry {
                        if let Some(key_book) = self
                            .orders
                            .get(order_id)
                            .await
                            .expect("Failed to load order")
                        {
                            filled_orders.push((key_book.account.chain_id, *owner, *order_id));
                        }
                    }
                    self.remove_order_ids(remove_entry).await;
                    if final_quantity == Amount::ZERO {
                        break;
                    }
                }
                if final_quantity != Amount::ZERO {
                    let view = self.ask_level(&price.to_ask()).await;
                    let order = OrderEntry {
                        quantity: final_quantity,
                        account: *account,
                        order_id,
                    };
                    view.queue.push_back(order);
                    self.insert_order(*account, OrderNature::Ask, order_id, *price)
                        .await;
                }
            }
        }
        (transfers, order_id, final_quantity, filled_orders)
    }
}

/// Transfer operation back to the owners
#[derive(Clone)]
pub struct Transfer {
    /// Beneficiary of the transfer
    pub account: Account,
    /// Amount being transferred
    pub amount: Amount,
    /// Index of the token being transferred (0 or 1)
    pub token_idx: u32,
}

impl LevelView {
    fn parameters(&self) -> Parameters {
        *self.context().extra()
    }

    /// A price level is cleared starting from the oldest one till the
    /// new order is completely filled or there is no more liquidity
    /// providing order remaining to fill it.
    pub async fn level_clearing(
        &mut self,
        account: &Account,
        quantity: &mut Amount,
        transfers: &mut Vec<Transfer>,
        nature: &OrderNature,
        price_level: Price,
        price_insert: Price,
    ) -> Vec<(AccountOwner, OrderId)> {
        let parameters = self.parameters();
        let mut remove_order = Vec::new();
        let orders = self
            .queue
            .iter_mut()
            .await
            .expect("Failed to load iterator over orders");
        for order in orders {
            let fill = min(order.quantity, *quantity);
            quantity.try_sub_assign(fill).unwrap();
            order.quantity.try_sub_assign(fill).unwrap();
            if fill > Amount::ZERO {
                transfers.extend_from_slice(&Self::get_transfers(
                    &parameters,
                    nature,
                    fill,
                    account,
                    order,
                    price_level,
                    price_insert,
                ));
            }
            if order.quantity == Amount::ZERO {
                remove_order.push((order.account.owner, order.order_id));
            }
            if *quantity == Amount::ZERO {
                break;
            }
        }
        self.remove_zero_orders_from_level().await;
        remove_order
    }

    /// Creates the transfers corresponding to the order:
    ///
    /// * `nature` is the nature of the order in question.
    /// * `fill` is the amount that is being processed.
    /// * `account` is the account owning the new order being inserted.
    /// * `order_level` is the liquidity providing order.
    /// * `price_level` is the price of the existing order that provides liquidity.
    /// * `price_insert` is the price that of the newly added order.
    ///
    /// If the new order satisfies bid > best_ask or ask < best_bid
    /// then there is money on the table. There are three possible
    /// ways to handle this:
    ///
    /// * The delta gets to the owner of the matching engine.
    /// * The liquidity providing order gets the delta.
    /// * The liquidity eating order gets the delta.
    ///
    /// We choose the second scenario since the liquidity providing
    /// order is waiting and so deserves to be rewarded for the wait.
    fn get_transfers(
        parameters: &Parameters,
        nature: &OrderNature,
        fill: Amount,
        account: &Account,
        order_level: &OrderEntry,
        price_level: Price,  // the price that was present in the level
        price_insert: Price, // the price of the inserted order
    ) -> Vec<Transfer> {
        let mut transfers = Vec::new();
        match nature {
            OrderNature::Bid => {
                // The order offers to buy token1 at price price_insert
                // * When the old order was created fill of token1 were committed
                //   by the seller.
                // * When the new order is created price_insert * fill of token0
                //   were committed by the buyer.
                // The result is that
                // * price_insert * fill of token0 go to the seller (more than he expected)
                // * fill of token1 go to the buyer.
                assert!(price_insert >= price_level);
                let transfer_to_buyer = Transfer {
                    account: *account,
                    amount: fill,
                    token_idx: 1,
                };
                let fill0 = parameters.product_price_amount(price_insert, fill);
                let transfer_to_seller = Transfer {
                    account: order_level.account,
                    amount: fill0,
                    token_idx: 0,
                };
                transfers.push(transfer_to_buyer);
                transfers.push(transfer_to_seller);
            }
            OrderNature::Ask => {
                // The order offers to sell token1 at price price_insert
                // * When the old order was created, price_level * fill of token0
                //   had to be committed by the buyer.
                // * When the new order is created, fill of token1 have to
                //   be committed by the seller.
                // The result is that
                // * price_insert * fill have to be sent to the seller
                // * the buyer receives
                //   - fill of token1
                //   - (price_level - price_insert) fill of token0 (nice bonus)
                assert!(price_insert <= price_level);
                let fill0 = parameters.product_price_amount(price_insert, fill);
                let transfer_to_seller = Transfer {
                    account: *account,
                    amount: fill0,
                    token_idx: 0,
                };
                let transfer_to_buyer1 = Transfer {
                    account: order_level.account,
                    amount: fill,
                    token_idx: 1,
                };
                transfers.push(transfer_to_buyer1);
                transfers.push(transfer_to_seller);
                if price_level != price_insert {
                    let price_diff = Price {
                        price: price_level.price - price_insert.price,
                    };
                    let fill0 = parameters.product_price_amount(price_diff, fill);
                    let transfer_to_buyer0 = Transfer {
                        account: order_level.account,
                        amount: fill0,
                        token_idx: 0,
                    };
                    transfers.push(transfer_to_buyer0);
                }
            }
        }
        transfers
    }

    /// Orders which have length 0 should be removed from the system.
    /// It is possible that we have some zero orders in the QueueView
    /// under the condition that they are not the oldest.
    /// An order can be of size zero for two reasons:
    /// * It has been totally cancelled
    /// * It has been filled that is the owner got what they wanted.
    pub async fn remove_zero_orders_from_level(&mut self) {
        // If some order has amount zero but is after an order of non-zero amount, then it is left.
        let iter = self
            .queue
            .iter_mut()
            .await
            .expect("Failed to load iterator over level queue");
        let n_remove = iter
            .take_while(|order| order.quantity == Amount::ZERO)
            .count();
        for _ in 0..n_remove {
            self.queue.delete_front();
        }
    }

    /// For a specific level of price, looks at all the orders and finds the one that
    /// has this specific order_id.
    /// When that order is found, then the cancellation is applied to it.
    /// Then the information is emitted for the handling of this operation.
    pub async fn modify_order_level(
        &mut self,
        order_id: OrderId,
        new_quantity: Amount,
    ) -> Option<(Amount, bool)> {
        let mut iter = self
            .queue
            .iter_mut()
            .await
            .expect("Failed to load iterator over level queue");
        let state_order = iter.find(|order| order.order_id == order_id)?;
        let cancel_quantity = state_order
            .quantity
            .try_sub(new_quantity)
            .expect("Attempt to increase quantity");
        state_order.quantity = new_quantity;
        self.remove_zero_orders_from_level().await;
        Some((cancel_quantity, new_quantity == Amount::ZERO))
    }
}
