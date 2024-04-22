// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;
use std::cmp::min;

use async_trait::async_trait;
use fungible::{Account, FungibleTokenAbi};
use linera_sdk::{
    base::{AccountOwner, Amount, ApplicationId, ChainId, WithContractAbi},
    ensure, Contract, ContractRuntime, ViewStateStorage,
};
use matching_engine::{
    product_price_amount, MatchingEngineAbi, Message, Operation, Order, OrderId, OrderNature,
    Parameters, Price,
};
use state::{LevelView, MatchingEngine, MatchingEngineError};

use crate::state::{KeyBook, OrderEntry};

pub struct MatchingEngineContract {
    state: MatchingEngine,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(MatchingEngineContract);

impl WithContractAbi for MatchingEngineContract {
    type Abi = MatchingEngineAbi;
}

/// An order can be cancelled which removes it totally or
/// modified which is a partial cancellation. The size of the
/// order can never increase.
#[derive(Clone, Debug)]
enum ModifyAmount {
    All,
    Partial(Amount),
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

#[async_trait]
impl Contract for MatchingEngineContract {
    type Error = MatchingEngineError;
    type Storage = ViewStateStorage<Self>;
    type State = MatchingEngine;
    type Message = Message;
    type InitializationArgument = ();
    type Parameters = Parameters;

    async fn new(
        state: MatchingEngine,
        runtime: ContractRuntime<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(MatchingEngineContract { state, runtime })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn instantiate(&mut self, _argument: ()) -> Result<(), Self::Error> {
        // Validate that the application parameters were configured correctly.
        let _ = self.runtime.application_parameters();

        Ok(())
    }

    /// Executes an order operation, or closes the chain.
    ///
    /// If the chain is the one of the matching engine then the order is processed
    /// locally. Otherwise, it gets transmitted as a message to the chain of the engine.
    async fn execute_operation(&mut self, operation: Operation) -> Result<(), Self::Error> {
        match operation {
            Operation::ExecuteOrder { order } => {
                let owner = Self::get_owner(&order);
                let chain_id = self.runtime.chain_id();
                self.check_account_authentication(owner)?;
                if chain_id == self.runtime.application_id().creation.chain_id {
                    self.execute_order_local(order, chain_id).await?;
                } else {
                    self.execute_order_remote(order)?;
                }
            }
            Operation::CloseChain => {
                for order_id in self.state.orders.indices().await? {
                    match self.modify_order(order_id, ModifyAmount::All).await {
                        Ok(transfer) => self.send_to(transfer),
                        // Orders with amount zero may have been cleared in an earlier iteration.
                        Err(MatchingEngineError::OrderNotPresent) => continue,
                        Err(error) => return Err(error),
                    }
                }
                self.runtime
                    .close_chain()
                    .map_err(|_| MatchingEngineError::CloseChainError)?;
            }
        }
        Ok(())
    }

    /// Execution of the order on the creation chain
    async fn execute_message(&mut self, message: Message) -> Result<(), Self::Error> {
        ensure!(
            self.runtime.chain_id() == self.runtime.application_id().creation.chain_id,
            Self::Error::MatchingEngineChainOnly
        );
        match message {
            Message::ExecuteOrder { order } => {
                let owner = Self::get_owner(&order);
                let message_id = self
                    .runtime
                    .message_id()
                    .expect("Incoming message ID has to be available when executing a message");
                self.check_account_authentication(owner)?;
                self.execute_order_local(order, message_id.chain_id).await?;
            }
        }
        Ok(())
    }
}

impl MatchingEngineContract {
    /// Get the owner from the order
    fn get_owner(order: &Order) -> AccountOwner {
        match order {
            Order::Insert {
                owner,
                amount: _,
                nature: _,
                price: _,
            } => *owner,
            Order::Cancel { owner, order_id: _ } => *owner,
            Order::Modify {
                owner,
                order_id: _,
                cancel_amount: _,
            } => *owner,
        }
    }

    /// authenticate the originator of the message
    fn check_account_authentication(
        &mut self,
        owner: AccountOwner,
    ) -> Result<(), MatchingEngineError> {
        match owner {
            AccountOwner::User(address) => {
                ensure!(
                    self.runtime.authenticated_signer() == Some(address),
                    MatchingEngineError::IncorrectAuthentication
                )
            }
            AccountOwner::Application(id) => {
                ensure!(
                    self.runtime.authenticated_caller_id() == Some(id),
                    MatchingEngineError::IncorrectAuthentication
                )
            }
        }

        Ok(())
    }

    /// The application engine is trading between two tokens. Those tokens are the parameters of the
    /// construction of the exchange and are accessed by index in the system.
    fn fungible_id(&mut self, token_idx: u32) -> ApplicationId<FungibleTokenAbi> {
        self.runtime.application_parameters().tokens[token_idx as usize]
    }

    /// Calls into the Fungible Token application to receive tokens from the given account.
    fn receive_from_account(
        &mut self,
        owner: &AccountOwner,
        amount: &Amount,
        nature: &OrderNature,
        price: &Price,
    ) {
        let destination = Account {
            chain_id: self.runtime.chain_id(),
            owner: AccountOwner::Application(self.runtime.application_id().forget_abi()),
        };
        let (amount, token_idx) = Self::get_amount_idx(nature, price, amount);
        self.transfer(*owner, amount, destination, token_idx)
    }

    /// Transfers `amount` tokens from the funds in custody to the `destination`.
    fn send_to(&mut self, transfer: Transfer) {
        let destination = transfer.account;
        let owner_app = AccountOwner::Application(self.runtime.application_id().forget_abi());
        self.transfer(owner_app, transfer.amount, destination, transfer.token_idx);
    }

    /// Transfers tokens from the owner to the destination
    fn transfer(
        &mut self,
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
        token_idx: u32,
    ) {
        let transfer = fungible::Operation::Transfer {
            owner,
            amount,
            target_account,
        };
        let token = self.fungible_id(token_idx);
        self.runtime.call_application(true, token, &transfer);
    }

    /// Execution of orders. There are three kinds:
    /// * Cancel for total cancellation
    /// * Modify where the order is partially cancelled
    /// * Insertion order where an order is inserted into the system. It goes into following steps:
    ///   - Transfer of tokens corresponding to the order in question so that it can be paid
    ///     to the counterparty.
    ///   - Insertion of the order into the market and immediately uncrossing the market that
    ///     is making sure that at the end we have best bid < best ask.
    ///   - Creation of the corresponding orders and operation of the corresponding transfers
    async fn execute_order_local(
        &mut self,
        order: Order,
        chain_id: ChainId,
    ) -> Result<(), MatchingEngineError> {
        match order {
            Order::Insert {
                owner,
                amount,
                nature,
                price,
            } => {
                self.receive_from_account(&owner, &amount, &nature, &price);
                let account = Account { chain_id, owner };
                let transfers = self
                    .insert_and_uncross_market(&account, amount, nature, &price)
                    .await?;
                for transfer in transfers {
                    self.send_to(transfer);
                }
            }
            Order::Cancel { owner, order_id } => {
                self.modify_order_check(order_id, ModifyAmount::All, &owner)
                    .await?;
            }
            Order::Modify {
                owner,
                order_id,
                cancel_amount,
            } => {
                self.modify_order_check(order_id, ModifyAmount::Partial(cancel_amount), &owner)
                    .await?;
            }
        }
        Ok(())
    }

    /// Returns amount and type of tokens that need to be transferred to the matching engine when
    /// an order is added:
    /// * For an ask, just the token1 have to be put forward
    /// * For a bid, the product of the price with the amount has to be put
    fn get_amount_idx(nature: &OrderNature, price: &Price, amount: &Amount) -> (Amount, u32) {
        match nature {
            OrderNature::Bid => {
                let size0 = product_price_amount(*price, *amount);
                (size0, 0)
            }
            OrderNature::Ask => (*amount, 1),
        }
    }

    /// Execution of the remote order. This is done in two steps:
    /// * Transfer of the token (under the same owner to the chain of the matching engine)
    ///   This is similar to the code for the crowd-funding.
    /// * Creation of the message that will represent the order on the chain of the matching
    ///   engine
    fn execute_order_remote(&mut self, order: Order) -> Result<(), MatchingEngineError> {
        let chain_id = self.runtime.application_id().creation.chain_id;
        let message = Message::ExecuteOrder {
            order: order.clone(),
        };
        if let Order::Insert {
            owner,
            amount,
            nature,
            price,
        } = order
        {
            // First, move the funds to the matching engine chain (under the same owner).
            let destination = Account { chain_id, owner };
            let (amount, token_idx) = Self::get_amount_idx(&nature, &price, &amount);
            self.transfer(owner, amount, destination, token_idx);
        }
        self.runtime
            .prepare_message(message)
            .with_authentication()
            .send_to(chain_id);
        Ok(())
    }

    /// Checks that the order exists and has been issued by the claimed owner.
    async fn check_order_id(
        &self,
        order_id: &OrderId,
        owner: &AccountOwner,
    ) -> Result<(), MatchingEngineError> {
        let value = self.state.orders.get(order_id).await?;
        match value {
            None => Err(MatchingEngineError::OrderNotPresent),
            Some(value) => {
                if &value.account.owner != owner {
                    return Err(MatchingEngineError::WrongOwnerOfOrder);
                }
                Ok(())
            }
        }
    }

    /// This function does three things:
    /// * check the order ownership
    /// * modify the order
    /// * Send the corresponding transfers
    async fn modify_order_check(
        &mut self,
        order_id: OrderId,
        cancel_amount: ModifyAmount,
        owner: &AccountOwner,
    ) -> Result<(), MatchingEngineError> {
        self.check_order_id(&order_id, owner).await?;
        let transfer = self.modify_order(order_id, cancel_amount).await?;
        self.send_to(transfer);
        Ok(())
    }

    /// Orders which have length 0 should be removed from the system.
    /// It is possible that we have some zero orders in the QueueView
    /// under the condition that they are not the oldest.
    /// An order can be of size zero for two reasons:
    /// * It has been totally cancelled
    /// * It has been filled that is the owner got what they wanted.
    async fn remove_zero_orders_from_level(
        view: &mut LevelView,
    ) -> Result<(), MatchingEngineError> {
        // If some order has amount zero but is after an order of non-zero amount, then it is left.
        let iter = view.queue.iter_mut().await?;
        let n_remove = iter
            .take_while(|order| order.amount == Amount::ZERO)
            .count();
        for _ in 0..n_remove {
            view.queue.delete_front();
        }
        Ok(())
    }

    /// For a specific level of price, looks at all the orders and finds the one that
    /// has this specific order_id.
    /// When that order is found, then the cancellation is applied to it.
    /// Then the information is emitted for the handling of this operation.
    async fn modify_order_level(
        view: &mut LevelView,
        order_id: OrderId,
        cancel_amount: ModifyAmount,
    ) -> Result<(Amount, bool), MatchingEngineError> {
        let mut iter = view.queue.iter_mut().await?;
        let state_order = iter
            .find(|order| order.order_id == order_id)
            .ok_or(MatchingEngineError::OrderNotPresent)?;
        let new_amount = match cancel_amount {
            ModifyAmount::All => Amount::ZERO,
            ModifyAmount::Partial(cancel_amount) => state_order
                .amount
                .try_sub(cancel_amount)
                .map_err(|_| MatchingEngineError::TooLargeModifyOrder)?,
        };
        let corr_cancel_amount = state_order.amount.try_sub(new_amount).unwrap();
        state_order.amount = new_amount;
        Self::remove_zero_orders_from_level(view).await?;
        Ok((corr_cancel_amount, new_amount == Amount::ZERO))
    }

    /// Modifies the order from the order_id.
    /// This means that some transfers have to be done and the size depends
    /// whether ask or bid.
    async fn modify_order(
        &mut self,
        order_id: OrderId,
        cancel_amount: ModifyAmount,
    ) -> Result<Transfer, MatchingEngineError> {
        let key_book = self.state.orders.get(&order_id).await?;
        let key_book = key_book.ok_or_else(|| MatchingEngineError::OrderNotPresent)?;
        match key_book.nature {
            OrderNature::Bid => {
                let view = self
                    .state
                    .bids
                    .load_entry_mut(&key_book.price.to_bid())
                    .await?;
                let (cancel_amount, remove_order_id) =
                    Self::modify_order_level(view, order_id, cancel_amount).await?;
                if remove_order_id {
                    self.remove_order_id((key_book.account.owner, order_id))
                        .await?;
                }
                let cancel_amount0 = product_price_amount(key_book.price, cancel_amount);
                let transfer = Transfer {
                    account: key_book.account,
                    amount: cancel_amount0,
                    token_idx: 0,
                };
                Ok(transfer)
            }
            OrderNature::Ask => {
                let view = self
                    .state
                    .asks
                    .load_entry_mut(&key_book.price.to_ask())
                    .await?;
                let (cancel_count, remove_order_id) =
                    Self::modify_order_level(view, order_id, cancel_amount).await?;
                if remove_order_id {
                    self.remove_order_id((key_book.account.owner, order_id))
                        .await?;
                }
                let transfer = Transfer {
                    account: key_book.account,
                    amount: cancel_count,
                    token_idx: 1,
                };
                Ok(transfer)
            }
        }
    }

    /// Gets the order_id that increases starting from 0.
    fn get_new_order_id(&mut self) -> Result<OrderId, MatchingEngineError> {
        let value = self.state.next_order_number.get_mut();
        let value_ret = *value;
        *value += 1;
        Ok(value_ret)
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
                // * When the old order was created fill of token1 were commited
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
                let fill0 = product_price_amount(price_insert, fill);
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
                //   had to be commited by the buyer.
                // * When the new order is created, fill of token1 have to
                //   be commited by the seller.
                // The result is that
                // * price_insert * fill have to be sent to the seller
                // * the buyer receives
                //   - fill of token1
                //   - (price_level - price_insert) fill of token0 (nice bonus)
                assert!(price_insert <= price_level);
                let fill0 = product_price_amount(price_insert, fill);
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
                    let fill0 = product_price_amount(price_diff, fill);
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

    /// A price level is cleared starting from the oldest one till the
    /// new order is completely filled or there is no more liquidity
    /// providing order remaining to fill it.
    async fn level_clearing(
        view: &mut LevelView,
        account: &Account,
        amount: &mut Amount,
        transfers: &mut Vec<Transfer>,
        nature: &OrderNature,
        price_level: Price,
        price_insert: Price,
    ) -> Result<Vec<(AccountOwner, OrderId)>, MatchingEngineError> {
        let mut remove_order = Vec::new();
        for order in view.queue.iter_mut().await? {
            let fill = min(order.amount, *amount);
            amount.try_sub_assign(fill).unwrap();
            order.amount.try_sub_assign(fill).unwrap();
            if fill > Amount::ZERO {
                transfers.extend_from_slice(&Self::get_transfers(
                    nature,
                    fill,
                    account,
                    order,
                    price_level,
                    price_insert,
                ));
            }
            if order.amount == Amount::ZERO {
                remove_order.push((order.account.owner, order.order_id));
            }
            if *amount == Amount::ZERO {
                break;
            }
        }
        Self::remove_zero_orders_from_level(view).await?;
        Ok(remove_order)
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
    ) -> Result<(), MatchingEngineError> {
        let account_info = self
            .state
            .account_info
            .get_mut_or_default(&account.owner)
            .await?;
        account_info.orders.insert(order_id);
        let key_book = KeyBook {
            price,
            nature,
            account,
        };
        self.state.orders.insert(&order_id, key_book)?;
        Ok(())
    }

    /// Removes one single (owner, order_id) from the database
    /// * This is done for the info by owners
    /// * And the symbolic information of orders
    async fn remove_order_id(
        &mut self,
        entry: (AccountOwner, OrderId),
    ) -> Result<(), MatchingEngineError> {
        let (owner, order_id) = entry;
        let account_info = self
            .state
            .account_info
            .get_mut(&owner)
            .await
            .expect("account_info")
            .unwrap();
        account_info.orders.remove(&order_id);
        Ok(())
    }

    /// Removes a bunch of order_id
    async fn remove_order_ids(
        &mut self,
        entries: Vec<(AccountOwner, OrderId)>,
    ) -> Result<(), MatchingEngineError> {
        for entry in entries {
            self.remove_order_id(entry).await?;
        }
        Ok(())
    }

    /// Inserts an order into the matching engine and this creates several things:
    /// * The price levels that matches are selected
    /// * Getting from the best matching price to the least good the price levels
    ///   are cleared.
    /// * That clearing creates a number of transfer orders.
    /// * If after the level clearing the order is completely filled then it it not
    ///   inserted. Otherwise, it became a liquidity order in the matching engine
    async fn insert_and_uncross_market(
        &mut self,
        account: &Account,
        amount: Amount,
        nature: OrderNature,
        price: &Price,
    ) -> Result<Vec<Transfer>, MatchingEngineError> {
        // Bids are ordered from the highest bid (most preferable) to the smallest bid.
        // Asks are ordered from the smallest (most preferable) to the highest.
        // The prices have custom serialization so that they are in increasing order.
        // To reverse the order of the bids, we take the bitwise complement of the price.
        let order_id = self.get_new_order_id()?;
        let mut final_amount = amount;
        let mut transfers = Vec::new();
        match nature {
            OrderNature::Bid => {
                let mut matching_price_asks = Vec::new();
                self.state
                    .asks
                    .for_each_index_while(|price_ask| {
                        let matches = price_ask.to_price() <= *price;
                        if matches {
                            matching_price_asks.push(price_ask);
                        }
                        Ok(matches)
                    })
                    .await?;
                for price_ask in matching_price_asks {
                    let view = self.state.asks.load_entry_mut(&price_ask).await?;
                    let remove_entry = Self::level_clearing(
                        view,
                        account,
                        &mut final_amount,
                        &mut transfers,
                        &nature,
                        price_ask.to_price(),
                        *price,
                    )
                    .await?;
                    if view.queue.count() == 0 {
                        self.state.asks.remove_entry(&price_ask)?;
                    }
                    self.remove_order_ids(remove_entry).await?;
                    if final_amount == Amount::ZERO {
                        break;
                    }
                }
                if final_amount != Amount::ZERO {
                    let view = self.state.bids.load_entry_mut(&price.to_bid()).await?;
                    let order = OrderEntry {
                        amount: final_amount,
                        account: *account,
                        order_id,
                    };
                    view.queue.push_back(order);
                    self.insert_order(*account, OrderNature::Bid, order_id, *price)
                        .await?;
                }
            }
            OrderNature::Ask => {
                let mut matching_price_bids = Vec::new();
                self.state
                    .bids
                    .for_each_index_while(|price_bid| {
                        let matches = price_bid.to_price() >= *price;
                        if matches {
                            matching_price_bids.push(price_bid);
                        }
                        Ok(matches)
                    })
                    .await?;
                for price_bid in matching_price_bids {
                    let view = self.state.bids.load_entry_mut(&price_bid).await?;
                    let remove_entry = Self::level_clearing(
                        view,
                        account,
                        &mut final_amount,
                        &mut transfers,
                        &nature,
                        price_bid.to_price(),
                        *price,
                    )
                    .await?;
                    if view.queue.count() == 0 {
                        self.state.bids.remove_entry(&price_bid)?;
                    }
                    self.remove_order_ids(remove_entry).await?;
                    if final_amount == Amount::ZERO {
                        break;
                    }
                }
                if final_amount != Amount::ZERO {
                    let view = self.state.asks.load_entry_mut(&price.to_ask()).await?;
                    let order = OrderEntry {
                        amount: final_amount,
                        account: *account,
                        order_id,
                    };
                    view.queue.push_back(order);
                    self.insert_order(*account, OrderNature::Ask, order_id, *price)
                        .await?;
                }
            }
        }
        Ok(transfers)
    }
}
