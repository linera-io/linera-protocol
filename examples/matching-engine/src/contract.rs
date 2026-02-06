// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::{Account, AccountOwner, Amount, ChainId, WithContractAbi},
    views::{linera_views, RootView, View},
    Contract, ContractRuntime,
};
use matching_engine::{
    MatchingEngineAbi, Message, Operation, Order, OrderNature, Parameters, PendingOrderInfo, Price,
};
use state::{MatchingEngineState, Transfer};

pub struct MatchingEngineContract {
    state: MatchingEngineState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(MatchingEngineContract);

impl WithContractAbi for MatchingEngineContract {
    type Abi = MatchingEngineAbi;
}

impl Contract for MatchingEngineContract {
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = Parameters;
    type EventValue = ();

    async fn load(mut runtime: ContractRuntime<Self>) -> Self {
        let parameters = runtime.application_parameters();
        let context = linera_views::context::ViewContext::new_unchecked(
            runtime.key_value_store(),
            Vec::new(),
            parameters,
        );
        let state = MatchingEngineState::load(context)
            .await
            .expect("Failed to load state");
        MatchingEngineContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {}

    /// Executes an order operation, or closes the chain.
    ///
    /// If the chain is the one of the matching engine then the order is processed
    /// locally. Otherwise, it gets transmitted as a message to the chain of the engine.
    async fn execute_operation(&mut self, operation: Operation) -> Self::Response {
        match operation {
            Operation::ExecuteOrder { order } => {
                self.runtime
                    .application_parameters()
                    .check_precision(&order);
                let owner = order.owner();
                let chain_id = self.runtime.chain_id();
                self.runtime
                    .check_account_permission(owner)
                    .expect("Permission for ExecuteOrder operation");
                if chain_id == self.runtime.application_creator_chain_id() {
                    self.execute_order_local(order, chain_id).await;
                } else {
                    self.execute_order_remote(order);
                }
            }
            Operation::CloseChain => {
                let order_ids = self
                    .state
                    .orders
                    .indices()
                    .await
                    .expect("Failed to read existing order IDs");
                for order_id in order_ids {
                    match self.state.modify_order(order_id, Amount::ZERO).await {
                        Some(transfer) => self.send_to(transfer),
                        // Orders with amount zero may have been cleared in an earlier iteration.
                        None => continue,
                    }
                }
                self.runtime
                    .close_chain()
                    .expect("The application does not have permissions to close the chain.");
            }
        }
    }

    /// Execution of messages
    async fn execute_message(&mut self, message: Message) {
        match message {
            Message::ExecuteOrder { order } => {
                // This message can only be executed on the matching engine chain
                assert_eq!(
                    self.runtime.chain_id(),
                    self.runtime.application_creator_chain_id(),
                    "ExecuteOrder can only be executed on the chain that created the matching engine"
                );
                let owner = order.owner();
                let origin_chain_id = self.runtime.message_origin_chain_id().expect(
                    "Incoming message origin chain ID has to be available when executing a message",
                );
                self.runtime
                    .check_account_permission(owner)
                    .expect("Permission for ExecuteOrder message");
                self.execute_order_local(order, origin_chain_id).await;
            }
            Message::OrderPending {
                owner,
                order_id,
                order_info,
            } => {
                // This message is received on the sender chain from the matching engine
                // Order was inserted and is pending
                let pending_orders = self
                    .state
                    .pending_orders
                    .get_mut_or_default(&owner)
                    .await
                    .expect("Failed to load pending orders");
                pending_orders.insert(order_id, order_info);
            }
            Message::OrderUpdated {
                owner,
                order_id,
                new_quantity,
            } => {
                // This message is received on the sender chain from the matching engine
                let pending_orders = self
                    .state
                    .pending_orders
                    .get_mut(&owner)
                    .await
                    .expect("Failed to load pending orders")
                    .expect("Account should have pending orders");

                if new_quantity == Amount::ZERO {
                    // Remove the pending order
                    pending_orders.remove(&order_id);
                } else {
                    // Update the pending order
                    if let Some(order) = pending_orders.get_mut(&order_id) {
                        order.quantity = new_quantity;
                    }
                }
            }
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl MatchingEngineContract {
    /// Calls into the Fungible Token application to receive tokens from the given account.
    fn receive_from_account(
        &mut self,
        owner: &AccountOwner,
        quantity: &Amount,
        nature: &OrderNature,
        price: &Price,
    ) {
        let destination = Account {
            chain_id: self.runtime.chain_id(),
            owner: self.runtime.application_id().into(),
        };
        let (amount, token_idx) = self
            .runtime
            .application_parameters()
            .get_amount_idx(nature, price, quantity);
        self.transfer(*owner, amount, destination, token_idx)
    }

    /// Transfers `amount` tokens from the funds in custody to the `destination`.
    fn send_to(&mut self, transfer: Transfer) {
        let destination = transfer.account;
        let owner_app = self.runtime.application_id().into();
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
        let transfer = fungible::FungibleOperation::Transfer {
            owner,
            amount,
            target_account,
        };
        let token = self.runtime.application_parameters().fungible_id(token_idx);
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
    async fn execute_order_local(&mut self, order: Order, chain_id: ChainId) {
        match order {
            Order::Insert {
                owner,
                quantity,
                nature,
                price,
            } => {
                self.receive_from_account(&owner, &quantity, &nature, &price);
                let account = Account { chain_id, owner };
                let (transfers, order_id, remaining_quantity, filled_orders) = self
                    .state
                    .insert_and_uncross_market(&account, quantity, nature, &price)
                    .await;
                for transfer in transfers {
                    self.send_to(transfer);
                }

                // Send removal notifications for filled orders
                let matching_engine_chain = self.runtime.chain_id();
                for (filled_chain_id, filled_owner, filled_order_id) in filled_orders {
                    if filled_chain_id != matching_engine_chain {
                        let removal_message = Message::OrderUpdated {
                            owner: filled_owner,
                            order_id: filled_order_id,
                            new_quantity: Amount::ZERO,
                        };
                        self.runtime
                            .prepare_message(removal_message)
                            .with_authentication()
                            .send_to(filled_chain_id);
                    }
                }

                // Send acknowledgment to the sender chain (if different from matching engine chain)
                if remaining_quantity > Amount::ZERO {
                    let order_info = PendingOrderInfo {
                        nature,
                        price,
                        quantity: remaining_quantity,
                    };
                    if chain_id != self.runtime.chain_id() {
                        let pending_message = Message::OrderPending {
                            owner,
                            order_id,
                            order_info,
                        };
                        self.runtime
                            .prepare_message(pending_message)
                            .with_authentication()
                            .send_to(chain_id);
                    } else {
                        let pending_orders = self
                            .state
                            .pending_orders
                            .get_mut_or_default(&owner)
                            .await
                            .expect("Failed to load pending orders");
                        pending_orders.insert(order_id, order_info);
                    }
                }
            }
            Order::Modify {
                owner,
                order_id,
                new_quantity,
            } => {
                self.state.check_order_id(&order_id, &owner).await;
                let key_book = self
                    .state
                    .orders
                    .get(&order_id)
                    .await
                    .expect("Failed to load order")
                    .expect("Order should exist");
                let sender_chain_id = key_book.account.chain_id;

                let transfer = self
                    .state
                    .modify_order(order_id, new_quantity)
                    .await
                    .expect("Failed to modify order");
                self.send_to(transfer);

                // Order still exists with reduced amount
                if sender_chain_id != self.runtime.chain_id() {
                    // Send update notification to the sender chain (if different from matching engine chain)
                    let update_message = Message::OrderUpdated {
                        owner,
                        order_id,
                        new_quantity,
                    };
                    self.runtime
                        .prepare_message(update_message)
                        .with_authentication()
                        .send_to(sender_chain_id);
                } else {
                    let pending_orders = self
                        .state
                        .pending_orders
                        .get_mut(&owner)
                        .await
                        .expect("Failed to load pending orders")
                        .expect("Account should have pending orders");
                    // Update the pending order
                    if let Some(order) = pending_orders.get_mut(&order_id) {
                        order.quantity = new_quantity;
                    }
                }
            }
        }
    }

    /// Execution of the remote order. This is done in two steps:
    /// * Transfer of the token (under the same owner to the chain of the matching engine)
    ///   This is similar to the code for the crowd-funding.
    /// * Creation of the message that will represent the order on the chain of the matching
    ///   engine
    fn execute_order_remote(&mut self, order: Order) {
        let chain_id = self.runtime.application_creator_chain_id();
        let message = Message::ExecuteOrder {
            order: order.clone(),
        };
        if let Order::Insert {
            owner,
            quantity,
            nature,
            price,
        } = order
        {
            // First, move the funds to the matching engine chain (under the same owner).
            let destination = Account { chain_id, owner };
            let (amount, token_idx) = self
                .runtime
                .application_parameters()
                .get_amount_idx(&nature, &price, &quantity);
            self.transfer(owner, amount, destination, token_idx);
        }
        self.runtime
            .prepare_message(message)
            .with_authentication()
            .send_to(chain_id);
    }
}
