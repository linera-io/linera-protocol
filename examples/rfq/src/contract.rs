// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use fungible::{Account, FungibleTokenAbi};
use linera_sdk::{
    base::{
        Amount, ApplicationPermissions, ChainId, ChainOwnership, Owner, TimeoutConfig,
        WithContractAbi,
    },
    views::{RootView, View},
    Contract, ContractRuntime,
};
use matching_engine::{
    MatchingEngineAbi, Operation as MEOperation, Order, OrderNature, Parameters as MEParameters,
};
use rfq::{Message, Operation, Parameters, RequestId, RfqAbi};

use self::state::RfqState;

pub struct RfqContract {
    state: RfqState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(RfqContract);

impl WithContractAbi for RfqContract {
    type Abi = RfqAbi;
}

impl Contract for RfqContract {
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = Parameters;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = RfqState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        RfqContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        // Validate that the application parameters were configured correctly.
        let _ = self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        match operation {
            Operation::RequestQuote {
                target,
                token_pair,
                amount,
            } => {
                let seq_number =
                    self.state
                        .create_new_request(target.clone(), token_pair.clone(), amount);
                let message = Message::RequestQuote {
                    seq_number,
                    token_pair,
                    amount,
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(target);
            }
            Operation::ProvideQuote {
                request_id,
                quote,
                quoter_owner,
            } => {
                self.state
                    .update_state_with_quote(&request_id, quote, quoter_owner)
                    .await;
                let message = Message::ProvideQuote {
                    seq_number: request_id.seq_number(),
                    quote,
                    quoter_owner,
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(request_id.chain_id())
            }
            Operation::AcceptQuote {
                request_id,
                owner,
                fee_budget,
            } => {
                let matching_engine_chain_id =
                    self.start_exchange(&request_id, fee_budget, owner).await;
                self.state
                    .start_exchange(&request_id, matching_engine_chain_id.clone())
                    .await;
            }
            Operation::FinalizeDeal { request_id } => {
                let awaiting_tokens = self.state.get_awaiting_tokens(&request_id).await;
                let owner = awaiting_tokens.quoter_account;
                let order = Order::Insert {
                    owner,
                    amount: awaiting_tokens.amount,
                    nature: OrderNature::Bid,
                    price: awaiting_tokens.price,
                };
                // the message should have been sent from the temporary chain
                let matching_engine_chain_id = awaiting_tokens.matching_engine_chain_id;

                // transfer tokens to the new chain
                let token_pair = awaiting_tokens.token_pair;
                let transfer = fungible::Operation::Transfer {
                    owner,
                    amount: matching_engine::product_price_amount(
                        awaiting_tokens.price,
                        awaiting_tokens.amount,
                    ),
                    target_account: Account {
                        chain_id: matching_engine_chain_id,
                        owner,
                    },
                };
                let token = token_pair.token_asked.with_abi::<FungibleTokenAbi>();
                self.runtime.call_application(true, token, &transfer);

                let message = Message::TokensSent {
                    matching_engine_app_id: awaiting_tokens.matching_engine_app_id,
                    order,
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(matching_engine_chain_id);
                self.state
                    .start_exchange(&request_id, matching_engine_chain_id)
                    .await;
            }
            Operation::CancelRequest { request_id } => {
                if let Some(matching_engine_chain_id) = self.state.cancel_request(&request_id).await
                {
                    let message = Message::RequestClosed {
                        seq_number: request_id.seq_number(),
                        matching_engine_chain_id,
                    };
                    self.runtime
                        .prepare_message(message)
                        .with_authentication()
                        .send_to(matching_engine_chain_id);
                    let message = Message::RequestClosed {
                        seq_number: request_id.seq_number(),
                        matching_engine_chain_id,
                    };
                    self.runtime
                        .prepare_message(message)
                        .with_authentication()
                        .send_to(request_id.chain_id());
                } else {
                    let message = Message::CancelRequest {
                        seq_number: request_id.seq_number(),
                    };
                    self.runtime
                        .prepare_message(message)
                        .with_authentication()
                        .send_to(request_id.chain_id());
                }
            }
            Operation::CloseRequest { request_id } => {
                let matching_engine_chain_id = self.state.close_request(&request_id).await;
                // send messages to the other party's chain and to the temporary chain
                let message = Message::RequestClosed {
                    seq_number: request_id.seq_number(),
                    matching_engine_chain_id,
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(matching_engine_chain_id);
                let message = Message::RequestClosed {
                    seq_number: request_id.seq_number(),
                    matching_engine_chain_id,
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(request_id.chain_id());
            }
        }
    }

    async fn execute_message(&mut self, message: Self::Message) {
        let request_id = RequestId::new(self.get_message_creation_chain_id(), message.seq_number());
        match message {
            Message::RequestQuote {
                token_pair, amount, ..
            } => {
                self.state.register_request(request_id, token_pair, amount);
            }
            Message::ProvideQuote {
                quote,
                quoter_owner,
                ..
            } => {
                self.state
                    .update_state_with_quote(&request_id, quote, quoter_owner)
                    .await;
            }
            Message::QuoteAccepted {
                request_id,
                matching_engine_message_id: _,
                matching_engine_app_id,
                ..
            } => {
                let matching_engine_chain_id = self.get_message_creation_chain_id();
                self.state
                    .quote_accepted(
                        &request_id,
                        matching_engine_chain_id,
                        matching_engine_app_id,
                    )
                    .await;
            }
            Message::CancelRequest { .. } => {
                if let Some(matching_engine_chain_id) = self.state.cancel_request(&request_id).await
                {
                    // the other side wasn't aware of the chain yet, or they would have
                    // sent `RequestClosed` - we have to close it ourselves
                    let message = Message::RequestClosed {
                        seq_number: request_id.seq_number(),
                        matching_engine_chain_id,
                    };
                    self.runtime
                        .prepare_message(message)
                        .with_authentication()
                        .send_to(matching_engine_chain_id);
                }
            }
            Message::RequestClosed {
                matching_engine_chain_id,
                ..
            } => {
                if matching_engine_chain_id == self.runtime.chain_id() {
                    // we're executing on the temporary chain - let's close it
                    let matching_engine_app_id = self
                        .state
                        .me_application_id()
                        .expect("No TempChainState found!");
                    self.runtime.call_application(
                        true,
                        matching_engine_app_id.with_abi::<MatchingEngineAbi>(),
                        &MEOperation::CloseChain,
                    );
                } else {
                    // we're executing on the other party's chain: just close the request
                    let _ = self.state.close_request(&request_id).await;
                }
            }
            // the message below should only be executed on the temporary chains
            Message::StartMatchingEngine {
                request_id,
                initiator,
                token_pair,
                order,
                matching_engine_message_id,
            } => {
                // create the matching engine app
                let parameters = self.runtime.application_parameters();
                let me_parameters = MEParameters {
                    tokens: [
                        token_pair.token_asked.with_abi::<FungibleTokenAbi>(),
                        token_pair.token_offered.with_abi::<FungibleTokenAbi>(),
                    ],
                };
                let me_application_id = self
                    .runtime
                    .create_application::<MatchingEngineAbi, MEParameters, ()>(
                        parameters.me_bytecode_id,
                        &me_parameters,
                        &(),
                        vec![],
                    );
                // save the info in the app state
                self.state.init_temp_chain_state(
                    request_id.clone(),
                    initiator,
                    me_application_id.forget_abi(),
                );
                // submit the order in the name of the temp chain creator
                self.runtime.call_application(
                    true,
                    me_application_id,
                    &MEOperation::ExecuteOrder { order },
                );
                // inform the other side that the temporary chain is ready
                let message = Message::QuoteAccepted {
                    request_id: RequestId::new(initiator, request_id.seq_number()),
                    matching_engine_message_id,
                    matching_engine_app_id: me_application_id.forget_abi(),
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(request_id.chain_id());
            }
            Message::TokensSent {
                matching_engine_app_id,
                order,
            } => {
                self.runtime.call_application(
                    true,
                    matching_engine_app_id.with_abi::<MatchingEngineAbi>(),
                    &MEOperation::ExecuteOrder { order },
                );
            }
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl RfqContract {
    fn get_message_creation_chain_id(&mut self) -> ChainId {
        self.runtime
            .message_id()
            .expect("Getting message id should not fail")
            .chain_id
    }

    async fn start_exchange(
        &mut self,
        request_id: &RequestId,
        fee_budget: Amount,
        owner: Owner,
    ) -> ChainId {
        let quote_provided = self.state.quote_provided(request_id).await;
        let ownership = ChainOwnership::multiple(
            [(owner, 100), (quote_provided.get_quoter_owner(), 100)],
            100,
            TimeoutConfig::default(),
        );
        let app_id = self.runtime.application_id();
        let permissions = ApplicationPermissions::new_single(app_id.forget_abi());
        let (matching_engine_message_id, matching_engine_chain_id) =
            self.runtime.open_chain(ownership, permissions, fee_budget);

        let token_pair = self.state.token_pair(request_id).await;

        // transfer tokens to the new chain
        let transfer = fungible::Operation::Transfer {
            owner: owner.into(),
            amount: quote_provided.get_amount(),
            target_account: Account {
                chain_id: matching_engine_chain_id,
                owner: owner.into(),
            },
        };
        let token = token_pair.token_offered.with_abi::<FungibleTokenAbi>();
        self.runtime.call_application(true, token, &transfer);

        // signal to the temporary chain that it should start the exchange!
        let initiator = self.runtime.chain_id();
        let order = quote_provided.get_ask_order(owner.into());
        let message = Message::StartMatchingEngine {
            initiator,
            request_id: request_id.clone(),
            order,
            token_pair,
            matching_engine_message_id,
        };
        self.runtime
            .prepare_message(message)
            .with_authentication()
            .send_to(matching_engine_chain_id);

        matching_engine_chain_id
    }
}
