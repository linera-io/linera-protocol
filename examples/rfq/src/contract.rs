// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use fungible::{Account, FungibleTokenAbi};
use linera_sdk::{
    linera_base_types::{
        AccountOwner, Amount, ApplicationPermissions, ChainId, ChainOwnership, Owner,
        TimeoutConfig, WithContractAbi,
    },
    views::{RootView, View},
    Contract, ContractRuntime,
};
use rfq::{Message, Operation, RequestId, RfqAbi, TokenPair, Tokens};

use self::state::{QuoteProvided, RfqState};

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
    type Parameters = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = RfqState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        RfqContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        if self.state.is_temp_chain() {
            // No operations should be executed directly on a temporary chain
            return;
        }
        match operation {
            Operation::RequestQuote {
                target,
                token_pair,
                amount,
            } => {
                let seq_number = self
                    .state
                    .create_new_request(target, token_pair.clone(), amount);
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
                let request_data = self
                    .state
                    .request_data(&request_id)
                    .await
                    .expect("Request not found!");
                if request_id.is_our_request() {
                    panic!("Tried to provide a quote for our own request!");
                }
                request_data.update_state_with_quote(quote, quoter_owner);
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
                let request_data = self
                    .state
                    .request_data(&request_id)
                    .await
                    .expect("Request not found!");
                if !request_id.is_our_request() {
                    panic!("Tried to accept a quote we have provided");
                }
                let quote_provided = request_data.quote_provided();
                let token_pair = request_data.token_pair();
                let temp_chain_id = self
                    .start_exchange(
                        request_id.clone(),
                        quote_provided,
                        token_pair,
                        fee_budget,
                        owner,
                    )
                    .await;
                self.state
                    .request_data(&request_id)
                    .await
                    .expect("Request not found!")
                    .start_exchange(temp_chain_id);
            }
            Operation::FinalizeDeal { request_id } => {
                let request_data = self
                    .state
                    .request_data(&request_id)
                    .await
                    .expect("Request not found!");
                let awaiting_tokens = request_data.awaiting_tokens();
                let owner = awaiting_tokens.quoter_account;
                // the message should have been sent from the temporary chain
                let temp_chain_id = awaiting_tokens.temp_chain_id;

                // transfer tokens to the new chain
                let token_pair = awaiting_tokens.token_pair;
                let transfer = fungible::Operation::Transfer {
                    owner,
                    amount: awaiting_tokens.amount_offered,
                    target_account: Account {
                        chain_id: temp_chain_id,
                        owner: AccountOwner::Application(
                            self.runtime.application_id().forget_abi(),
                        ),
                    },
                };
                let token = token_pair.token_asked;
                self.runtime.call_application(
                    true,
                    token.with_abi::<FungibleTokenAbi>(),
                    &transfer,
                );

                let message = Message::TokensSent {
                    tokens: Box::new(Tokens {
                        token_id: token,
                        owner: Account {
                            chain_id: self.runtime.chain_id(),
                            owner,
                        },
                        amount: awaiting_tokens.amount_offered,
                    }),
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(temp_chain_id);
                request_data.start_exchange(temp_chain_id);
            }
            Operation::CancelRequest { request_id } => {
                if let Some(temp_chain_id) = self.state.cancel_request(&request_id).await {
                    let message = Message::CloseChain;
                    self.runtime
                        .prepare_message(message)
                        .with_authentication()
                        .send_to(temp_chain_id);
                } else {
                    let message = Message::CancelRequest {
                        seq_number: request_id.seq_number(),
                        recipient_requested: !request_id.is_our_request(),
                    };
                    self.runtime
                        .prepare_message(message)
                        .with_authentication()
                        .send_to(request_id.chain_id());
                }
            }
        }
    }

    async fn execute_message(&mut self, message: Self::Message) {
        let request_id = message.request_id(self.get_message_creation_chain_id());
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
                    .request_data(&request_id)
                    .await
                    .expect("Request not found!")
                    .update_state_with_quote(quote, quoter_owner);
            }
            Message::QuoteAccepted { request_id } => {
                let temp_chain_id = self.get_message_creation_chain_id();
                self.state
                    .request_data(&request_id)
                    .await
                    .expect("Request not found!")
                    .accept_quote(temp_chain_id);
            }
            Message::CancelRequest { .. } => {
                if let Some(temp_chain_id) = self.state.cancel_request(&request_id).await {
                    // the other side wasn't aware of the chain yet, or they would have
                    // sent `RequestClosed` - we have to close it ourselves
                    let message = Message::CloseChain;
                    self.runtime
                        .prepare_message(message)
                        .with_authentication()
                        .send_to(temp_chain_id);
                }
            }
            Message::ChainClosed { request_id } => {
                self.state.close_request(&request_id).await;
            }
            // the message below should only be executed on the temporary chains
            Message::StartExchange {
                request_id,
                initiator,
                token_pair,
                tokens,
            } => {
                // save the info in the app state
                self.state.init_temp_chain_state(
                    request_id.clone(),
                    initiator,
                    *token_pair,
                    *tokens,
                );

                // inform the other side that the temporary chain is ready
                let message = Message::QuoteAccepted {
                    request_id: RequestId::new(initiator, request_id.seq_number(), false),
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(request_id.chain_id());
            }
            Message::TokensSent { tokens } => {
                match self.state.take_temp_chain_held_tokens() {
                    None => {
                        // This should never really happen, but if it does, return the
                        // sent tokens
                        let app_id = self.runtime.application_id().forget_abi();
                        self.runtime.call_application(
                            true,
                            tokens.token_id.with_abi::<fungible::FungibleTokenAbi>(),
                            &fungible::Operation::Transfer {
                                owner: AccountOwner::Application(app_id),
                                amount: tokens.amount,
                                target_account: tokens.owner,
                            },
                        );
                    }
                    Some(held_tokens) => {
                        // we have tokens from both parties now: return them to the
                        // correct parties
                        let app_id = self.runtime.application_id().forget_abi();
                        self.runtime.call_application(
                            true,
                            tokens.token_id.with_abi::<fungible::FungibleTokenAbi>(),
                            &fungible::Operation::Transfer {
                                owner: AccountOwner::Application(app_id),
                                amount: tokens.amount,
                                target_account: held_tokens.owner,
                            },
                        );
                        self.runtime.call_application(
                            true,
                            held_tokens
                                .token_id
                                .with_abi::<fungible::FungibleTokenAbi>(),
                            &fungible::Operation::Transfer {
                                owner: AccountOwner::Application(app_id),
                                amount: held_tokens.amount,
                                target_account: tokens.owner,
                            },
                        );
                    }
                }
                // exchange is finished, we can close the chain
                self.close_chain();
            }
            Message::CloseChain => {
                self.close_chain();
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
        request_id: RequestId,
        quote_provided: QuoteProvided,
        token_pair: TokenPair,
        fee_budget: Amount,
        owner: Owner,
    ) -> ChainId {
        let ownership = ChainOwnership::multiple(
            [(owner, 100), (quote_provided.get_quoter_owner(), 100)],
            100,
            TimeoutConfig::default(),
        );
        let app_id = self.runtime.application_id();
        let permissions = ApplicationPermissions::new_single(app_id.forget_abi());
        let (_, temp_chain_id) = self.runtime.open_chain(ownership, permissions, fee_budget);

        // transfer tokens to the new chain
        let transfer = fungible::Operation::Transfer {
            owner: owner.into(),
            amount: quote_provided.get_amount(),
            target_account: Account {
                chain_id: temp_chain_id,
                owner: AccountOwner::Application(self.runtime.application_id().forget_abi()),
            },
        };
        let token = token_pair.token_offered;
        self.runtime
            .call_application(true, token.with_abi::<FungibleTokenAbi>(), &transfer);

        // signal to the temporary chain that it should start the exchange!
        let initiator = self.runtime.chain_id();
        let message = Message::StartExchange {
            initiator,
            request_id,
            token_pair: Box::new(token_pair),
            tokens: Box::new(Tokens {
                token_id: token,
                owner: Account {
                    chain_id: self.runtime.chain_id(),
                    owner: owner.into(),
                },
                amount: quote_provided.get_amount(),
            }),
        };
        self.runtime
            .prepare_message(message)
            .with_authentication()
            .send_to(temp_chain_id);

        temp_chain_id
    }

    fn close_chain(&mut self) {
        // we're executing on the temporary chain
        // if we hold some tokens, we return them before closing
        if let Some(tokens) = self.state.temp_chain_held_tokens() {
            let app_id = self.runtime.application_id().forget_abi();
            self.runtime.call_application(
                true,
                tokens.token_id.with_abi::<fungible::FungibleTokenAbi>(),
                &fungible::Operation::Transfer {
                    owner: AccountOwner::Application(app_id),
                    amount: tokens.amount,
                    target_account: tokens.owner,
                },
            );
        }

        self.runtime
            .close_chain()
            .expect("Failed to close the temporary chain");

        // let both users know that the chain is now closed
        let (initiator, request_id) = self.state.temp_chain_initiator_and_request_id();
        let message = Message::ChainClosed {
            request_id: request_id.clone(),
        };
        self.runtime
            .prepare_message(message)
            .with_authentication()
            .send_to(initiator);

        let other_chain = request_id.chain_id();
        let request_id = RequestId::new(initiator, request_id.seq_number(), false);
        let message = Message::ChainClosed { request_id };
        self.runtime
            .prepare_message(message)
            .with_authentication()
            .send_to(other_chain);
    }
}
