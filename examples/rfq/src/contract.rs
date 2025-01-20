// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use fungible::FungibleTokenAbi;
use linera_sdk::{
    base::{
        Amount, ApplicationPermissions, ChainId, ChainOwnership, PublicKey, TimeoutConfig,
        WithContractAbi,
    },
    views::{RootView, View},
    Contract, ContractRuntime,
};
use matching_engine::{
    MatchingEngineAbi, Operation as MEOperation, Order, Parameters as MEParameters,
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
                quoter_pub_key,
                quoter_account,
            } => {
                self.state
                    .update_state_with_quote(
                        &request_id,
                        quote,
                        quoter_pub_key,
                        Some(quoter_account),
                    )
                    .await;
                let message = Message::ProvideQuote {
                    seq_number: request_id.seq_number(),
                    quote,
                    quoter_pub_key,
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(request_id.chain_id())
            }
            Operation::AcceptQuote {
                request_id,
                account_owner,
                pub_key,
                fee_budget,
            } => {
                let (exchange_order, quoter_pub_key) =
                    self.state.get_bid_order(&request_id, account_owner).await;
                let matching_engine_chain_id = self
                    .start_exchange(
                        &request_id,
                        exchange_order,
                        pub_key,
                        quoter_pub_key,
                        fee_budget,
                    )
                    .await;
                self.state
                    .start_exchange(&request_id, matching_engine_chain_id.clone())
                    .await;
            }
            Operation::CancelRequest { request_id } => {
                let _maybe_matching_engine_chain_id = self.state.cancel_request(&request_id).await;
                // TODO: close the chain if it exists
                let message = Message::CancelRequest {
                    seq_number: request_id.seq_number(),
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(request_id.chain_id())
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
                quoter_pub_key,
                ..
            } => {
                self.state
                    .update_state_with_quote(&request_id, quote, quoter_pub_key, None)
                    .await;
            }
            Message::QuoteAccepted {
                request_id,
                matching_engine_message_id: _,
                matching_engine_app_id,
                ..
            } => {
                let owner = self.state.get_quoter_account(&request_id).await;
                let order = self.state.get_ask_order(&request_id, owner).await;
                // the message should have been sent from the temporary chain
                let matching_engine_chain_id = self.get_message_creation_chain_id();
                self.state
                    .start_exchange(&request_id, matching_engine_chain_id)
                    .await;
                self.runtime.call_application(
                    true,
                    matching_engine_app_id.with_abi::<MatchingEngineAbi>(),
                    &MEOperation::ExecuteOrder { order },
                );
            }
            Message::CancelRequest { .. } => {
                let _maybe_matching_engine_chain_id = self.state.cancel_request(&request_id).await;
                // TODO: close the chain if it exists
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
                        token_pair.token_offered.with_abi::<FungibleTokenAbi>(),
                        token_pair.token_asked.with_abi::<FungibleTokenAbi>(),
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
                self.runtime.send_message(
                    request_id.chain_id(),
                    Message::QuoteAccepted {
                        request_id: RequestId::new(initiator, request_id.seq_number()),
                        matching_engine_message_id,
                        matching_engine_app_id: me_application_id.forget_abi(),
                    },
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
        order: Order,
        our_pub_key: PublicKey,
        quoter_pub_key: PublicKey,
        fee_budget: Amount,
    ) -> ChainId {
        // TODO: start the new chain with matching engine and send the order there
        let ownership = ChainOwnership::multiple(
            [(our_pub_key, 100), (quoter_pub_key, 100)],
            100,
            TimeoutConfig::default(),
        );
        let app_id = self.runtime.application_id();
        let permissions = ApplicationPermissions::new_single(app_id.forget_abi());
        let (matching_engine_message_id, matching_engine_chain_id) =
            self.runtime.open_chain(ownership, permissions, fee_budget);

        // create an application and submit the order!
        let initiator = self.runtime.chain_id();
        let token_pair = self.state.token_pair(request_id).await;
        self.runtime.send_message(
            matching_engine_chain_id,
            Message::StartMatchingEngine {
                initiator,
                request_id: request_id.clone(),
                order,
                token_pair,
                matching_engine_message_id,
            },
        );

        matching_engine_chain_id
    }
}
